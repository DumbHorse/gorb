package core

import (
	"net"
	"net/netip"
	"testing"

	"github.com/cloudflare/ipvs"
	"github.com/cloudflare/ipvs/netmask"
	"github.com/qk4l/gorb/disco"
	"github.com/qk4l/gorb/pulse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type fakeDisco struct {
	mock.Mock
}

func (d *fakeDisco) Expose(name, host string, port uint16) error {
	args := d.Called(name, host, port)
	return args.Error(0)
}

func (d *fakeDisco) Remove(name string) error {
	args := d.Called(name)
	return args.Error(0)
}

type fakeIpvs struct {
	mock.Mock
}

func (f *fakeIpvs) Info() (ipvs.Info, error) {
	var info ipvs.Info
	return info, nil
}

func (f *fakeIpvs) Config() (ipvs.Config, error) {
	var config ipvs.Config
	return config, nil
}

func (f *fakeIpvs) SetConfig(config ipvs.Config) error {
	args := f.Called(config)
	return args.Error(0)
}

func (f *fakeIpvs) Services() ([]ipvs.ServiceExtended, error) {
	var services []ipvs.ServiceExtended
	return services, nil
}

func (f *fakeIpvs) Service(s ipvs.Service) (ipvs.ServiceExtended, error) {
	args := f.Called(s)
	var service ipvs.ServiceExtended
	return service, args.Error(1)
}

func (f *fakeIpvs) CreateService(s ipvs.Service) error {
	args := f.Called(s)
	return args.Error(0)
}

func (f *fakeIpvs) UpdateService(s ipvs.Service) error {
	args := f.Called(s)
	return args.Error(0)
}

func (f *fakeIpvs) RemoveService(s ipvs.Service) error {
	args := f.Called(s)
	return args.Error(0)
}

func (f *fakeIpvs) Destinations(s ipvs.Service) ([]ipvs.DestinationExtended, error) {
	args := f.Called(s)
	var destinations []ipvs.DestinationExtended
	return destinations, args.Error(0)
}

func (f *fakeIpvs) CreateDestination(s ipvs.Service, destination ipvs.Destination) error {
	args := f.Called(s, destination)
	return args.Error(0)
}

func (f *fakeIpvs) UpdateDestination(s ipvs.Service, destination ipvs.Destination) error {
	args := f.Called(s, destination)
	return args.Error(0)
}

func (f *fakeIpvs) RemoveDestination(s ipvs.Service, destination ipvs.Destination) error {
	args := f.Called(s, destination)
	return args.Error(0)
}

func newRoutineContext(services map[string]*Service, ipvs ipvs.Client) *Context {
	c := newContext(ipvs, &fakeDisco{})
	c.services = services
	return c
}

func newContext(ipvs ipvs.Client, disco disco.Driver) *Context {
	return &Context{
		ipvs:     ipvs,
		endpoint: net.ParseIP("127.0.0.1"),
		services: map[string]*Service{},
		pulseCh:  make(chan pulse.Update),
		stopCh:   make(chan struct{}),
		disco:    disco,
	}
}

var (
	vsID                   = "virtualServiceId"
	rsID                   = "realServerID"
	virtualService         = Service{options: &ServiceOptions{Port: 80, Host: "localhost", Protocol: "tcp", LbMethod: "sh"}}
	virtualServiceFallBack = Service{options: &ServiceOptions{Port: 80, Host: "localhost", Protocol: "tcp", Fallback: "fb-zero-to-one"}}
	serviceConfig          = ServiceConfig{
		ServiceOptions:  &ServiceOptions{Port: 80, Host: "localhost", Protocol: "tcp", LbMethod: "sh", ShFlags: "sh-port|sh-fallback"},
		ServiceBackends: map[string]*BackendOptions{},
	}
	svc = ipvs.Service{
		Address:   netip.MustParseAddr("127.0.0.1"),
		Port:      serviceConfig.ServiceOptions.Port,
		Netmask:   netmask.MaskFrom4([4]byte{255, 255, 255, 255}), // /32
		Scheduler: serviceConfig.ServiceOptions.LbMethod,
		Timeout:   0,
		Flags:     *new(ipvs.Flags),
		Family:    ipvs.INET,
		Protocol:  ipvs.TCP,
	}
	dst = ipvs.Destination{
		Address:   netip.MustParseAddr("127.0.0.1"),
		FwdMethod: ipvs.Tunnel,
		Weight:    0,
		Port:      123,
		Family:    ipvs.INET,
	}
)

func TestServiceIsCreated(t *testing.T) {
	options := serviceConfig
	options.ServiceOptions = virtualService.options
	mockIpvs := &fakeIpvs{}
	mockDisco := &fakeDisco{}
	c := newContext(mockIpvs, mockDisco)

	mockIpvs.On("CreateService", svc).Return(nil)
	mockIpvs.On("Service", svc).Return(nil, ErrIpvsSyscallFailed)
	mockDisco.On("Expose", vsID, "127.0.0.1", uint16(80)).Return(nil)

	err := c.createService(vsID, &options)
	assert.NoError(t, err)
	mockIpvs.AssertExpectations(t)
	mockDisco.AssertExpectations(t)
}

func TestPulseUpdateSetsBackendWeightToZeroOnStatusDown(t *testing.T) {
	stash := make(map[pulse.ID]uint32)
	backends := map[string]*Backend{rsID: &Backend{service: &virtualService, options: &BackendOptions{weight: 100}}}
	services := map[string]*Service{vsID: &virtualService}
	services[vsID].backends = backends
	mockIpvs := &fakeIpvs{}

	c := newRoutineContext(services, mockIpvs)

	mockIpvs.On("UpdateDestination", mock.Anything, mock.Anything).Return(nil)

	c.processPulseUpdate(stash, pulse.Update{pulse.ID{VsID: vsID, RsID: rsID}, pulse.Metrics{Status: pulse.StatusDown}})
	assert.Equal(t, len(stash), 1)
	assert.Equal(t, stash[pulse.ID{VsID: vsID, RsID: rsID}], uint32(100))
	mockIpvs.AssertExpectations(t)
}

func TestPulseUpdateSetsBackendWeightWithFallBackZeroToOne(t *testing.T) {
	stash := make(map[pulse.ID]uint32)
	backends := map[string]*Backend{rsID: &Backend{service: &virtualService, options: &BackendOptions{weight: 100}}}
	services := map[string]*Service{vsID: &virtualServiceFallBack}
	services[vsID].backends = backends
	mockIpvs := &fakeIpvs{}

	c := newRoutineContext(services, mockIpvs)

	mockIpvs.On("UpdateDestination", mock.Anything, mock.Anything).Return(nil)

	c.processPulseUpdate(stash, pulse.Update{pulse.ID{VsID: vsID, RsID: rsID}, pulse.Metrics{Status: pulse.StatusDown}})
	assert.Equal(t, len(stash), 1)
	assert.Equal(t, stash[pulse.ID{VsID: vsID, RsID: rsID}], uint32(100))
	mockIpvs.AssertExpectations(t)
}

func TestPulseUpdateIncreasesBackendWeightRelativeToTheHealthOnStatusUp(t *testing.T) {
	stash := map[pulse.ID]uint32{pulse.ID{VsID: vsID, RsID: rsID}: uint32(12)}
	backends := map[string]*Backend{rsID: &Backend{service: &virtualService, options: &BackendOptions{}}}
	services := map[string]*Service{vsID: &virtualService}
	services[vsID].backends = backends
	mockIpvs := &fakeIpvs{}

	c := newRoutineContext(services, mockIpvs)

	mockIpvs.On("UpdateDestination", mock.Anything, mock.Anything).Return(nil)

	c.processPulseUpdate(stash, pulse.Update{pulse.ID{VsID: vsID, RsID: rsID}, pulse.Metrics{Status: pulse.StatusUp, Health: 0.5}})
	assert.Equal(t, len(stash), 1)
	assert.Equal(t, stash[pulse.ID{VsID: vsID, RsID: rsID}], uint32(12))
	mockIpvs.AssertExpectations(t)
}

func TestPulseUpdateRemovesStashWhenBackendHasFullyRecovered(t *testing.T) {
	stash := map[pulse.ID]uint32{pulse.ID{VsID: vsID, RsID: rsID}: uint32(12)}
	backends := map[string]*Backend{rsID: &Backend{service: &virtualService, options: &BackendOptions{}}}
	services := map[string]*Service{vsID: &virtualService}
	services[vsID].backends = backends
	mockIpvs := &fakeIpvs{}

	c := newRoutineContext(services, mockIpvs)

	mockIpvs.On("UpdateDestination", mock.Anything, mock.Anything).Return(nil)

	c.processPulseUpdate(stash, pulse.Update{pulse.ID{VsID: vsID, RsID: rsID}, pulse.Metrics{Status: pulse.StatusUp, Health: 1}})
	assert.Empty(t, stash)
	mockIpvs.AssertExpectations(t)
}

func TestPulseUpdateRemovesStashWhenBackendIsDeleted(t *testing.T) {
	stash := map[pulse.ID]uint32{pulse.ID{VsID: vsID, RsID: rsID}: uint32(0)}
	backends := make(map[string]*Backend)
	services := map[string]*Service{vsID: &virtualService}
	services[vsID].backends = backends
	mockIpvs := &fakeIpvs{}

	c := newRoutineContext(services, mockIpvs)
	c.processPulseUpdate(stash, pulse.Update{pulse.ID{VsID: vsID, RsID: rsID}, pulse.Metrics{}})

	assert.Empty(t, stash)
	mockIpvs.AssertExpectations(t)
}

func TestPulseUpdateRemovesStashWhenDeletedAfterNotification(t *testing.T) {
	stash := map[pulse.ID]uint32{pulse.ID{VsID: vsID, RsID: rsID}: uint32(0)}
	backends := map[string]*Backend{rsID: &Backend{service: &virtualService, options: &BackendOptions{}}}
	services := map[string]*Service{vsID: &virtualService}
	services[vsID].backends = backends
	mockIpvs := &fakeIpvs{}

	c := newRoutineContext(services, mockIpvs)
	c.processPulseUpdate(stash, pulse.Update{pulse.ID{VsID: vsID, RsID: rsID}, pulse.Metrics{Status: pulse.StatusRemoved}})

	assert.Empty(t, stash)
	mockIpvs.AssertExpectations(t)
}

func TestStatusDownDuringIncreasingWeight(t *testing.T) {
	stash := map[pulse.ID]uint32{pulse.ID{VsID: vsID, RsID: rsID}: uint32(100)}
	backends := map[string]*Backend{rsID: &Backend{service: &virtualService, options: &BackendOptions{}}}
	services := map[string]*Service{vsID: &virtualService}
	services[vsID].backends = backends
	mockIpvs := &fakeIpvs{}

	c := newRoutineContext(services, mockIpvs)

	mockIpvs.On("UpdateDestination", mock.Anything, mock.Anything).Return(nil)
	c.processPulseUpdate(stash, pulse.Update{pulse.ID{VsID: vsID, RsID: rsID}, pulse.Metrics{Status: pulse.StatusDown, Health: 0.5}})

	assert.Equal(t, len(stash), 1)
	assert.Equal(t, stash[pulse.ID{VsID: vsID, RsID: rsID}], uint32(100))
	mockIpvs.AssertExpectations(t)
}
