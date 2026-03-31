/*
   Copyright (c) 2015 Andrey Sibiryov <me@kobology.ru>
   Copyright (c) 2015 Other contributors as noted in the AUTHORS file.

   This file is part of GORB - Go Routing and Balancing.

   GORB is free software; you can redistribute it and/or modify
   it under the terms of the GNU Lesser General Public License as published by
   the Free Software Foundation; either version 3 of the License, or
   (at your option) any later version.

   GORB is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public License
   along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package core

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"strconv"
	"sync"

	"strings"

	"github.com/cloudflare/ipvs/netmask"
	"github.com/qk4l/gorb/disco"
	"github.com/qk4l/gorb/pulse"
	"github.com/qk4l/gorb/util"
	"github.com/vishvananda/netlink"

	"github.com/cloudflare/ipvs"
	log "github.com/sirupsen/logrus"
)

// Service flags
var (
	schedulerFlags = map[string]ipvs.Flags{
		"srv-persistent": ipvs.ServicePersistent,
		"srv-one-packet": ipvs.ServiceOnePacket,
		"srv-hashed":     ipvs.ServiceHashed,
		"sh-fallback":    ipvs.ServiceSchedulerOpt1, // Source Hashing sched flag
		"sh-port":        ipvs.ServiceSchedulerOpt2, // Source Hashing sched flag
		"flag-1":         ipvs.ServiceSchedulerOpt1,
		"flag-2":         ipvs.ServiceSchedulerOpt2,
		"flag-3":         ipvs.ServiceSchedulerOpt3,
	}
	fallbackFlags = map[string]int16{
		"fb-default":     Default,
		"fb-zero-to-one": ZeroToOne,
	}
)

// Possible runtime errors.
var (
	ErrIpvsSyscallFailed = errors.New("error while calling into IPVS")
	ErrObjectExists      = errors.New("specified object already exists")
	ErrObjectNotFound    = errors.New("unable to locate specified object")
	ErrIncompatibleAFs   = errors.New("incompatible address families")
)

// Fallback options
const (
	// Default - Set 0 weight to failed backend
	Default int16 = iota
	// ZeroToOne - Set weight 1 to all if all backends have StatusDown
	ZeroToOne
)

// Context abstacts away the underlying IPVS bindings implementation.
type Context struct {
	vipInterface netlink.Link
	flushOnExit  bool
	endpoint     net.IP
	mutex        sync.RWMutex

	services map[string]*Service

	disco disco.Driver
	store *Store
	ipvs  ipvs.Client

	pulseCh chan pulse.Update
	stopCh  chan struct{}
}

func initIPVSClient(options ContextOptions) (*ipvs.Client, error) {
	client, err := ipvs.New()
	if err != nil {
		log.Errorf("%v: %v", ErrIpvsSyscallFailed, err)
		return nil, err
	}
	timeouts, err := client.Config()
	if err != nil {
		log.Errorf("%v: %v", ErrIpvsSyscallFailed, err)
		return &client, err
	}
	log.Infof("IPVS config: %+v", timeouts)
	if !options.IpvsOptions.Compare(&timeouts) {
		log.Infof("IPVS config is different from the one provided, update")
		if err := client.SetConfig(options.IpvsOptions.Timeouts); err != nil {
			log.Errorf("%v: %v", ErrIpvsSyscallFailed, err)
			return &client, err
		}
	}
	return &client, nil
}

// NewContext creates a new Context and initializes IPVS.
func NewContext(options ContextOptions) (*Context, error) {
	log.Info("initializing IPVS context")
	client, err := initIPVSClient(options)
	if err != nil {
		return nil, err
	}

	ctx := &Context{
		ipvs:        *client,
		flushOnExit: options.FlushOnExit,
		services:    make(map[string]*Service),
		pulseCh:     make(chan pulse.Update),
		stopCh:      make(chan struct{}),
	}

	if len(options.Disco) > 0 {
		log.Infof("creating Consul client with Agent URL: %s", options.Disco)
		ctx.disco, err = disco.New(&disco.Options{
			Type: "consul",
			Args: util.DynamicMap{"URL": options.Disco}})

		if err != nil {
			return nil, err
		}
	} else {
		ctx.disco, _ = disco.New(&disco.Options{Type: "none"})
	}

	if len(options.Endpoints) > 0 {
		// TODO(@kobolog): Bind virtual services on multiple endpoints.
		ctx.endpoint = options.Endpoints[0]
		if options.ListenPort != 0 {
			log.Info("Registered the REST service to Consul.")
			ctx.disco.Expose("gorb", ctx.endpoint.String(), options.ListenPort)
		}
	}

	if options.VipInterface != "" {
		var err error
		if ctx.vipInterface, err = netlink.LinkByName(options.VipInterface); err != nil {
			ctx.Close()
			return nil, fmt.Errorf(
				"unable to find the interface '%s' for VIPs: %s",
				options.VipInterface, err)
		}
		log.Infof("VIPs will be added to interface '%s'", ctx.vipInterface.Attrs().Name)
	}

	// Fire off a pulse notifications sink goroutine.
	go ctx.run()

	return ctx, nil
}

// Close shuts down IPVS and closes the Context.
func (ctx *Context) Close() {
	log.Info("shutting down IPVS context")

	// This will also shutdown the pulse notification sink goroutine.
	close(ctx.stopCh)

	if ctx.flushOnExit {
		for vsID := range ctx.services {
			ctx.RemoveService(vsID)
		}
	}
}

// GetIpvsService extract service from ipvs
func (ctx *Context) GetIpvsService(svc *ipvs.Service) (ipvs.ServiceExtended, error) {
	service, err := ctx.ipvs.Service(*svc)
	if err != nil {
		log.Errorf("Failed to get pools from ipvs: %s", err)
		return ipvs.ServiceExtended{}, ErrIpvsSyscallFailed
	}

	return service, nil
}

// GetIpvsServiceBackends extract service destinations from ipvs
func (ctx *Context) GetIpvsServiceBackends(svc *ipvs.Service) ([]ipvs.DestinationExtended, error) {
	backends, err := ctx.ipvs.Destinations(*svc)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			log.Warnf("No destinations for service %s:%d", svc.Address.String(), svc.Port)
			return make([]ipvs.DestinationExtended, 0), nil
		}
		log.Errorf("Failed to get pools from ipvs: %s", err)
		return make([]ipvs.DestinationExtended, 0), ErrIpvsSyscallFailed
	}
	log.Debugf("Found %d destinations", len(backends))

	return backends, nil
}

// CreateService registers a new virtual service with IPVS.
func (ctx *Context) createService(vsID string, serviceConfig *ServiceConfig) error {
	serviceOptions := serviceConfig.ServiceOptions
	if err := serviceOptions.Validate(netip.MustParseAddr(ctx.endpoint.String())); err != nil {
		return err
	}

	if _, exists := ctx.services[vsID]; exists {
		return ErrObjectExists
	}

	if ctx.vipInterface != nil {
		ifName := ctx.vipInterface.Attrs().Name
		vip := &netlink.Addr{IPNet: &net.IPNet{
			IP: net.ParseIP(serviceOptions.host.String()), Mask: net.IPv4Mask(255, 255, 255, 255)}}
		if err := netlink.AddrAdd(ctx.vipInterface, vip); err != nil {
			log.Infof(
				"failed to add VIP %s to interface '%s' for service [%s]: %s",
				serviceOptions.host, ifName, vsID, err)
		} else {
			serviceOptions.delIfAddr = true
		}
		log.Infof("VIP %s has been added to interface '%s'", serviceOptions.host, ifName)
	}

	log.Infof("creating virtual service [%s] on %s:%d", vsID, serviceOptions.host,
		serviceOptions.Port)

	var flags ipvs.Flags
	for _, flag := range strings.Split(serviceOptions.ShFlags, "|") {
		flags = flags | schedulerFlags[flag]
	}

	var svc = ipvs.Service{
		Address:   netip.MustParseAddr(serviceOptions.host.String()),
		Port:      serviceOptions.Port,
		Netmask:   netmask.MaskFrom4([4]byte{255, 255, 255, 255}), // /32
		Scheduler: serviceOptions.LbMethod,
		Timeout:   serviceOptions.Timeout,
		Flags:     flags,
		Family:    ipvs.INET,
		Protocol:  serviceOptions.protocol,
	}

	if _, err := ctx.GetIpvsService(&svc); err == nil {
		log.Infof("Service %s:%d already existed skip creation", svc.Address.String(), svc.Port)
	} else {
		log.Infof("Service %s:%d does not exist, create", svc.Address.String(), svc.Port)
		if err := ctx.ipvs.CreateService(svc); err != nil {
			log.Errorf("error while creating virtual service: %s", err)
			return ErrIpvsSyscallFailed
		}
	}

	if _, ok := ctx.services[vsID]; !ok {
		ctx.services[vsID] = &Service{vsID: vsID, options: serviceOptions, svc: svc, backends: make(map[string]*Backend)}
	}

	if err := ctx.disco.Expose(vsID, serviceOptions.host.String(), serviceOptions.Port); err != nil {
		log.Errorf("error while exposing service to Disco: %s", err)
	}

	// init backends
	for rsID, backendOpts := range serviceConfig.ServiceBackends {
		err := ctx.createBackend(vsID, rsID, backendOpts)
		if err != nil {
			return err
		}
	}

	return nil
}

// CreateService registers a new virtual service with IPVS.
func (ctx *Context) CreateService(vsID string, serviceConfig *ServiceConfig) error {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	return ctx.createService(vsID, serviceConfig)
}

// CreateBackend registers a new backend with a virtual service.
func (ctx *Context) createBackend(vsID, rsID string, opts *BackendOptions) error {
	var skipCreation bool

	// Validate input
	vs, exists := ctx.services[vsID]
	if !exists {
		return fmt.Errorf("%w vsID: %s", ErrObjectNotFound, vsID)
	}
	if vs.BackendExist(rsID) {
		return fmt.Errorf("%w rsID: %s", ErrObjectExists, rsID)
	}
	if err := opts.Validate(vs.options.MaxWeight, vs.options.Port); err != nil {
		return err
	}

	if opts.host.Is4() != vs.options.host.Is4() {
		return ErrIncompatibleAFs
	}

	log.Infof("creating backend [%s] on %s:%d for virtual service [%s]",
		rsID,
		opts.host,
		opts.Port,
		vsID)

	var newDest = ipvs.Destination{
		Address:   netip.MustParseAddr(opts.host.String()),
		Port:      opts.Port,
		FwdMethod: vs.options.methodID,
		Weight:    opts.MaxWeight,
		Family:    ipvs.INET,
	}

	pool, err := ctx.GetIpvsServiceBackends(&vs.svc)
	if err != nil {
		log.Errorf("Failed to get pool for service [%s]: %s", vs.svc.Address.String(), err)
		return ErrIpvsSyscallFailed
	}

	for _, dest := range pool {
		if dest.Address.Compare(newDest.Address) == 0 && dest.Port == newDest.Port {
			log.Infof(
				"Backend %s:%d already existed in service [%s]. Skip creation",
				newDest.Address.String(), newDest.Port, vsID)
			skipCreation = true
		}
	}

	if !skipCreation {
		if err := ctx.ipvs.CreateDestination(
			vs.svc,
			newDest,
		); err != nil {
			log.Errorf("error while creating backend [%s/%s]: %s", vsID, rsID, err)
			return ErrIpvsSyscallFailed
		}
	}

	err = vs.CreateBackend(rsID, opts)
	if err != nil {
		return err
	}

	// Fire off the configured pulse goroutine, attach it to the Context.
	go vs.backends[rsID].monitor.Loop(pulse.ID{VsID: vsID, RsID: rsID}, ctx.pulseCh, ctx.stopCh)

	return nil
}

// CreateBackend registers a new backend with a virtual service.
func (ctx *Context) CreateBackend(vsID, rsID string, opts *BackendOptions) error {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	return ctx.createBackend(vsID, rsID, opts)
}

// UpdateBackend updates the specified backend's weight.
func (ctx *Context) updateBackend(vsID, rsID string, weight uint32) (uint32, error) {

	vs, exists := ctx.services[vsID]
	if !exists {
		return 0, fmt.Errorf("%w vsID: %s", ErrObjectNotFound, vsID)
	}
	rs, exists := vs.backends[rsID]
	if !exists {
		return 0, ErrObjectNotFound
	}

	log.Infof("updating backend [%s/%s] with weight: %d", vsID, rsID,
		weight)

	if err := ctx.ipvs.UpdateDestination(
		vs.svc,
		ipvs.Destination{
			Address:   rs.options.host,
			Port:      rs.options.Port,
			FwdMethod: vs.options.methodID,
			Weight:    weight,
			Family:    ipvs.INET,
		}); err != nil {
		log.Errorf("error while updating backend [%s/%s]", vsID, rsID)
		return 0, ErrIpvsSyscallFailed
	}

	// Save the old backend weight and update the current backend weight.
	prevWeight := rs.UpdateWeight(weight)

	// Currently the backend options are changing only the weight.
	// The weight value is set to the value requested at the first setting,
	// and the weight value is updated when the pulse fails in the gorb.
	// In kvstore, it seems correct to record the request at the first setting and
	// not reflect the updated weight value.
	//if ctx.store != nil {
	//	ctx.store.UpdateBackend(vsID, rsID, rs.options)
	//}

	return prevWeight, nil
}

// UpdateBackend updates the specified backend's weight.
func (ctx *Context) UpdateBackend(vsID, rsID string, weight uint32) (uint32, error) {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	return ctx.updateBackend(vsID, rsID, weight)
}

// RemoveService deregisters a virtual service.
func (ctx *Context) removeService(vsID string) (*ServiceOptions, error) {
	vs, exists := ctx.services[vsID]
	if !exists {
		return nil, fmt.Errorf("%w vsID: %s", ErrObjectNotFound, vsID)
	}

	if ctx.vipInterface != nil && vs.options.delIfAddr {
		ifName := ctx.vipInterface.Attrs().Name
		vip := &netlink.Addr{IPNet: &net.IPNet{
			IP: net.ParseIP(vs.options.host.String()), Mask: net.IPv4Mask(255, 255, 255, 255)}}
		if err := netlink.AddrDel(ctx.vipInterface, vip); err != nil {
			log.Infof(
				"failed to delete VIP %s to interface '%s' for service [%s]: %s",
				vs.options.host, ifName, vsID, err)
		}
		log.Infof("VIP %s has been deleted from interface '%s'", vs.options.host, ifName)
	}

	log.Infof("removing virtual service [%s] from %s:%d", vsID,
		vs.options.host,
		vs.options.Port)

	if err := ctx.ipvs.RemoveService(vs.svc); err != nil {
		log.Errorf("error while removing virtual service [%s] from ipvs: %s", vsID, err)
		return nil, ErrIpvsSyscallFailed
	}

	delete(ctx.services, vsID)
	vs.Cleanup()

	// TODO(@kobolog): This will never happen in case of gorb-link.
	if err := ctx.disco.Remove(vsID); err != nil {
		log.Errorf("error while removing service from Disco: %s", err)
	}

	return vs.options, nil
}

// RemoveService deregisters a virtual service.
func (ctx *Context) RemoveService(vsID string) (*ServiceOptions, error) {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	return ctx.removeService(vsID)
}

// RemoveBackend deregisters a backend.
func (ctx *Context) removeBackend(vsID, rsID string) (*BackendOptions, error) {
	vs, exist := ctx.services[vsID]
	if !exist {
		return nil, fmt.Errorf("%w vsID: %s", ErrObjectNotFound, vsID)
	}
	rs, exists := vs.backends[rsID]
	if !exists {
		return nil, ErrObjectNotFound
	}

	log.Infof("removing backend [%s/%s]", vsID, rsID)
	if err := ctx.ipvs.RemoveDestination(
		vs.svc,
		ipvs.Destination{
			Address:   rs.options.host,
			Port:      rs.options.Port,
			Family:    ipvs.INET,
			FwdMethod: vs.options.methodID,
		}); err != nil {
		log.Errorf("error while removing backend [%s/%s] form ipvs: %s", vsID, rsID, err)
		return nil, ErrIpvsSyscallFailed
	}

	return vs.RemoveBackend(rsID)
}

// RemoveBackend deregisters a backend.
func (ctx *Context) RemoveBackend(vsID, rsID string) (*BackendOptions, error) {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	return ctx.removeBackend(vsID, rsID)
}

// ListServices returns a list of all registered services.
func (ctx *Context) ListServices() ([]string, error) {
	ctx.mutex.RLock()
	defer ctx.mutex.RUnlock()

	r := make([]string, 0, len(ctx.services))

	for vsID := range ctx.services {
		r = append(r, vsID)
	}

	return r, nil
}

// ServiceInfo contains information about virtual service options,
// its backends and overall virtual service health.
type ServiceInfo struct {
	Options       *ServiceOptions `json:"options"`
	Health        float64         `json:"health"`
	Backends      []string        `json:"backends"`
	BackendsCount uint16          `json:"backends_count"`
	FallBack      string          `json:"fallback"`
}

// GetService returns information about a virtual service.
func (ctx *Context) GetService(vsID string) (*ServiceInfo, error) {
	ctx.mutex.RLock()
	defer ctx.mutex.RUnlock()

	vs, exists := ctx.services[vsID]

	if !exists {
		return nil, ErrObjectNotFound
	}
	serviceStats := vs.CalcServiceStat()

	return serviceStats, nil
}

// BackendInfo contains information about backend options and pulse.
type BackendInfo struct {
	Options *BackendOptions `json:"options"`
	Metrics pulse.Metrics   `json:"metrics"`
	Weight  uint32          `json:"weight"`
}

// GetBackend returns information about a backend.
func (ctx *Context) GetBackend(vsID, rsID string) (*BackendInfo, error) {
	ctx.mutex.RLock()
	defer ctx.mutex.RUnlock()

	vs, exists := ctx.services[vsID]
	if !exists {
		return nil, fmt.Errorf("%w vsID: %s", ErrObjectNotFound, vsID)
	}

	rs, exists := vs.backends[rsID]
	if !exists {
		return nil, fmt.Errorf("%w rsID: %s", ErrObjectNotFound, rsID)
	}

	return &BackendInfo{rs.options, rs.metrics, rs.options.weight}, nil
}

// SetStore if external kvstore exists, set store to context
func (ctx *Context) SetStore(store *Store) {
	ctx.store = store
}

// StoreExist Checks if store set
func (ctx *Context) StoreExist() bool {
	if ctx.store == nil {
		return false
	}
	return true
}

func (ctx *Context) CompareWith(storeServices map[string]*ServiceConfig) *StoreSyncStatus {
	ctx.mutex.RLock()
	defer ctx.mutex.RUnlock()
	syncStatus := &StoreSyncStatus{}

	// find removed services in store
	for vsID, service := range ctx.services {
		if storeServiceOptions, ok := storeServices[vsID]; !ok {
			log.Debugf("service [%s] not found in store", vsID)
			syncStatus.RemovedServices = append(syncStatus.RemovedServices, vsID)
		} else {
			// find updated services in store
			if !service.options.CompareStoreOptions(storeServiceOptions.ServiceOptions) {
				log.Debugf("service [%s] is outdated.", vsID)
				syncStatus.UpdatedServices = append(syncStatus.UpdatedServices, vsID)
			}
			for rsID, backend := range service.backends {
				backendName := fmt.Sprintf("[%s/%s]", vsID, rsID)
				if storeBackendOptions, ok := storeServiceOptions.ServiceBackends[rsID]; !ok {
					log.Debugf("backend %s not found in store", backendName)
					syncStatus.RemovedBackends = append(syncStatus.RemovedBackends, backendName)
				} else {
					storeBackendOptions.Validate(service.options.MaxWeight, service.options.Port)
					// find updated backends
					if !backend.options.CompareStoreOptions(storeBackendOptions) {
						log.Debugf("backend %s is outdated.", backendName)
						syncStatus.UpdatedBackends = append(syncStatus.UpdatedBackends, backendName)
					}
					delete(storeServiceOptions.ServiceBackends, rsID)
				}
			}
			// find new Backends
			for rsID, storeBackend := range storeServiceOptions.ServiceBackends {
				backendName := fmt.Sprintf("[%s/%s]", storeBackend.vsID, rsID)
				log.Debugf("new backend %s found.", backendName)
				syncStatus.NewBackends = append(syncStatus.NewBackends, backendName)
			}
			delete(storeServices, vsID)
		}
	}

	// find new services
	for id, _ := range storeServices {
		log.Debugf("new service [%s] found.", id)
		syncStatus.NewServices = append(syncStatus.NewServices, id)
	}

	syncStatus.Status = syncStatus.CheckStatus()
	return syncStatus
}

func (ctx *Context) synchronizeWithStore(storeServicesConfig map[string]*ServiceConfig) error {
	defer log.Info("============================ END STORE SYNC ============================")
	log.Info("============================== STORE SYNC ==============================")
	log.Debug("external store content")
	for vsID, service := range storeServicesConfig {
		log.Debugf("SERVICE[%s]: %#v", vsID, service)
	}

	log.Info("sync services")
	// synchronize services with store
	for vsID, service := range ctx.services {
		if storeService, ok := storeServicesConfig[vsID]; !ok {
			log.Debugf("service [%s] not found. removing", vsID)
			if _, err := ctx.removeService(vsID); err != nil {
				return err
			}
		} else {
			if !service.options.CompareStoreOptions(storeService.ServiceOptions) {
				if _, err := ctx.removeService(vsID); err != nil {
					return err
				}
				if err := ctx.createService(vsID, storeService); err != nil {
					return err
				}
				service = ctx.services[vsID]
			}
			for rsID, backend := range service.backends {
				if storeBackendOptions, ok := storeService.ServiceBackends[rsID]; !ok {
					log.Debugf("backend [%s/%s] not found in store", vsID, rsID)
					if _, err := ctx.removeBackend(vsID, rsID); err != nil {
						return err
					}
				} else {
					// find updated backends
					storeBackendOptions.Validate(service.options.MaxWeight, service.options.Port)
					if !backend.options.CompareStoreOptions(storeBackendOptions) {
						log.Debugf("backend [%s/%s] is outdated.", vsID, rsID)
						if _, err := ctx.removeBackend(vsID, rsID); err != nil {
							return err
						}
						if err := ctx.createBackend(vsID, rsID, storeBackendOptions); err != nil {
							return err
						}

					}
					delete(storeService.ServiceBackends, rsID)
				}
			}

			if len(storeService.ServiceBackends) > 0 {
				log.Infof("create new backends for [%s]. count: %d", vsID, len(storeService.ServiceBackends))
				for rsID, storeBackendOptions := range storeService.ServiceBackends {
					if err := ctx.createBackend(vsID, rsID, storeBackendOptions); err != nil {
						return err
					}
				}
			}
			delete(storeServicesConfig, vsID)
		}
	}
	if len(storeServicesConfig) > 0 {
		log.Infof("create new services. count: %d", len(storeServicesConfig))
		for id, storeServiceOptions := range storeServicesConfig {
			if err := ctx.createService(id, storeServiceOptions); err != nil {
				return err
			}
		}
	}

	log.Info("Successfully synced with store")
	return nil
}

func (ctx *Context) synchronizeWithIPVS() error {
	defer log.Info("============================ END IPVS SYNC ============================")
	log.Info("============================== IPVS SYNC ==============================")
	vsID := strings.Builder{}
	rsID := strings.Builder{}

	ipvsService, err := ctx.ipvs.Services()
	if err != nil {
		return err
	}

	for _, service := range ipvsService {
		vsID.Reset()
		vsID.WriteString(service.Service.Address.String())
		vsID.WriteString(":")
		vsID.WriteString(strconv.Itoa(int(service.Service.Port)))

		srv, ok := ctx.services[vsID.String()]
		if !ok {
			log.Infof("service [%s] not found in context. removing", vsID.String())
			if err := ctx.ipvs.RemoveService(service.Service); err != nil {
				log.Errorf("error while removing service [%s] from ipvs: %s", vsID.String(), err)
				return err
			}
			continue
		}
		log.Infof("service [%s] found in context. updating", vsID.String())
		err := ctx.ipvs.UpdateService(srv.svc)
		if err != nil {
			log.Errorf("error while updating service [%s] from ipvs: %s", vsID.String(), err)
			return err
		}

		ipvsBackends, err := ctx.GetIpvsServiceBackends(&srv.svc)
		if err != nil {
			log.Errorf("error while getting backends of service [%s] from ipvs: %s", vsID.String(), err)
			return err
		}
		for _, backend := range ipvsBackends {
			rsID.Reset()
			rsID.WriteString(backend.Address.String())
			rsID.WriteString(":")
			rsID.WriteString(strconv.Itoa(int(backend.Port)))

			if _, ok := srv.backends[rsID.String()]; !ok {
				log.Infof("backend [%s/%s] not found in context. removing", vsID.String(), rsID.String())
				if err := ctx.ipvs.RemoveDestination(srv.svc, backend.Destination); err != nil {
					log.Errorf("error while removing backend [%s:%s] from ipvs: %s", vsID.String(), rsID.String(), err)
					return err
				}
			}

		}
	}
	return nil
}

func (ctx *Context) Synchronize(storeServicesConfig map[string]*ServiceConfig) error {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	err := ctx.synchronizeWithStore(storeServicesConfig)
	if err != nil {
		return err
	}

	err = ctx.synchronizeWithIPVS()
	if err != nil {
		return err
	}

	return nil
}
