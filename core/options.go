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
	"net"
	"net/netip"
	"strings"

	"github.com/cloudflare/ipvs"
	"github.com/qk4l/gorb/pulse"
)

// Possible validation errors.
var (
	ErrMissingEndpoint     = errors.New("endpoint information is missing")
	ErrUnknownMethod       = errors.New("specified forwarding method is unknown")
	ErrUnknownProtocol     = errors.New("specified protocol is unknown")
	ErrUnknownFlag         = errors.New("specified flag is unknown")
	ErrUnknownFallbackFlag = errors.New("specified fallback flag is unknown")
)

// ContextOptions configure Context behavior.
type ContextOptions struct {
	Disco        string
	Endpoints    []net.IP
	FlushOnExit  bool
	ListenPort   uint16
	VipInterface string
	IpvsOptions  IPVSOptions
}

type IPVSOptions struct {
	Timeouts ipvs.Config
}

func (o *IPVSOptions) Compare(other *ipvs.Config) bool {
	if o.Timeouts.UDPTimeout != other.UDPTimeout ||
		o.Timeouts.TCPTimeout != other.TCPTimeout ||
		o.Timeouts.TCPFinTimeout != other.TCPFinTimeout {
		return false
	}

	return true
}

// ServiceOptions describe a virtual service.
type ServiceOptions struct {
	//service settings
	Host     string `json:"host" yaml:"host"`
	Port     uint16 `json:"port" yaml:"port"`
	Protocol string `json:"protocol" yaml:"protocol"`
	LbMethod string `json:"lb_method" yaml:"lb_method"`
	ShFlags  string `json:"sh_flags" yaml:"sh_flags"`

	// Persistent service settings
	Persistent bool   `json:"persistent" yaml:"persistent"`
	Timeout    uint32 `json:"timeout" yaml:"timeout"`

	Fallback string `json:"fallback" yaml:"fallback"`

	// service backends settings
	FwdMethod string         `json:"fwd_method" yaml:"fwd_method"`
	Pulse     *pulse.Options `json:"pulse" yaml:"pulse"`
	MaxWeight uint32         `json:"max_weight" yaml:"max_weight"`

	// Host string resolved to an IP, including DNS lookup.
	host      netip.Addr
	delIfAddr bool

	// Protocol string converted to a protocol number.
	protocol ipvs.Protocol

	// Forwarding method string converted to a forwarding method number.
	methodID ipvs.ForwardType
}

// Validate fills missing fields and validates virtual service configuration.
func (o *ServiceOptions) Validate(defaultHost netip.Addr) error {
	if o.Port == 0 {
		return ErrMissingEndpoint
	}

	if len(o.Host) != 0 {
		if addr, err := net.ResolveIPAddr("ip", o.Host); err == nil {
			o.host = netip.MustParseAddr(addr.IP.String())
		} else {
			return err
		}
	} else if !defaultHost.IsUnspecified() {
		o.host = defaultHost
	} else {
		return ErrMissingEndpoint
	}

	if len(o.Protocol) == 0 {
		o.Protocol = "tcp"
	}

	o.Protocol = strings.ToLower(o.Protocol)

	switch o.Protocol {
	case "tcp":
		o.protocol = ipvs.TCP
	case "udp":
		o.protocol = ipvs.UDP
	default:
		return ErrUnknownProtocol
	}

	if o.ShFlags != "" {
		for _, flag := range strings.Split(o.ShFlags, "|") {
			if _, ok := schedulerFlags[flag]; !ok {
				return ErrUnknownFlag
			}
		}
	}

	if o.Timeout == 0 {
		o.Timeout = 320
	}

	if o.Fallback != "" {
		for _, flag := range strings.Split(o.Fallback, "|") {
			if _, ok := fallbackFlags[flag]; !ok {
				return ErrUnknownFallbackFlag
			}
		}
	} else {
		o.Fallback = "fb-default"
	}

	if len(o.LbMethod) == 0 {
		// WRR since Pulse will dynamically reweight backends.
		o.LbMethod = "wrr"
	}

	if o.MaxWeight <= 0 {
		o.MaxWeight = 100
	}

	if len(o.FwdMethod) == 0 {
		o.FwdMethod = "nat"
	}

	o.FwdMethod = strings.ToLower(o.FwdMethod)

	switch o.FwdMethod {
	case "dr":
		o.methodID = ipvs.DirectRoute
	case "nat":
		o.methodID = ipvs.Masquerade
	case "tunnel", "ipip":
		o.methodID = ipvs.Tunnel
	default:
		return ErrUnknownMethod
	}

	if o.Pulse == nil {
		// It doesn't make much sense to have a backend with no Pulse.
		o.Pulse = &pulse.Options{}
	}

	return nil
}

// CompareStoreOptions compares two ServiceOptions.
// It is used to check if Service configuration has changed.
func (o *ServiceOptions) CompareStoreOptions(options *ServiceOptions) bool {
	if o.Host != options.Host {
		return false
	}
	if o.Port != options.Port {
		return false
	}
	if o.Protocol != options.Protocol {
		return false
	}
	if o.ShFlags != options.ShFlags {
		return false
	}
	if o.LbMethod != options.LbMethod {
		return false
	}
	if o.Persistent != options.Persistent {
		return false
	}
	if o.Timeout != options.Timeout {
		return false
	}
	if o.Fallback != options.Fallback {
		return false
	}
	if o.FwdMethod != options.FwdMethod {
		return false
	}
	if o.MaxWeight != options.MaxWeight {
		return false
	}
	if !o.Pulse.Compare(options.Pulse) {
		return false
	}
	return true
}

// BackendOptions describe a virtual service backend.
type BackendOptions struct {
	Host string `json:"host" yaml:"host"`
	Port uint16 `json:"port" yaml:"port"`
	// MaxWeight override service MaxWeight
	MaxWeight uint32 `json:"max_weight" yaml:"max_weight"`

	// vsID of backend
	vsID string
	// Host string resolved to an IP, including DNS lookup.
	host netip.Addr
	// Backend current weight
	weight uint32
}

// Validate fills missing fields and validates backend configuration.
func (o *BackendOptions) Validate(maxWeight uint32, port uint16) error {
	if len(o.Host) == 0 || port == 0 {
		return ErrMissingEndpoint
	}
	o.Port = port

	if addr, err := net.ResolveIPAddr("ip", o.Host); err == nil {
		o.host = netip.MustParseAddr(addr.IP.String())
	} else {
		return err
	}
	if o.MaxWeight == 0 {
		if maxWeight == 0 {
			o.MaxWeight = uint32(100)
		} else {
			o.MaxWeight = maxWeight
		}
	}
	o.weight = o.MaxWeight
	return nil
}

// CompareStoreOptions compares two BackendOptions.
// It is used to check if Backend configuration has changed.
func (o *BackendOptions) CompareStoreOptions(options *BackendOptions) bool {
	if o.Host != options.Host {
		return false
	}
	if o.MaxWeight != options.MaxWeight {
		return false
	}
	return true
}
