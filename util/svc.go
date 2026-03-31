package util

import "github.com/cloudflare/ipvs"

func CompareSvc(first ipvs.Service, second ipvs.Service) bool {
	if first.Netmask != second.Netmask {
		return false
	}
	if first.Port != second.Port {
		return false
	}
	if first.Scheduler != second.Scheduler {
		return false
	}
	if first.Flags != second.Flags {
		return false
	}
	if first.Address.Compare(second.Address) != 0 {
		return false
	}
	if first.Protocol != second.Protocol {
		return false
	}
	return true
}
