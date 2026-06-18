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

package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/cloudflare/ipvs"
	"github.com/qk4l/gorb/core"
	"github.com/qk4l/gorb/util"

	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"

	_ "net/http/pprof"
	"strings"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// Version get dynamically set to git rev by ldflags at build time
	Version = "0.5.1"

	debug        = flag.Bool("v", false, "enable verbose output")
	device       = flag.String("i", "eth0", "default interface to bind services on")
	flushOnExit  = flag.Bool("f", false, "flushOnExit IPVS pools on exit")
	listen       = flag.String("l", ":4672", "endpoint to listen for HTTP requests")
	consul       = flag.String("c", "", "URL for Consul HTTP API")
	vipInterface = flag.String("vipi", "", "interface to add VIPs")
	storeURLs    = flag.String("store", "", "comma delimited list of store urls for sync data. All urls must have"+
		" identical schemes and paths.")
	storeUseTLS      = flag.Bool("store-use-tls", false, "Use TLS to connect to store backend")
	storeSyncTime    = flag.Int64("store-sync-time", 60, "sync-time for store")
	storeServicePath = flag.String("store-service-path", "services", "store service path")
	storeBackendPath = flag.String("store-backend-path", "backends", "store backend path")
	tcpTimeout       = flag.Uint64("tcp-timeout", 28800, "ipvs TCP timeout in seconds")
	tcpFinTimeout    = flag.Uint64("tcpfin-timeout", 120, "ipvs TCP FIN timeout in seconds")
	udpTimeout       = flag.Uint64("udp-timeout", 300, "ipvs UDP timeout in seconds")
)

func main() {
	// Called first to interrupt bootstrap and display usage if the user passed -h.
	//config.InitConfig()
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, syscall.SIGINT, syscall.SIGTERM)

	flag.Parse()

	if *debug {
		log.SetLevel(log.DebugLevel)
	}

	log.Info("starting GORB Daemon v" + Version)

	if os.Geteuid() != 0 {
		log.Fatalf("this program has to be run with root priveleges to access IPVS")
	}

	hostIPs, err := util.InterfaceIPs(*device)

	if err != nil {
		log.Fatalf("error while obtaining interface addresses: %s", err)
	}

	listenAddr, err := net.ResolveTCPAddr("tcp", *listen)
	listenPort := uint16(0)

	if err != nil {
		log.Fatalf("error while obtaining listening port from '%s': %s", *listen, err)
	} else {
		listenPort = uint16(listenAddr.Port)
	}

	if *debug {
		go func() {
			log.Println(http.ListenAndServe(fmt.Sprintf("%s:6061", listenAddr.IP), nil))
		}()
	}

	ipvsOpt := core.IPVSOptions{
		ipvs.Config{
			TCPFinTimeout: uint32(*tcpFinTimeout),
			TCPTimeout:    uint32(*tcpTimeout),
			UDPTimeout:    uint32(*udpTimeout),
		},
	}

	ctx, err := core.NewContext(
		core.ContextOptions{
			Disco:        *consul,
			Endpoints:    hostIPs,
			FlushOnExit:  *flushOnExit,
			ListenPort:   listenPort,
			VipInterface: *vipInterface,
			IpvsOptions:  ipvsOpt,
		})

	if err != nil {
		log.Fatalf("error while initializing server context: %s", err)
	}

	// While it's not strictly required, close IPVS socket explicitly.
	defer ctx.Close()
	var store *core.Store
	// sync with external store
	if storeURLs != nil && len(*storeURLs) > 0 {
		urls := strings.Split(*storeURLs, ",")
		store, err = core.NewStore(urls, *storeServicePath, *storeBackendPath, *storeSyncTime, *storeUseTLS, ctx)
		if err != nil {
			log.Fatalf("error while initializing external store sync: %s", err)
		}
		defer store.Close()
	}

	core.RegisterPrometheusExporter(ctx)
	r := mux.NewRouter()

	r.Handle("/service/{vsID}", serviceCreateHandler{ctx}).Methods("PUT")
	r.Handle("/service/{vsID}/{rsID}", backendCreateHandler{ctx}).Methods("PUT")
	r.Handle("/service/{vsID}", serviceRemoveHandler{ctx}).Methods("DELETE")
	r.Handle("/service/{vsID}/{rsID}", backendRemoveHandler{ctx}).Methods("DELETE")
	r.Handle("/service", serviceListHandler{ctx}).Methods("GET")
	r.Handle("/service/{vsID}", serviceStatusHandler{ctx}).Methods("GET")
	r.Handle("/service/{vsID}/{rsID}", backendStatusHandler{ctx}).Methods("GET")
	r.Handle("/store/sync", storeSyncHandler{store}).Methods("GET")
	r.Handle("/store/sync/status", storeSyncStatusHandler{store}).Methods("GET")
	r.Handle("/metrics", promhttp.Handler()).Methods("GET")

	server := &http.Server{
		Addr:    *listen,
		Handler: r,
	}

	go func() {
		log.Infof("setting up HTTP server on %s", *listen)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("error while starting HTTP server: %s", err)
		}
	}()

	<-stopChan
	signal.Stop(stopChan)

	log.Info("shutting down...")
	if err = server.Shutdown(nil); err != nil {
		return
	}
}
