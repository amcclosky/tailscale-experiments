package main

import (
	"flag"
	"log"
	"net"
	"strings"
	"time"

	"tailscale.com/ipn/store"
	"tailscale.com/tsnet"
	"tailscale.com/types/logger"
)

var (
	addr = flag.String("addr", ":80", "address to listen on")
)

func main() {
	var logf logger.Logf = log.Printf
	flag.Parse()
	s := new(tsnet.Server)
	s.Hostname = "db-proxy"
	s.Ephemeral = true

	store, err := store.New(logf, "mem:")
	if err != nil {
		log.Fatal(err)
	}

	s.Store = store

	ln, err := s.Listen("tcp", ":5432")
	if err != nil {
		log.Fatal(err)
	}

	outboundDialer := func() (net.Conn, error) {
		return net.Dial("tcp", "127.0.0.1:5439")
	}

	p := NewProxy(ln, 60*time.Second, outboundDialer, logger.StdLogger(log.Printf), LogEverything)
	go func() {
		p.Accept()
	}()
	defer p.Shutdown()
	p.Wait()
}

func firstLabel(s string) string {
	if i := strings.Index(s, "."); i != -1 {
		return s[:i]
	}
	return s
}
