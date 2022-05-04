package main

import (
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// LogConnections will log messages about open/closed connections.
	LogConnections = 1
	// LogConnectionErrors will log errors encountered during backend dialing, other errors (after handshake).
	LogConnectionErrors = 2
	// LogHandshakeErrors will log errors with new connections before/during handshake.
	LogHandshakeErrors = 4
	// LogEverything will log all things.
	LogEverything = LogHandshakeErrors | LogConnectionErrors | LogConnections
)

// Logger is used by this package to log messages
type Logger interface {
	Printf(format string, v ...interface{})
}

// Dialer represents a function that can dial a backend/destination for forwarding connections.
type Dialer func() (net.Conn, error)

// Proxy will take incoming connections from a listener and forward them to
// a backend through the given dialer.
type Proxy struct {
	// Listener to accept connetions on.
	Listener net.Listener
	// ConnectTimeout after which connections are terminated.
	ConnectTimeout time.Duration
	// Dial function to reach backend to forward connections to.
	Dial Dialer
	// Logger is used to log information messages about connections, errors.
	Logger Logger

	// Internal state to indicate that we want to shut down.
	quit int32
	// Logging flags
	loggerFlags int
	// Enable HAproxy's PROXY protocol
	// see: https://www.haproxy.org/download/1.8/doc/proxy-protocol.txt
	proxyProtocol bool
	// Internal wait group to keep track of outstanding handlers.
	handlers *sync.WaitGroup
}

// New creates a new proxy.
func NewProxy(listener net.Listener, timeout time.Duration, dial Dialer, logger Logger, loggerFlags int) *Proxy {
	p := &Proxy{
		Listener:       listener,
		ConnectTimeout: timeout,
		Dial:           dial,
		Logger:         logger,
		quit:           0,
		loggerFlags:    loggerFlags,
		handlers:       &sync.WaitGroup{},
	}

	// Add one handler to the wait group, so that Wait() will always block until
	// Shutdown() is called even if the proxy hasn't started yet. This prevents
	// a race condition if someone calls Accept() in a Goroutine and then immediately
	// calls Wait() on the proxy object.
	p.handlers.Add(1)
	return p
}

// Shutdown tells the proxy to close the listener & stop accepting connections.
func (p *Proxy) Shutdown() {
	if atomic.LoadInt32(&p.quit) == 1 {
		return
	}
	atomic.StoreInt32(&p.quit, 1)
	p.Listener.Close()
	p.handlers.Done()
}

// Wait until the proxy is shut down (listener closed, connections drained).
// This function will block even if the proxy isn't in the accept loop yet,
// so it's safe to concurrently run Accept() in a Goroutine and then immediately
// call Wait().
func (p *Proxy) Wait() {
	p.handlers.Wait()
}

// Accept incoming connections and spawn Go routines to handle them and forward
// the data to the backend. Will stop accepting connections if Shutdown() is called.
// Run this in a Goroutine, call Wait() to block on proxy shutdown/connection drain.
func (p *Proxy) Accept() {
	for {
		// Wait for new connection
		conn, err := p.Listener.Accept()
		if err != nil {
			// Check if we're supposed to stop
			if atomic.LoadInt32(&p.quit) == 1 {
				return
			}

			continue
		}

		go func() {
			defer conn.Close()
			backend, err := p.Dial()
			if err != nil {
				p.logConditional(LogConnectionErrors, "error on dial: %s", err)
				return
			}

			p.handlers.Add(1)
			defer p.handlers.Done()
			p.fuse(conn, backend)
		}()
	}
}

// Fuse connections together
func (p *Proxy) fuse(client, backend net.Conn) {
	// Copy from client -> backend, and from backend -> client
	defer p.logConnectionMessage("closed", client, backend)
	p.logConnectionMessage("opening", client, backend)

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() { p.copyData(client, backend); wg.Done() }()
	p.copyData(backend, client)
	wg.Wait()
}

// Copy data between two connections
func (p *Proxy) copyData(dst net.Conn, src net.Conn) {
	defer dst.Close()
	defer src.Close()

	_, err := io.Copy(dst, src)

	if err != nil && !isClosedConnectionError(err) {
		// We don't log individual "read from closed connection" errors, because
		// we already have a log statement showing that a pipe has been closed.
		p.logConditional(LogConnectionErrors, "error during copy: %s", err)
	}
}

// Log information message about connection
func (p *Proxy) logConnectionMessage(action string, dst net.Conn, src net.Conn) {
	p.logConditional(
		LogConnections,
		"%s pipe: %s:%s [%s] <-> %s:%s [%s]",
		action,
		dst.RemoteAddr().Network(),
		dst.RemoteAddr().String(),
		src.RemoteAddr().Network(),
		src.RemoteAddr().String(),
	)
}

func (p *Proxy) logConditional(flag int, msg string, args ...interface{}) {
	if (p.loggerFlags & flag) > 0 {
		p.Logger.Printf(msg, args...)
	}
}

func isClosedConnectionError(err error) bool {
	if e, ok := err.(*net.OpError); ok {
		return e.Op == "read" && strings.Contains(err.Error(), "closed network connection")
	}
	return false
}
