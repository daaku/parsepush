// Package parsepush provides a client for receiving pushes from the Parse Push
// API. This is useful for building services that can receive pushes. With Go
// being usable on certain embedded devices, this can serve as a useful library
// for IoT devices.
package parsepush

import (
	"bufio"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/facebookgo/clock"
)

const (
	defaultAddr          = "push.parse.com:443"
	defaultPingInterval  = 10 * time.Minute
	defaultDialerTimeout = time.Minute
	defaultRetryInterval = 10 * time.Second
)

var (
	errMissingApplicationID  = errors.New("parsepush: missing Application ID")
	errMissingInstallationID = errors.New("parsepush: missing Installation ID")
	errMissingPushHandler    = errors.New("parsepush: missing Push Handler")

	pingMessage = []byte("{}\r\n")
)

// A payload contains the push payload. It includes a timestamp (which should be
// persisted by the client and passed back in at connection time via
// ConnLastTime) and the actual push data.
type payload struct {
	Time string          `json:"time"`
	Data json.RawMessage `json:"data"`
}

// A Conn is a connection receiving pushes from Parse.
type Conn struct {
	addr           string
	tlsConfig      *tls.Config
	dialer         *net.Dialer
	pingInterval   time.Duration
	installationID string
	applicationID  string
	lastTime       atomic.Value
	pushHandler    func([]byte)
	errHandler     func(error)
	retry          func(int) time.Duration
	closeOnce      sync.Once
	closeChan      chan chan struct{}

	// for testing purposes
	clock clock.Clock
	dialF func(*net.Dialer, string, string, *tls.Config) (net.Conn, error)
}

func (c *Conn) handleErr(err error) {
	if err != nil && c.errHandler != nil {
		c.errHandler(err)
	}
}

// dial returns a conn if possible. it will return nil if a conn has not been
// established yet and we're closing down.
func (c *Conn) dial() net.Conn {
	ir := map[string]string{
		"installation_id": c.installationID,
		"oauth_key":       c.applicationID,
		"v":               "e1.0.0",
	}
	if lastTime := c.LastTime(); lastTime != "" {
		ir["last"] = lastTime
	}
	irb, _ := json.Marshal(ir)
	irb = append(irb, '\r', '\n')

	attempt := 0
	for {
		conn, err := c.dialF(c.dialer, "tcp", c.addr, c.tlsConfig)

		// successfully dialed out. tell parse who we are and return the prepped
		// connection.
		if err == nil {
			_, err = conn.Write(irb)
			if err == nil {
				return conn
			}
		}

		// failed to dial, log and wait some time before we retry, give up only if
		// we are closing down.
		c.handleErr(err)
		select {
		case <-c.clock.After(c.retry(attempt)):
		case done := <-c.closeChan:
			close(done)
			return nil
		}
		attempt++
	}
}

func (c *Conn) do() {
	var lr lineReader
	var conn net.Conn
	pingTicker := c.clock.Ticker(c.pingInterval)
	defer pingTicker.Stop()

	handleErr := func(err error) {
		if err != nil {
			c.handleErr(err)
			conn.Close()
			close(lr.close)
			conn = nil
			<-lr.done
		}
	}

	for {
		// we may be connecting the first time around, or trying to reconnect if we
		// lost our connection.
		if conn == nil {
			conn = c.dial()
			if conn == nil {
				// this can happen if we close before we successfully dial out.
				return
			}
			lr.reader = conn
			lr.lines = make(chan []byte)
			lr.errch = make(chan error)
			lr.close = make(chan struct{})
			lr.done = make(chan struct{})
			go lr.read()
		}

		select {
		case push := <-lr.lines:
			var p payload
			if err := json.Unmarshal(push, &p); err != nil {
				handleErr(err)
				break
			}
			// Ignore empty "{}" keep-alive response from server.
			if len(p.Time) == 0 && len(p.Data) == 0 {
				break
			}
			if p.Time > c.LastTime() {
				c.lastTime.Store(p.Time)
			}
			c.pushHandler([]byte(p.Data))
		case err := <-lr.errch:
			handleErr(err)
		case <-pingTicker.C:
			_, err := conn.Write(pingMessage)
			handleErr(err)
		case done := <-c.closeChan:
			conn.Close()
			close(lr.close)
			<-lr.done
			close(done)
			return
		}
	}
}

// LastTime returns the timestamp of the last push we received, if available.
// This should be stored between the connection being closed and supplied using
// ConnLastTime when setting up a connection if available. The suggested order
// is to Close the connection and then retrive this value which you should then
// persist.
func (c *Conn) LastTime() string {
	v, _ := c.lastTime.Load().(string)
	return v
}

// Close closes the underlying connection and stops receiving pushes.
func (c *Conn) Close() error {
	c.closeOnce.Do(func() {
		done := make(chan struct{})
		c.closeChan <- done
		<-done
	})
	return nil
}

// ConnOption allows configuring various aspects of the Conn.
type ConnOption func(*Conn) error

// ConnAddr configures the parse push server address. If unspecified, it
// defaults to "push.parse.com:443".
func ConnAddr(addr string) ConnOption {
	return func(c *Conn) error {
		c.addr = addr
		return nil
	}
}

// ConnTLSConfig configures TLS options. Defaults to nil.
func ConnTLSConfig(config *tls.Config) ConnOption {
	return func(c *Conn) error {
		c.tlsConfig = config
		return nil
	}
}

// ConnDialer configures a Dialer. This allows for configuring timeouts etc.
// The default dialer has a 1 minute timeout.
func ConnDialer(dialer *net.Dialer) ConnOption {
	return func(c *Conn) error {
		c.dialer = dialer
		return nil
	}
}

// ConnPingInterval configures the interval at which we send a ping on the
// connection. Defaults to 10 minutes.
func ConnPingInterval(interval time.Duration) ConnOption {
	return func(c *Conn) error {
		c.pingInterval = interval
		return nil
	}
}

// ConnApplicationID configures the application ID. This is required for a
// connection.
func ConnApplicationID(id string) ConnOption {
	return func(c *Conn) error {
		c.applicationID = id
		return nil
	}
}

// ConnInstallationID configures the installation ID we're receiving pushes
// for. This is required for a connection.
func ConnInstallationID(id string) ConnOption {
	return func(c *Conn) error {
		c.installationID = id
		return nil
	}
}

// ConnLastTime configures the timestamp of the last push we received. Although
// this timestamp is currently an ISO8601 string, it should be treated as an
// opaque string by the client. This ensures that we don't miss any relevant
// pushes since the last disconnect.
func ConnLastTime(timestamp string) ConnOption {
	return func(c *Conn) error {
		c.lastTime.Store(timestamp)
		return nil
	}
}

// ConnPushHandler configures the function that will be invoked when a push
// arrives. The push handler is invoked synchronously so you should not perform
// long running operations in this callback.
func ConnPushHandler(handler func([]byte)) ConnOption {
	return func(c *Conn) error {
		c.pushHandler = handler
		return nil
	}
}

// ConnErrorHandler configures an optional error handler. In the face of
// errors, we keep retrying. Configuring an error handler lets you process the
// ignored errors.
func ConnErrorHandler(handler func(error)) ConnOption {
	return func(c *Conn) error {
		c.errHandler = handler
		return nil
	}
}

// ConnRetryStrategy configures an optional retry strategy. The configured
// function is given the attempt number and must return the delay duration
// before which a reconnect will be attempted.
//
// https://godoc.org/aqwari.net/retry provides an excellent set of strategies.
func ConnRetryStrategy(s func(nth int) time.Duration) ConnOption {
	return func(c *Conn) error {
		c.retry = s
		return nil
	}
}

// NewConn creates a new Conn with the given options. At minimum you need to
// provide ConnApplicationID, ConnInstallationID and ConnPushHandler.
func NewConn(options ...ConnOption) (*Conn, error) {
	c := Conn{
		clock:     clock.New(),
		dialF:     defaultDialF,
		closeChan: make(chan chan struct{}),
	}
	for _, o := range options {
		if err := o(&c); err != nil {
			return nil, err
		}
	}
	if c.addr == "" {
		c.addr = defaultAddr
	}
	if c.pingInterval.Nanoseconds() == 0 {
		c.pingInterval = defaultPingInterval
	}
	if c.applicationID == "" {
		return nil, errMissingApplicationID
	}
	if c.installationID == "" {
		return nil, errMissingInstallationID
	}
	if c.pushHandler == nil {
		return nil, errMissingPushHandler
	}
	if c.dialer == nil {
		c.dialer = &net.Dialer{
			Timeout: defaultDialerTimeout,
		}
	}
	if c.retry == nil {
		c.retry = defaultRetry
	}
	go c.do()
	return &c, nil
}

type lineReader struct {
	reader io.Reader
	lines  chan []byte
	errch  chan error
	close  chan struct{}
	done   chan struct{}
}

func (l lineReader) read() {
	defer close(l.done)
	scanner := bufio.NewScanner(l.reader)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		// scanner.Bytes are only valid until the next call to Scan, so we must
		// copy them out.
		line := scanner.Bytes()
		lineCopy := make([]byte, len(line))
		copy(lineCopy, line)

		// send our line, unless we're closed
		select {
		case l.lines <- lineCopy:
		case <-l.close:
			return
		}
	}

	// either a real error or send io.EOF
	err := scanner.Err()
	if err == nil {
		err = io.EOF
	}

	// send our error, unless we're closed
	select {
	case l.errch <- err:
	case <-l.close:
	}
}

func defaultRetry(int) time.Duration {
	return defaultRetryInterval
}

func defaultDialF(
	dialer *net.Dialer,
	network string,
	addr string,
	config *tls.Config,
) (c net.Conn, err error) {
	c, err = tls.DialWithDialer(dialer, network, addr, config)
	if err != nil {
		c = nil // remove type info
	}
	return c, err
}
