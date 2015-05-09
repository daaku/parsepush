package parsepush

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net"
	"regexp"
	"sync/atomic"
	"testing"
	"time"

	"github.com/facebookgo/clock"
	"github.com/facebookgo/ensure"
)

func TestHandleError(t *testing.T) {
	var called bool
	givenErr := errors.New("")
	c := Conn{
		errHandler: func(err error) {
			ensure.DeepEqual(t, err, givenErr)
			called = true
		},
	}
	c.handleErr(givenErr)
	ensure.True(t, called)
}

func TestHandleErrorOptional(t *testing.T) {
	var c Conn
	c.handleErr(errors.New(""))
}

func TestHandleErrorNilIgnored(t *testing.T) {
	var called bool
	c := Conn{
		errHandler: func(err error) {
			called = true
		},
	}
	c.handleErr(nil)
	ensure.False(t, called)
}

func TestNewConnMissingApplicationID(t *testing.T) {
	_, err := NewConn()
	ensure.DeepEqual(t, err, errMissingApplicationID)
}

func TestNewConnMissingInstallationID(t *testing.T) {
	_, err := NewConn(
		ConnApplicationID("x"),
	)
	ensure.DeepEqual(t, err, errMissingInstallationID)
}

func TestNewConnDefaults(t *testing.T) {
	c, err := NewConn(
		ConnApplicationID("x"),
		ConnInstallationID("x"),
		ConnPushHandler(func([]byte) {}),
	)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, c.addr, defaultAddr)
	ensure.DeepEqual(t, c.pingInterval, defaultPingInterval)
	ensure.DeepEqual(t, c.dialer, &net.Dialer{
		Timeout: defaultDialerTimeout,
	})
	go c.Close() // dont slow down the tests since this test hits the prod addr
}

func TestNewConnError(t *testing.T) {
	givenErr := errors.New("")
	_, err := NewConn(
		func(*Conn) error { return givenErr },
	)
	ensure.DeepEqual(t, err, givenErr)
}

const fakeAddr = "32b6ed67-a07b-4f0c-a6c6-fdcbb81c94fa"

func TestConnRetryStrategy(t *testing.T) {
	const magic = 42 * time.Second
	c, err := NewConn(
		ConnAddr(fakeAddr),
		ConnApplicationID("x"),
		ConnInstallationID("x"),
		ConnPushHandler(func([]byte) {}),
		ConnRetryStrategy(func(int) time.Duration { return magic }),
	)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, c.retry(1), magic)
	ensure.Nil(t, c.Close())
}

func TestConnPushHandler(t *testing.T) {
	var called bool
	c, err := NewConn(
		ConnAddr(fakeAddr),
		ConnApplicationID("x"),
		ConnInstallationID("x"),
		ConnPushHandler(func([]byte) { called = true }),
	)
	ensure.Nil(t, err)
	c.pushHandler(nil)
	ensure.True(t, called)
	ensure.Nil(t, c.Close())
}

func TestConnPushHandlerMissing(t *testing.T) {
	_, err := NewConn(
		ConnAddr(fakeAddr),
		ConnApplicationID("x"),
		ConnInstallationID("x"),
	)
	ensure.DeepEqual(t, err, errMissingPushHandler)
}

func TestConnErrorHandler(t *testing.T) {
	var called int32
	c, err := NewConn(
		ConnAddr(fakeAddr),
		ConnApplicationID("x"),
		ConnInstallationID("x"),
		ConnPushHandler(func([]byte) {}),
		ConnErrorHandler(func(error) {
			atomic.AddInt32(&called, 1)
		}),
	)
	ensure.Nil(t, err)
	c.handleErr(errors.New(""))
	ensure.DeepEqual(t, atomic.LoadInt32(&called), int32(1))
	ensure.Nil(t, c.Close())
}

func TestConnDefaultLastHash(t *testing.T) {
	c, err := NewConn(
		ConnAddr(fakeAddr),
		ConnApplicationID("x"),
		ConnInstallationID("x"),
		ConnPushHandler(func([]byte) {}),
	)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, c.LastTime(), "")
	ensure.Nil(t, c.Close())
}

func TestConnConfiguredLastTime(t *testing.T) {
	const givenTime = "2014-10-16T10:16:35.392Z"
	c, err := NewConn(
		ConnAddr(fakeAddr),
		ConnApplicationID("x"),
		ConnInstallationID("x"),
		ConnPushHandler(func([]byte) {}),
		ConnLastTime(givenTime),
	)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, c.LastTime(), givenTime)
	ensure.Nil(t, c.Close())
}

func TestConnPingInterval(t *testing.T) {
	const pingInterval = 42 * time.Second
	c, err := NewConn(
		ConnAddr(fakeAddr),
		ConnApplicationID("x"),
		ConnInstallationID("x"),
		ConnPushHandler(func([]byte) {}),
		ConnPingInterval(pingInterval),
	)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, c.pingInterval, pingInterval)
	ensure.Nil(t, c.Close())
}

func TestConnDialer(t *testing.T) {
	d := &net.Dialer{}
	c, err := NewConn(
		ConnAddr(fakeAddr),
		ConnApplicationID("x"),
		ConnInstallationID("x"),
		ConnPushHandler(func([]byte) {}),
		ConnDialer(d),
	)
	ensure.Nil(t, err)
	ensure.True(t, c.dialer == d)
	ensure.Nil(t, c.Close())
}

func TestConnTLSConfig(t *testing.T) {
	tc := &tls.Config{}
	c, err := NewConn(
		ConnAddr(fakeAddr),
		ConnApplicationID("x"),
		ConnInstallationID("x"),
		ConnPushHandler(func([]byte) {}),
		ConnTLSConfig(tc),
	)
	ensure.Nil(t, err)
	ensure.True(t, c.tlsConfig == tc)
	ensure.Nil(t, c.Close())
}

func TestConnAddr(t *testing.T) {
	const addr = "addr"
	c, err := NewConn(
		ConnAddr(fakeAddr),
		ConnApplicationID("x"),
		ConnInstallationID("x"),
		ConnPushHandler(func([]byte) {}),
		ConnAddr(addr),
	)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, c.addr, addr)
	ensure.Nil(t, c.Close())
}

func TestDialSuccess(t *testing.T) {
	givenDialer := &net.Dialer{}
	givenTLSConfig := &tls.Config{}
	givenAddr := "42"
	in, out := net.Pipe()
	c := &Conn{
		dialer:    givenDialer,
		tlsConfig: givenTLSConfig,
		addr:      givenAddr,
		dialF: func(
			dialer *net.Dialer,
			network, addr string,
			config *tls.Config,
		) (net.Conn, error) {
			ensure.True(t, dialer == givenDialer)
			ensure.True(t, network == "tcp")
			ensure.True(t, addr == givenAddr)
			ensure.True(t, config == givenTLSConfig)
			return in, nil
		},
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		bd, err := ioutil.ReadAll(out)
		ensure.Nil(t, err)
		ensure.True(t, bytes.HasSuffix(bd, []byte("\r\n")))
		ir := make(map[string]string)
		ensure.Nil(t, json.Unmarshal(bd, &ir))
		ensure.DeepEqual(t, ir, map[string]string{
			"installation_id": "",
			"oauth_key":       "",
			"v":               "e1.0.0",
		})
	}()
	ensure.DeepEqual(t, c.dial(), in)
	in.Close()
	<-done
}

func TestDialSuccessWithLastHash(t *testing.T) {
	in, out := net.Pipe()
	c := &Conn{
		dialF: func(*net.Dialer, string, string, *tls.Config) (net.Conn, error) {
			return in, nil
		},
	}
	const givenTime = "2014-10-16T10:16:35.392Z"
	c.lastTime.Store(givenTime)
	done := make(chan struct{})
	go func() {
		defer close(done)
		bd, err := ioutil.ReadAll(out)
		ensure.Nil(t, err)
		ensure.True(t, bytes.HasSuffix(bd, []byte("\r\n")))
		ir := make(map[string]string)
		ensure.Nil(t, json.Unmarshal(bd, &ir))
		ensure.DeepEqual(t, ir, map[string]string{
			"installation_id": "",
			"oauth_key":       "",
			"v":               "e1.0.0",
			"last":            givenTime,
		})
	}()
	ensure.DeepEqual(t, c.dial(), in)
	in.Close()
	<-done
}

func TestDialErrorAndRetryAndClose(t *testing.T) {
	const givenSleep = 42 * time.Second
	givenErr := errors.New("")
	var retryCalled, errorHandlerCalled bool
	var c *Conn
	c = &Conn{
		clock:     clock.NewMock(),
		closeChan: make(chan chan struct{}),
		dialF: func(*net.Dialer, string, string, *tls.Config) (net.Conn, error) {
			return nil, givenErr
		},
		retry: func(nth int) time.Duration {
			retryCalled = true
			ensure.DeepEqual(t, nth, 0)
			go c.Close()
			return givenSleep
		},
		errHandler: func(err error) {
			errorHandlerCalled = true
			ensure.True(t, err == givenErr)
		},
	}
	ensure.True(t, c.dial() == nil)
	ensure.True(t, retryCalled)
	ensure.True(t, errorHandlerCalled)
}

func TestDialErrorAndRetryRetryAndClose(t *testing.T) {
	const givenSleep = 42 * time.Second
	var retryCalled int32
	var c *Conn
	mockClock := clock.NewMock()
	c = &Conn{
		clock:     mockClock,
		closeChan: make(chan chan struct{}),
		dialF: func(*net.Dialer, string, string, *tls.Config) (net.Conn, error) {
			return nil, errors.New("")
		},
		retry: func(nth int) time.Duration {
			switch atomic.AddInt32(&retryCalled, 1) {
			case 1:
				go mockClock.Add(givenSleep)
				return givenSleep
			case 2:
				go c.Close()
				return givenSleep
			}
			panic("not reached")
		},
	}
	ensure.True(t, c.dial() == nil)
	ensure.DeepEqual(t, atomic.LoadInt32(&retryCalled), int32(2))
}

func TestReadPushes(t *testing.T) {
	in, out := net.Pipe()
	pushes := make(chan []byte, 2)

	givenPush1 := []byte(`{"time":"2014-10-16T10:16:35.392Z","data":{"foo":"bar"}}`)
	expectedPayload1 := []byte(`{"foo":"bar"}`)

	givenPush2 := []byte(`{"time":"2014-10-16T10:16:36.402Z","data":{"bar":"baz"}}`)
	expectedPayload2 := []byte(`{"bar":"baz"}`)

	c := &Conn{
		clock:        clock.New(),
		pingInterval: time.Minute,
		closeChan:    make(chan chan struct{}),
		dialF: func(*net.Dialer, string, string, *tls.Config) (net.Conn, error) {
			return in, nil
		},
		pushHandler: func(p []byte) { pushes <- p },
	}
	go io.Copy(ioutil.Discard, out)
	go c.do()
	go func() {
		out.Write(givenPush1)
		out.Write([]byte("\r\n"))
		out.Write(givenPush2)
		out.Write([]byte("\r\n"))
	}()
	ensure.DeepEqual(t, <-pushes, expectedPayload1)
	ensure.DeepEqual(t, <-pushes, expectedPayload2)
	ensure.Nil(t, c.Close())
}

func TestReadError(t *testing.T) {
	errs := make(chan error, 2)
	givenDialErr := errors.New("")
	var dialCount int32
	in, out := net.Pipe()
	c := &Conn{
		clock:        clock.New(),
		pingInterval: time.Minute,
		closeChan:    make(chan chan struct{}),
		dialF: func(*net.Dialer, string, string, *tls.Config) (net.Conn, error) {
			switch atomic.AddInt32(&dialCount, 1) {
			case 1:
				return in, nil
			default:
				return nil, givenDialErr
			}
		},
		errHandler: func(err error) {
			errs <- err
		},
		retry: func(int) time.Duration {
			return time.Microsecond
		},
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		bytes, err := bufio.NewReader(out).ReadBytes('\n')
		ensure.Nil(t, err)
		ensure.DeepEqual(t, len(bytes), 52)
	}()
	go c.do()
	<-done
	go out.Close()
	ensure.DeepEqual(t, <-errs, io.EOF)
	ensure.DeepEqual(t, <-errs, givenDialErr)
	ensure.Nil(t, c.Close())
	ensure.True(t, atomic.LoadInt32(&dialCount) > 1)
}

func TestPingMessage(t *testing.T) {
	errs := make(chan error, 2)
	in, out := net.Pipe()
	mockClock := clock.NewMock()
	c := &Conn{
		pingInterval: time.Minute,
		clock:        mockClock,
		closeChan:    make(chan chan struct{}),
		dialF: func(*net.Dialer, string, string, *tls.Config) (net.Conn, error) {
			return in, nil
		},
		errHandler: func(err error) {
			errs <- err
		},
	}
	initDone := make(chan struct{})
	pingDone1 := make(chan struct{})
	pingDone2 := make(chan struct{})
	go func() {
		r := bufio.NewReader(out)

		bytes, err := r.ReadBytes('\n')
		ensure.Nil(t, err)
		ensure.DeepEqual(t, len(bytes), 52)
		close(initDone)

		bytes, err = r.ReadBytes('\n')
		ensure.Nil(t, err)
		ensure.DeepEqual(t, bytes, pingMessage)
		close(pingDone1)

		bytes, err = r.ReadBytes('\n')
		ensure.Nil(t, err)
		ensure.DeepEqual(t, bytes, pingMessage)
		close(pingDone2)

		io.Copy(ioutil.Discard, r)
	}()
	go c.do()
	<-initDone
	mockClock.Add(c.pingInterval)
	<-pingDone1
	mockClock.Add(c.pingInterval)
	<-pingDone2
	ensure.Nil(t, c.Close())
	close(errs)
	ensure.Nil(t, <-errs)
}

type fReader func(p []byte) (n int, err error)

func (f fReader) Read(p []byte) (n int, err error) { return f(p) }

func TestLineReader(t *testing.T) {
	const givenStr1 = "abc"
	const givenStr2 = "def"
	givenErr := errors.New("")
	count := 0
	l := lineReader{
		lines: make(chan []byte),
		errch: make(chan error),
		close: make(chan struct{}),
		done:  make(chan struct{}),
		reader: fReader(func(p []byte) (int, error) {
			count++
			switch count {
			case 1:
				return copy(p, givenStr1+"\n"), nil
			case 2:
				return copy(p, givenStr2+"\n"), nil
			case 3:
				return 0, givenErr
			}
			panic("not reached")
		}),
	}
	go l.read()
	ensure.DeepEqual(t, string(<-l.lines), givenStr1)
	ensure.DeepEqual(t, string(<-l.lines), givenStr2)
	ensure.DeepEqual(t, <-l.errch, givenErr)
}

func TestLineReaderClose(t *testing.T) {
	l := lineReader{
		lines: make(chan []byte),
		errch: make(chan error),
		close: make(chan struct{}),
		done:  make(chan struct{}),
		reader: fReader(func(p []byte) (int, error) {
			return copy(p, "foo\n"), nil
		}),
	}
	go l.read()
	close(l.close)
	<-l.done
}

func TestLineReaderErrorClose(t *testing.T) {
	l := lineReader{
		lines: make(chan []byte),
		errch: make(chan error),
		close: make(chan struct{}),
		done:  make(chan struct{}),
		reader: fReader(func(p []byte) (int, error) {
			return 0, io.EOF
		}),
	}
	go l.read()
	close(l.close)
	<-l.done
}

func TestDefautRetryInterval(t *testing.T) {
	ensure.DeepEqual(t, defaultRetry(0), defaultRetryInterval)
	ensure.DeepEqual(t, defaultRetry(1), defaultRetryInterval)
}

func TestDefaultDialF(t *testing.T) {
	c, err := defaultDialF(&net.Dialer{}, "foo", "bar", nil)
	ensure.Nil(t, c)
	ensure.Err(t, err, regexp.MustCompile("unknown network foo"))
}
