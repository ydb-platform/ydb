//go:build darwin || freebsd || linux
// +build darwin freebsd linux

package logrotate

import (
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"sync/atomic"
	"unsafe"

	"github.com/ydb-platform/ydb/library/go/core/xerrors"
	"go.uber.org/zap"
)

const defaultSchemeName = "logrotate"

// Register logrotate sink in zap sink registry.
// This sink internally is like file sink, but listens to provided logrotate signal
// and reopens file when that signal is delivered
// This can be called only once. Any future calls will result in an error
func RegisterLogrotateSink(sig ...os.Signal) error {
	return RegisterNamedLogrotateSink(defaultSchemeName, sig...)
}

// Same as RegisterLogrotateSink, but use provided schemeName instead of default `logrotate`
// Can be useful in special cases for registering different types of sinks for different signal
func RegisterNamedLogrotateSink(schemeName string, sig ...os.Signal) error {
	factory := func(url *url.URL) (sink zap.Sink, e error) {
		return NewLogrotateSink(url, sig...)
	}
	return zap.RegisterSink(schemeName, factory)
}

// sink itself, use RegisterLogrotateSink to register it in zap machinery
type sink struct {
	path     string
	notifier chan os.Signal
	file     unsafe.Pointer
}

// Factory for logrotate sink, which accepts os.Signals to listen to for reloading
// Generally if you don't build your own core it is used by zap machinery.
// See RegisterLogrotateSink.
func NewLogrotateSink(u *url.URL, sig ...os.Signal) (zap.Sink, error) {
	notifier := make(chan os.Signal, 1)
	signal.Notify(notifier, sig...)

	if u.User != nil {
		return nil, fmt.Errorf("user and password not allowed with logrotate file URLs: got %v", u)
	}
	if u.Fragment != "" {
		return nil, fmt.Errorf("fragments not allowed with logrotate file URLs: got %v", u)
	}
	// Error messages are better if we check hostname and port separately.
	if u.Port() != "" {
		return nil, fmt.Errorf("ports not allowed with logrotate file URLs: got %v", u)
	}
	if hn := u.Hostname(); hn != "" && hn != "localhost" {
		return nil, fmt.Errorf("logrotate file URLs must leave host empty or use localhost: got %v", u)
	}

	sink := &sink{
		path:     u.Path,
		notifier: notifier,
	}
	if err := sink.reopen(); err != nil {
		return nil, err
	}
	go sink.listenToSignal()
	return sink, nil
}

// wait for signal delivery or chanel close
func (m *sink) listenToSignal() {
	for {
		_, ok := <-m.notifier
		if !ok {
			return
		}
		if err := m.reopen(); err != nil {
			// Last chance to signalize about an error
			_, _ = fmt.Fprintf(os.Stderr, "%s", err)
		}
	}
}

func (m *sink) reopen() error {
	file, err := os.OpenFile(m.path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return xerrors.Errorf("failed to open log file on %s: %w", m.path, err)
	}
	old := (*os.File)(m.file)
	atomic.StorePointer(&m.file, unsafe.Pointer(file))
	if old != nil {
		if err := old.Close(); err != nil {
			return xerrors.Errorf("failed to close old file: %w", err)
		}
	}
	return nil
}

func (m *sink) getFile() *os.File {
	return (*os.File)(atomic.LoadPointer(&m.file))
}

func (m *sink) Close() error {
	signal.Stop(m.notifier)
	close(m.notifier)
	return m.getFile().Close()
}

func (m *sink) Write(p []byte) (n int, err error) {
	return m.getFile().Write(p)
}

func (m *sink) Sync() error {
	return m.getFile().Sync()
}
