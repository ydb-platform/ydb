//go:build !darwin && !freebsd && !linux
// +build !darwin,!freebsd,!linux

package logrotate

import (
	"net/url"
	"os"

	"go.uber.org/zap"
)

func RegisterLogrotateSink(sig ...os.Signal) error {
	return ErrNotSupported
}

func RegisterNamedLogrotateSink(schemeName string, sig ...os.Signal) error {
	return ErrNotSupported
}

func NewLogrotateSink(u *url.URL, sig ...os.Signal) (zap.Sink, error) {
	return nil, ErrNotSupported
}
