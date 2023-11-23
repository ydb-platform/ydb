// +build linux darwin dragonfly freebsd netbsd openbsd solaris illumos

package clickhouse

import (
	"errors"
	"fmt"
	"io"
	"syscall"
	"time"
)

var errUnexpectedRead = errors.New("unexpected read from socket")

func (conn *connect) connCheck() error {
	var sysErr error

	sysConn, ok := conn.Conn.(syscall.Conn)
	if !ok {
		return nil
	}
	rawConn, err := sysConn.SyscallConn()
	if err != nil {
		return err
	}
	// If this connection has a ReadTimeout which we've been setting on
	// reads, reset it to zero value before we attempt a non-blocking
	// read, otherwise we may get os.ErrDeadlineExceeded for the cached
	// connection from the pool with an expired timeout.
	if conn.readTimeout != 0 {
		err = conn.SetReadDeadline(time.Time{})
		if err != nil {
			return fmt.Errorf("set read deadline: %w", err)
		}
		conn.lastReadDeadlineTime = time.Time{}
	}
	err = rawConn.Read(func(fd uintptr) bool {
		var buf [1]byte
		n, err := syscall.Read(int(fd), buf[:])
		switch {
		case n == 0 && err == nil:
			sysErr = io.EOF
		case n > 0:
			sysErr = errUnexpectedRead
		case err == syscall.EAGAIN || err == syscall.EWOULDBLOCK:
			sysErr = nil
		default:
			sysErr = err
		}
		return true
	})
	if err != nil {
		return err
	}

	return sysErr
}
