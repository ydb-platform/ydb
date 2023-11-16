// +build !linux,!darwin,!dragonfly,!freebsd,!netbsd,!openbsd,!solaris,!illumos

package clickhouse

func (conn *connect) connCheck() error {
	return nil
}
