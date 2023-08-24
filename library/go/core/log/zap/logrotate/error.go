package logrotate

import "errors"

var ErrNotSupported = errors.New("logrotate sink is not supported on your platform")
