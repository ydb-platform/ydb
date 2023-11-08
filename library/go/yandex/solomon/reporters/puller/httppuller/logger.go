package httppuller

import "github.com/ydb-platform/ydb/library/go/core/log"

type loggerOption struct {
	logger log.Logger
}

func (*loggerOption) isOption() {}

func WithLogger(logger log.Logger) Option {
	return &loggerOption{
		logger: logger,
	}
}
