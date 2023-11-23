package paging

import (
	"fmt"

	"github.com/ydb-platform/ydb/library/go/core/log"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/config"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
)

// ReadLimiter helps to limitate amount of data returned by Connector server in every read request.
// This is generally should be avoided after https://st.yandex-team.ru/YQ-2057
type ReadLimiter interface {
	addRow() error
}

type readLimiterNoop struct {
}

func (rl readLimiterNoop) addRow() error { return nil }

type readLimiterRows struct {
	rowsRead  uint64
	rowsLimit uint64
}

func (rl *readLimiterRows) addRow() error {
	if rl.rowsRead >= rl.rowsLimit {
		return fmt.Errorf("can read only %d line(s) from data source per request: %w",
			rl.rowsLimit,
			utils.ErrReadLimitExceeded)
	}

	rl.rowsRead++

	return nil
}

type ReadLimiterFactory struct {
	cfg *config.TServerReadLimit
}

func (rlf *ReadLimiterFactory) MakeReadLimiter(logger log.Logger) ReadLimiter {
	if rlf.cfg == nil {
		return readLimiterNoop{}
	}

	logger.Warn(fmt.Sprintf("Server will return only first %d lines from the data source", rlf.cfg.GetRows()))

	return &readLimiterRows{rowsRead: 0, rowsLimit: rlf.cfg.GetRows()}
}

func NewReadLimiterFactory(cfg *config.TServerReadLimit) *ReadLimiterFactory {
	return &ReadLimiterFactory{cfg: cfg}
}
