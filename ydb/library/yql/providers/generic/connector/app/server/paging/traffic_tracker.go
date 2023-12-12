package paging

import (
	"fmt"

	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/config"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

type TrafficTracker[T utils.Acceptor] struct {
	pagination  *config.TPagingConfig
	sizePattern *sizePattern[T]

	// cumulative sums of bytes passed and rows handled since the start of the request
	bytesTotal *utils.Counter[uint64]
	rowsTotal  *utils.Counter[uint64]

	// sums of bytes and rows accumulated since last flush
	bytesCurr *utils.Counter[uint64]
	rowsCurr  *utils.Counter[uint64]
}

// tryAddRow checks if the addition of the next row
// would exceed the limits on the page size.
// If there's enough space in buffer, it returns true and increases the internal counters.
// Otherwise it return false, but doesn't change internal state.
func (tt *TrafficTracker[T]) tryAddRow(acceptors []T) (bool, error) {
	if err := tt.maybeInit(acceptors); err != nil {
		return false, fmt.Errorf("maybe init: %w", err)
	}

	totalBytes, err := tt.sizePattern.estimate(acceptors)
	if err != nil {
		return false, fmt.Errorf("size pattern estimate: %w", err)
	}

	wouldBeEnough, err := tt.checkPageSizeLimit(totalBytes, 1)
	if err != nil {
		return false, fmt.Errorf("check page size limit: %w", err)
	}

	if wouldBeEnough {
		return false, nil
	}

	tt.bytesCurr.Add(totalBytes)
	tt.rowsCurr.Add(1)

	return true, nil
}

func (tt *TrafficTracker[T]) maybeInit(acceptors []T) error {
	if tt.sizePattern == nil {
		// lazy initialization when the first row is ready
		var err error
		tt.sizePattern, err = newSizePattern(acceptors)

		if err != nil {
			return fmt.Errorf("new size pattern: %w", err)
		}
	}

	return nil
}

func (tt *TrafficTracker[T]) checkPageSizeLimit(bytesDelta, rowsDelta uint64) (bool, error) {
	if tt.pagination.BytesPerPage != 0 {
		// almost impossible case, but have to check
		if bytesDelta > tt.pagination.BytesPerPage {
			err := fmt.Errorf(
				"single row size exceeds page size limit (%d > %d bytes): %w",
				bytesDelta,
				tt.pagination.BytesPerPage,
				utils.ErrPageSizeExceeded)

			return true, err
		}

		if tt.bytesCurr.Value()+bytesDelta > tt.pagination.BytesPerPage {
			return true, nil
		}
	}

	if tt.pagination.RowsPerPage != 0 {
		if tt.rowsCurr.Value()+rowsDelta > tt.pagination.RowsPerPage {
			return true, nil
		}
	}

	return false, nil
}

func (tt *TrafficTracker[T]) refreshCounters() {
	tt.bytesCurr = tt.bytesTotal.MakeChild()
	tt.rowsCurr = tt.rowsTotal.MakeChild()
}

func (tt *TrafficTracker[T]) DumpStats(total bool) *api_service_protos.TReadSplitsResponse_TStats {
	rowsCounter := tt.rowsCurr
	bytesCounter := tt.bytesCurr

	if total {
		rowsCounter = tt.rowsTotal
		bytesCounter = tt.bytesTotal
	}

	result := &api_service_protos.TReadSplitsResponse_TStats{
		Rows:  rowsCounter.Value(),
		Bytes: bytesCounter.Value(),
	}

	return result
}

func NewTrafficTracker[T utils.Acceptor](pagination *config.TPagingConfig) *TrafficTracker[T] {
	tt := &TrafficTracker[T]{
		pagination: pagination,
		bytesTotal: utils.NewCounter[uint64](),
		rowsTotal:  utils.NewCounter[uint64](),
	}

	tt.refreshCounters()

	return tt
}
