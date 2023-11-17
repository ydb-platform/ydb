package paging

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/config"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

func TestTrafficTracker(t *testing.T) {
	t.Run("separate pages by rows", func(t *testing.T) {
		cfg := &config.TPagingConfig{
			RowsPerPage: 2,
		}

		tt := NewTrafficTracker(cfg)

		col1Acceptor := new(int32)
		col2Acceptor := new(string)
		col3Acceptor := new(uint64)

		acceptors := []any{col1Acceptor, col2Acceptor, col3Acceptor}

		*col1Acceptor = 1       // 4 bytes
		*col2Acceptor = "abcde" // 5 bytes
		*col3Acceptor = 1       // 8 bytes

		ok, err := tt.tryAddRow(acceptors)
		require.NoError(t, err)
		require.True(t, ok)

		*col1Acceptor = 1       // 4 bytes
		*col2Acceptor = "абвгд" // 10 bytes
		*col3Acceptor = 1       // 8 bytes

		ok, err = tt.tryAddRow(acceptors)
		require.NoError(t, err)
		require.True(t, ok)

		actualStats := tt.DumpStats(false)
		expectedStats := &api_service_protos.TReadSplitsResponse_TStats{
			Rows:  2,
			Bytes: (4 + 5 + 8) + (4 + 10 + 8),
		}
		require.Equal(t, expectedStats, actualStats)

		// emulate buffer flushing

		tt.refreshCounters()
		require.Zero(t, tt.bytesCurr.Value())
		require.Zero(t, tt.rowsCurr.Value())
		require.Equal(t, expectedStats.Bytes, tt.bytesTotal.Value())
		require.Equal(t, expectedStats.Rows, tt.rowsTotal.Value())

		// add some more rows after flushing

		*col1Acceptor = 1       // 4 bytes
		*col2Acceptor = "abcde" // 5 bytes
		*col3Acceptor = 1       // 8 bytes

		ok, err = tt.tryAddRow(acceptors)
		require.NoError(t, err)
		require.True(t, ok)

		actualStats = tt.DumpStats(false)
		expectedStats = &api_service_protos.TReadSplitsResponse_TStats{
			Rows:  1,
			Bytes: (4 + 5 + 8),
		}
		require.Equal(t, expectedStats, actualStats)

		// global stats reflects previous events
		actualStats = tt.DumpStats(true)
		expectedStats = &api_service_protos.TReadSplitsResponse_TStats{
			Rows:  3,
			Bytes: (4 + 5 + 8) + (4 + 10 + 8) + (4 + 5 + 8),
		}
		require.Equal(t, expectedStats, actualStats)
	})

	t.Run("separate pages by bytes", func(t *testing.T) {
		cfg := &config.TPagingConfig{
			BytesPerPage: 40,
		}

		tt := NewTrafficTracker(cfg)

		col1Acceptor := new(uint64)
		col2Acceptor := new([]byte)
		col3Acceptor := new(time.Time)

		acceptors := []any{col1Acceptor, col2Acceptor, col3Acceptor}

		*col1Acceptor = 1                     // 8 bytes
		*col2Acceptor = []byte{1, 2, 3, 4, 5} // 5 bytes
		*col3Acceptor = time.Now().UTC()      // approximately 16 bytes

		ok, err := tt.tryAddRow(acceptors)
		require.NoError(t, err)
		require.True(t, ok) // (8 + 5 + 16) < 40

		ok, err = tt.tryAddRow(acceptors)
		require.NoError(t, err)
		require.False(t, ok) // 2 * (8 + 5 + 16) > 40

		// only first addition resides in stats
		actualStats := tt.DumpStats(false)
		expectedStats := &api_service_protos.TReadSplitsResponse_TStats{
			Rows:  1,
			Bytes: (8 + 5 + 16),
		}
		require.Equal(t, expectedStats, actualStats)

		tt.refreshCounters()
		require.Zero(t, tt.bytesCurr.Value())
		require.Zero(t, tt.rowsCurr.Value())
		require.Equal(t, expectedStats.Bytes, tt.bytesTotal.Value())
		require.Equal(t, expectedStats.Rows, tt.rowsTotal.Value())
	})

	t.Run("too small page", func(t *testing.T) {
		cfg := &config.TPagingConfig{
			BytesPerPage: 1,
		}

		tt := NewTrafficTracker(cfg)
		col1Acceptor := new(int32)
		acceptors := []any{col1Acceptor}

		*col1Acceptor = 1 // 4 bytes > 1 byte

		ok, err := tt.tryAddRow(acceptors)
		require.True(t, errors.Is(err, utils.ErrPageSizeExceeded))
		require.False(t, ok)
	})
}
