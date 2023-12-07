package streaming

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/ipc"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb/library/go/core/log"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/config"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/paging"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/rdbms"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/rdbms/clickhouse"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
	api_service "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

var _ api_service.Connector_ReadSplitsServer = (*streamMock)(nil)

type streamMock struct {
	mock.Mock
	api_service.Connector_ReadSplitsServer
	logger log.Logger
}

func (m *streamMock) Context() context.Context {
	args := m.Called()

	return args.Get(0).(context.Context)
}

func (m *streamMock) Send(response *api_service_protos.TReadSplitsResponse) error {
	args := m.Called(response)

	return args.Error(0)
}

func (m *streamMock) makeSendMatcher(
	t *testing.T,
	split *api_service_protos.TSplit,
	expectedColumnarBlock [][]any,
	expectedRowCount int,
) func(response *api_service_protos.TReadSplitsResponse) bool {
	return func(response *api_service_protos.TReadSplitsResponse) bool {
		// Check values in data blocks
		buf := bytes.NewBuffer(response.GetArrowIpcStreaming())

		reader, err := ipc.NewReader(buf)
		require.NoError(t, err)

		for reader.Next() {
			record := reader.Record()

			require.Equal(t, len(split.Select.What.Items), len(record.Columns()))

			if record.NumRows() != int64(expectedRowCount) {
				return false
			}

			col0 := record.Column(0).(*array.Int32)
			require.Equal(t, &arrow.Int32Type{}, col0.DataType())

			for i := 0; i < len(expectedColumnarBlock[0]); i++ {
				if expectedColumnarBlock[0][i] != col0.Value(i) {
					return false
				}
			}

			col1 := record.Column(1).(*array.Binary)
			require.Equal(t, &arrow.BinaryType{}, col1.DataType())

			for i := 0; i < len(expectedColumnarBlock[1]); i++ {
				if !bytes.Equal([]byte(expectedColumnarBlock[1][i].(string)), col1.Value(i)) {
					return false
				}
			}
		}

		reader.Release()

		// Check stats
		require.NotNil(t, response.Stats)
		require.Equal(t, uint64(len(expectedColumnarBlock[0])), response.Stats.Rows)

		// TODO: come up with more elegant way of expected data size computing
		var expectedBytes int

		expectedBytes += len(expectedColumnarBlock[0]) * 4 // int32 -> 4 bytes
		for _, val := range expectedColumnarBlock[1] {
			expectedBytes += len(val.(string))
		}

		require.Equal(t, uint64(expectedBytes), response.Stats.Bytes)

		return true
	}
}

type testCaseStreaming struct {
	src                 [][]any
	rowsPerPage         int
	bufferQueueCapacity int
	scanErr             error
	sendErr             error
}

func (tc testCaseStreaming) name() string {
	return fmt.Sprintf(
		"totalRows_%d_rowsPerBlock_%d_bufferQueueCapacity_%d_scanErr_%v_sendErr_%v",
		len(tc.src), tc.rowsPerPage, tc.bufferQueueCapacity, tc.scanErr != nil, tc.sendErr != nil)
}

func (tc testCaseStreaming) messageParams() (sentMessages, rowsInLastMessage int) {
	modulo := len(tc.src) % tc.rowsPerPage

	if modulo == 0 {
		sentMessages = len(tc.src) / tc.rowsPerPage
		rowsInLastMessage = tc.rowsPerPage
	} else {
		sentMessages = len(tc.src)/tc.rowsPerPage + 1
		rowsInLastMessage = modulo
	}

	if tc.scanErr != nil {
		sentMessages--
		rowsInLastMessage = tc.rowsPerPage
	}

	return
}

func (tc testCaseStreaming) execute(t *testing.T) {
	logger := utils.NewTestLogger(t)
	request := &api_service_protos.TReadSplitsRequest{}
	split := utils.MakeTestSplit()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream := &streamMock{logger: logger}

	stream.On("Context").Return(ctx)

	connection := &utils.ConnectionMock{}

	connectionManager := &utils.ConnectionManagerMock{}
	connectionManager.On("Make", split.Select.DataSourceInstance).Return(connection, nil).Once()
	connectionManager.On("Release", connection).Return().Once()

	rows := &utils.RowsMock{
		PredefinedData: tc.src,
	}
	connection.On("Query", `SELECT "col0", "col1" FROM "example_1"`).Return(rows, nil).Once()

	col0Acceptor := new(*int32)
	*col0Acceptor = new(int32)
	col1Acceptor := new(*string)
	*col1Acceptor = new(string)

	transformer := &utils.TransformerMock{
		Acceptors: []any{
			col0Acceptor,
			col1Acceptor,
		},
	}

	if tc.scanErr == nil {
		rows.On("MakeTransformer", []*Ydb.Type{utils.NewPrimitiveType(Ydb.Type_INT32), utils.NewPrimitiveType(Ydb.Type_STRING)}).Return(transformer, nil).Once()
		rows.On("Next").Return(true).Times(len(rows.PredefinedData))
		rows.On("Next").Return(false).Once()
		rows.On("Scan", transformer.GetAcceptors()...).Return(nil).Times(len(rows.PredefinedData))
		rows.On("Err").Return(nil).Once()
		rows.On("Close").Return(nil).Once()
	} else {
		rows.On("MakeTransformer", []*Ydb.Type{utils.NewPrimitiveType(Ydb.Type_INT32), utils.NewPrimitiveType(Ydb.Type_STRING)}).Return(transformer, nil).Once()
		rows.On("Next").Return(true).Times(len(rows.PredefinedData) + 1)
		rows.On("Scan", transformer.GetAcceptors()...).Return(nil).Times(len(rows.PredefinedData))
		// instead of the last message, an error occurs
		rows.On("Scan", transformer.GetAcceptors()...).Return(tc.scanErr).Once()
		rows.On("Err").Return(nil).Once()
		rows.On("Close").Return(nil).Once()
	}

	totalMessages, rowsInLastMessage := tc.messageParams()

	expectedColumnarBlocks := utils.DataConverter{}.RowsToColumnBlocks(rows.PredefinedData, tc.rowsPerPage)

	if tc.sendErr == nil {
		for sendCallID := 0; sendCallID < totalMessages; sendCallID++ {
			expectedColumnarBlock := expectedColumnarBlocks[sendCallID]

			rowsInMessage := tc.rowsPerPage
			if sendCallID == totalMessages-1 {
				rowsInMessage = rowsInLastMessage
			}

			matcher := stream.makeSendMatcher(t, split, expectedColumnarBlock, rowsInMessage)

			stream.On("Send", mock.MatchedBy(matcher)).Return(nil).Once()
		}
	} else {
		// the first attempt to send response is failed
		stream.On("Send", mock.MatchedBy(func(response *api_service_protos.TReadSplitsResponse) bool {
			cancel() // emulate real behavior of GRPC

			return true
		})).Return(tc.sendErr).Once()
	}

	typeMapper := clickhouse.NewTypeMapper()

	dataSourcePreset := &rdbms.Preset{
		SQLFormatter:      clickhouse.NewSQLFormatter(),
		ConnectionManager: connectionManager,
		TypeMapper:        typeMapper,
	}

	dataSource := rdbms.NewDataSource(logger, dataSourcePreset)

	columnarBufferFactory, err := paging.NewColumnarBufferFactory(
		logger,
		memory.NewGoAllocator(),
		paging.NewReadLimiterFactory(nil),
		api_service_protos.TReadSplitsRequest_ARROW_IPC_STREAMING,
		split.Select.What)
	require.NoError(t, err)

	pagingCfg := &config.TPagingConfig{RowsPerPage: uint64(tc.rowsPerPage)}
	trafficTracker := paging.NewTrafficTracker(pagingCfg)
	readLimiterFactory := paging.NewReadLimiterFactory(nil)
	sink, err := paging.NewSink(
		ctx,
		logger,
		trafficTracker,
		columnarBufferFactory,
		readLimiterFactory.MakeReadLimiter(logger),
		tc.bufferQueueCapacity,
	)
	require.NoError(t, err)

	streamer := NewStreamer(logger, stream, request, split, sink, dataSource)

	err = streamer.Run()

	switch {
	case tc.scanErr != nil:
		require.True(t, errors.Is(err, tc.scanErr))
	case tc.sendErr != nil:
		require.True(t, errors.Is(err, tc.sendErr))
	default:
		require.NoError(t, err)
	}

	mocks := []interface{}{stream, connectionManager, connection, transformer}

	mock.AssertExpectationsForObjects(t, mocks...)
}

func TestStreaming(t *testing.T) {
	srcValues := [][][]any{
		{
			{int32(1), "a"},
			{int32(2), "b"},
			{int32(3), "c"},
			{int32(4), "d"},
		},
		{
			{int32(1), "a"},
			{int32(2), "b"},
			{int32(3), "c"},
			{int32(4), "d"},
			{int32(5), "e"},
		},
	}
	rowsPerBlockValues := []int{2}
	bufferQueueCapacityValues := []int{
		0,
		1,
		10,
	}

	var testCases []testCaseStreaming

	for _, src := range srcValues {
		for _, rowsPerBlock := range rowsPerBlockValues {
			for _, bufferQueueCapacity := range bufferQueueCapacityValues {
				tc := testCaseStreaming{
					src:                 src,
					rowsPerPage:         rowsPerBlock,
					bufferQueueCapacity: bufferQueueCapacity,
				}

				testCases = append(testCases, tc)
			}
		}
	}

	t.Run("positive", func(t *testing.T) {
		for _, tc := range testCases {
			tc := tc
			t.Run(tc.name(), func(t *testing.T) {
				tc.execute(t)
			})
		}
	})

	t.Run("scan error", func(t *testing.T) {
		scanErr := fmt.Errorf("scan error")

		for _, tc := range testCases {
			tc := tc
			tc.scanErr = scanErr
			t.Run(tc.name(), func(t *testing.T) {
				tc.execute(t)
			})
		}
	})

	t.Run("send error", func(t *testing.T) {
		sendErr := fmt.Errorf("stream send error")

		for _, tc := range testCases {
			tc := tc
			tc.sendErr = sendErr
			t.Run(tc.name(), func(t *testing.T) {
				tc.execute(t)
			})
		}
	})
}
