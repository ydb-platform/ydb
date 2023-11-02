package streaming

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/paging"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/rdbms"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
	api_service "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

var _ api_service.Connector_ReadSplitsServer = (*streamMock)(nil)

type streamMock struct {
	mock.Mock
	api_service.Connector_ReadSplitsServer
	sendTriggeredChan chan struct{}
}

func (m *streamMock) Context() context.Context {
	args := m.Called()

	return args.Get(0).(context.Context)
}

func (m *streamMock) Send(response *api_service_protos.TReadSplitsResponse) error {
	args := m.Called(response)

	if m.sendTriggeredChan != nil {
		close(m.sendTriggeredChan)
	}

	return args.Error(0)
}

func TestStreaming(t *testing.T) {
	logger := utils.NewTestLogger(t)

	request := &api_service_protos.TReadSplitsRequest{}
	split := &api_service_protos.TSplit{}

	t.Run("positive", func(t *testing.T) {
		ctx := context.Background()
		stream := &streamMock{}
		stream.On("Context").Return(ctx)

		writer := &paging.WriterMock{
			ColumnarBufferChan: make(chan paging.ColumnarBuffer),
		}

		writerFactory := &paging.WriterFactoryMock{}
		writerFactory.On("MakeWriter", request.GetPagination()).Return(writer, nil)

		handler := &rdbms.HandlerMock{
			ReadFinished: make(chan struct{}),
		}

		// populate channel with predefined data
		const (
			pageSize   = 1 << 10
			totalPages = 3
		)

		preparedColumnarBuffers := []paging.ColumnarBuffer{}

		for i := 0; i < totalPages; i++ {
			cb := &paging.ColumnarBufferMock{}
			response := &api_service_protos.TReadSplitsResponse{
				Payload: &api_service_protos.TReadSplitsResponse_ArrowIpcStreaming{
					ArrowIpcStreaming: make([]byte, pageSize),
				},
			}
			cb.On("ToResponse").Return(response, nil).Once()
			stream.On("Send", response).Return(nil).Once()
			cb.On("Release").Return().Once()

			preparedColumnarBuffers = append(preparedColumnarBuffers, cb)
		}

		go func() {
			// inject buffers into queue
			for _, cb := range preparedColumnarBuffers {
				writer.ColumnarBufferChan <- cb
			}

			// let handler return
			close(handler.ReadFinished)
		}()

		// read is succesfull
		readFinished := handler.
			On("ReadSplit", request.GetDataSourceInstance(), split, writer).
			Return(nil).Once()

		writer.On("Finish").Return(nil).NotBefore(readFinished).Once()

		streamer, err := NewStreamer(
			logger,
			stream,
			request,
			split,
			writerFactory,
			handler,
		)

		require.NoError(t, err)
		require.NotNil(t, streamer)

		totalBytes, err := streamer.Run()
		require.NoError(t, err)
		require.Equal(t, totalBytes, uint64(pageSize*totalPages))

		mocks := []interface{}{stream, writer, writerFactory, handler}
		for _, cb := range preparedColumnarBuffers {
			mocks = append(mocks, cb)
		}

		mock.AssertExpectationsForObjects(t, mocks...)
	})

	t.Run("handler read splits error", func(t *testing.T) {
		ctx := context.Background()
		stream := &streamMock{}
		stream.On("Context").Return(ctx)

		writer := &paging.WriterMock{
			ColumnarBufferChan: make(chan paging.ColumnarBuffer),
		}

		writerFactory := &paging.WriterFactoryMock{}
		writerFactory.On("MakeWriter", request.GetPagination()).Return(writer, nil)

		handler := &rdbms.HandlerMock{
			ReadFinished: make(chan struct{}),
		}

		// populate channel with predefined data
		const pageSize = 1 << 10

		cb := &paging.ColumnarBufferMock{}
		response1 := &api_service_protos.TReadSplitsResponse{
			Payload: &api_service_protos.TReadSplitsResponse_ArrowIpcStreaming{
				ArrowIpcStreaming: make([]byte, pageSize),
			},
		}

		cb.On("ToResponse").Return(response1, nil).Once()
		stream.On("Send", response1).Return(nil).Once()
		cb.On("Release").Return().NotBefore().Once()

		go func() {
			// after first received block an error occurs
			writer.ColumnarBufferChan <- cb

			close(handler.ReadFinished)
		}()

		// reading from data source returned an error
		readErr := fmt.Errorf("failed to read from data source")
		handler.
			On("ReadSplit", request.GetDataSourceInstance(), split, writer).
			Return(readErr).Once()

		streamer, err := NewStreamer(
			logger,
			stream,
			request,
			split,
			writerFactory,
			handler,
		)

		require.NoError(t, err)
		require.NotNil(t, streamer)

		totalBytes, err := streamer.Run()
		require.Error(t, err)
		require.True(t, errors.Is(err, readErr))
		require.Equal(t, totalBytes, uint64(pageSize))

		mock.AssertExpectationsForObjects(t, stream, writer, writerFactory, handler, cb)
	})

	t.Run("grpc stream send error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		stream := &streamMock{
			sendTriggeredChan: make(chan struct{}),
		}
		stream.On("Context").Return(ctx)

		writer := &paging.WriterMock{
			ColumnarBufferChan: make(chan paging.ColumnarBuffer),
		}

		writerFactory := &paging.WriterFactoryMock{}
		writerFactory.On("MakeWriter", request.GetPagination()).Return(writer, nil)

		handler := &rdbms.HandlerMock{
			ReadFinished: stream.sendTriggeredChan,
		}

		// populate channel with predefined data
		const pageSize = 1 << 10

		cb := &paging.ColumnarBufferMock{}
		response1 := &api_service_protos.TReadSplitsResponse{
			Payload: &api_service_protos.TReadSplitsResponse_ArrowIpcStreaming{
				ArrowIpcStreaming: make([]byte, pageSize),
			},
		}

		cb.On("ToResponse").Return(response1, nil).Once()

		// network error occures when trying to send first page to the stream
		sendErr := fmt.Errorf("GRPC error")
		send1 := stream.On("Send", response1).Return(sendErr).Once()
		cb.On("Release").Return().Once()

		go func() {
			// populate incoming queue with data
			writer.ColumnarBufferChan <- cb
		}()

		go func() {
			// trigger context cancellation after send error occurs
			<-stream.sendTriggeredChan
			cancel()
		}()

		handler.
			On("ReadSplit", request.GetDataSourceInstance(), split, writer).
			Return(nil).NotBefore(send1).Once()

		streamer, err := NewStreamer(
			logger,
			stream,
			request,
			split,
			writerFactory,
			handler,
		)

		require.NoError(t, err)
		require.NotNil(t, streamer)

		totalBytes, err := streamer.Run()
		require.Error(t, err)
		require.True(t, errors.Is(err, sendErr))
		require.Equal(t, totalBytes, uint64(0))

		mock.AssertExpectationsForObjects(t, stream, writer, writerFactory, handler, cb)
	})
}
