package streaming

import (
	"context"
	"fmt"
	"sync"

	"github.com/ydb-platform/ydb/library/go/core/log"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/datasource"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/paging"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
	api_service "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

type Streamer[T utils.Acceptor] struct {
	stream     api_service.Connector_ReadSplitsServer
	request    *api_service_protos.TReadSplitsRequest
	dataSource datasource.DataSource[T]
	split      *api_service_protos.TSplit
	sink       paging.Sink[T]
	logger     log.Logger
	ctx        context.Context // clone of a stream context
	cancel     context.CancelFunc
}

func (s *Streamer[T]) writeDataToStream() error {
	// exit from this function will cause publisher's goroutine termination as well
	defer s.cancel()

	for {
		select {
		case result, ok := <-s.sink.ResultQueue():
			if !ok {
				// correct termination
				return nil
			}

			if result.Error != nil {
				return fmt.Errorf("read result: %w", result.Error)
			}

			// handle next data block
			if err := s.sendResultToStream(result); err != nil {
				return fmt.Errorf("send buffer to stream: %w", err)
			}
		case <-s.stream.Context().Done():
			// handle request termination
			return s.stream.Context().Err()
		}
	}
}

func (s *Streamer[T]) sendResultToStream(result *paging.ReadResult[T]) error {
	// buffer must be explicitly marked as unused,
	// otherwise memory will leak
	defer result.ColumnarBuffer.Release()

	resp, err := result.ColumnarBuffer.ToResponse()
	if err != nil {
		return fmt.Errorf("buffer to response: %w", err)
	}

	resp.Stats = result.Stats

	utils.DumpReadSplitsResponse(s.logger, resp)

	if err := s.stream.Send(resp); err != nil {
		return fmt.Errorf("stream send: %w", err)
	}

	return nil
}

func (s *Streamer[T]) Run() error {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	defer wg.Wait()

	// Launch reading from the data source.
	// Subsriber goroutine controls publisher goroutine lifetime.
	go func() {
		defer wg.Done()

		s.dataSource.ReadSplit(s.ctx, s.logger, s.split, s.sink)
	}()

	// pass received blocks into the GRPC channel
	if err := s.writeDataToStream(); err != nil {
		return fmt.Errorf("write data to stream: %w", err)
	}

	return nil
}

func NewStreamer[T utils.Acceptor](
	logger log.Logger,
	stream api_service.Connector_ReadSplitsServer,
	request *api_service_protos.TReadSplitsRequest,
	split *api_service_protos.TSplit,
	sink paging.Sink[T],
	dataSource datasource.DataSource[T],
) *Streamer[T] {
	ctx, cancel := context.WithCancel(stream.Context())

	return &Streamer[T]{
		logger:     logger,
		stream:     stream,
		split:      split,
		request:    request,
		dataSource: dataSource,
		sink:       sink,
		ctx:        ctx,
		cancel:     cancel,
	}
}
