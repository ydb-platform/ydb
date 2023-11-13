package streaming

import (
	"context"
	"fmt"
	"sync"

	"github.com/ydb-platform/ydb/library/go/core/log"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/paging"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/rdbms"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
	api_service "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

type Streamer struct {
	stream         api_service.Connector_ReadSplitsServer
	request        *api_service_protos.TReadSplitsRequest
	handler        rdbms.Handler
	split          *api_service_protos.TSplit
	sink           paging.Sink
	totalBytesSent uint64 // TODO: replace with stats accumulator
	logger         log.Logger
	ctx            context.Context // clone of a stream context
	cancel         context.CancelFunc
}

func (s *Streamer) writeDataToStream() error {
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
			if err := s.sendBufferToStream(result.ColumnarBuffer); err != nil {
				return fmt.Errorf("send buffer to stream: %w", err)
			}
		case <-s.stream.Context().Done():
			// handle request termination
			return s.stream.Context().Err()
		}
	}
}

func (s *Streamer) sendBufferToStream(buffer paging.ColumnarBuffer) error {
	// buffer must be explicitly marked as unused,
	// otherwise memory will leak
	defer buffer.Release()

	resp, err := buffer.ToResponse()
	if err != nil {
		return fmt.Errorf("buffer to response: %w", err)
	}

	utils.DumpReadSplitsResponse(s.logger, resp)

	if err := s.stream.Send(resp); err != nil {
		return fmt.Errorf("stream send: %w", err)
	}

	s.totalBytesSent += uint64(len(resp.GetArrowIpcStreaming()))

	return nil
}

func (s *Streamer) Run() (uint64, error) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	defer wg.Wait()

	// launch read from the data source;
	// subsriber goroutine controls publisher goroutine lifetime
	go func() {
		defer wg.Done()

		s.handler.ReadSplit(s.ctx, s.logger, s.split, s.sink)
	}()

	// pass received blocks into the GRPC channel
	if err := s.writeDataToStream(); err != nil {
		return s.totalBytesSent, fmt.Errorf("write data to stream: %w", err)
	}

	return s.totalBytesSent, nil
}

func NewStreamer(
	logger log.Logger,
	stream api_service.Connector_ReadSplitsServer,
	request *api_service_protos.TReadSplitsRequest,
	split *api_service_protos.TSplit,
	sink paging.Sink,
	handler rdbms.Handler,
) *Streamer {
	ctx, cancel := context.WithCancel(stream.Context())

	return &Streamer{
		logger:  logger,
		stream:  stream,
		split:   split,
		request: request,
		handler: handler,
		sink:    sink,
		ctx:     ctx,
		cancel:  cancel,
	}
}
