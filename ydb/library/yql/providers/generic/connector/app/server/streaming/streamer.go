package streaming

import (
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
	pagingWriter   paging.Writer
	totalBytesSent uint64 // TODO: replace with stats accumulator
	logger         log.Logger
}

func (s *Streamer) writeDataToStream(readErrChan <-chan error) error {
	for {
		select {
		case buffer, ok := <-s.pagingWriter.BufferQueue():
			if !ok {
				// correct termination
				return nil
			}

			// handle next data block
			if err := s.sendBufferToStream(buffer); err != nil {
				return fmt.Errorf("send buffer to stream: %w", err)
			}
		case err := <-readErrChan:
			// terminate loop in case of read error
			if err != nil {
				return fmt.Errorf("read error: %w", err)
			}

			// otherwise drain the last rows left in writer into a channel
			if err := s.pagingWriter.Finish(); err != nil {
				return fmt.Errorf("finish paging writer: %w", err)
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

func (s *Streamer) readDataFromSource() error {
	// run blocking read
	err := s.handler.ReadSplit(
		s.stream.Context(),
		s.logger,
		s.request.GetDataSourceInstance(),
		s.split,
		s.pagingWriter,
	)

	if err != nil {
		return fmt.Errorf("read split: %w", err)
	}

	return nil
}

func (s *Streamer) Run() (uint64, error) {
	readErrChan := make(chan error)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	defer wg.Wait()

	// launch read from the data source;
	// reading goroutine controls writing goroutine lifetime
	go func() {
		defer wg.Done()

		select {
		case readErrChan <- s.readDataFromSource():
		case <-s.stream.Context().Done():
		}
	}()

	// pass received blocks into the GRPC channel
	if err := s.writeDataToStream(readErrChan); err != nil {
		return s.totalBytesSent, fmt.Errorf("write data to stream: %w", err)
	}

	return s.totalBytesSent, nil
}

func NewStreamer(
	logger log.Logger,
	stream api_service.Connector_ReadSplitsServer,
	request *api_service_protos.TReadSplitsRequest,
	split *api_service_protos.TSplit,
	pagingWriterFactory paging.WriterFactory,
	handler rdbms.Handler,
) (*Streamer, error) {
	pagingWriter, err := pagingWriterFactory.MakeWriter(
		stream.Context(),
		logger,
		request.GetPagination(),
	)
	if err != nil {
		return nil, fmt.Errorf("new paging writer: %w", err)
	}

	return &Streamer{
		logger:       logger,
		stream:       stream,
		split:        split,
		request:      request,
		handler:      handler,
		pagingWriter: pagingWriter,
	}, nil
}
