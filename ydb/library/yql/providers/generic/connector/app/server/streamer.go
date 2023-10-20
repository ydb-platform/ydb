package server

import (
	"fmt"

	"github.com/ydb-platform/ydb/library/go/core/log"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/paging"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/rdbms"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
	api_service "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
	"golang.org/x/sync/errgroup"
)

type Streamer struct {
	stream         api_service.Connector_ReadSplitsServer
	bufferQueue    chan paging.ColumnarBuffer
	request        *api_service_protos.TReadSplitsRequest
	handler        rdbms.Handler
	split          *api_service_protos.TSplit
	pagingWriter   *paging.Writer
	totalBytesSent uint64
	logger         log.Logger
}

func (s *Streamer) writeDataToStream() error {
	for {
		select {
		case buffer, ok := <-s.bufferQueue:
			if !ok {
				return nil
			}

			if err := s.sendBufferToStream(buffer); err != nil {
				return fmt.Errorf("send buffer to stream: %w", err)
			}
		case <-s.stream.Context().Done():
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

	// when read is finished, dump data to stream
	if err := s.pagingWriter.Finish(); err != nil {
		return fmt.Errorf("finish paging writer: %w", err)
	}

	return nil
}

func (s *Streamer) Run() (uint64, error) {
	var group errgroup.Group

	group.Go(s.readDataFromSource)
	group.Go(s.writeDataToStream)

	if err := group.Wait(); err != nil {
		return 0, fmt.Errorf("group wait: %w", err)
	}

	return s.totalBytesSent, nil
}

func NewStreamer(
	logger log.Logger,
	stream api_service.Connector_ReadSplitsServer,
	request *api_service_protos.TReadSplitsRequest,
	split *api_service_protos.TSplit,
	columnarBufferFactory *paging.ColumnarBufferFactory,
	handler rdbms.Handler,
) (*Streamer, error) {
	bufferQueue := make(chan paging.ColumnarBuffer, 10)

	pagingWriter, err := paging.NewWriter(
		stream.Context(),
		logger,
		columnarBufferFactory,
		bufferQueue,
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
		bufferQueue:  bufferQueue,
	}, nil
}
