package utils

import (
	"fmt"

	"github.com/ydb-platform/ydb/library/go/core/log"
	api_service "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

type PagingWriter struct {
	buffer       ColumnarBuffer                         // row data accumulator
	stream       api_service.Connector_ReadSplitsServer // outgoing data stream
	pagination   *api_service_protos.TPagination        // settings
	rowsReceived uint64                                 // simple stats
	logger       log.Logger                             // annotated logger
	operational  bool                                   // flag showing if it's ready to return data
}

func (pw *PagingWriter) AddRow(acceptors []any) error {
	if !pw.operational {
		return fmt.Errorf("paging writer is not operational")
	}

	if err := pw.buffer.AddRow(acceptors); err != nil {
		return fmt.Errorf("acceptors to row set: %w", err)
	}

	if pw.isEnough() {
		if err := pw.flush(); err != nil {
			return fmt.Errorf("flush: %w", err)
		}
	}

	pw.rowsReceived++

	return nil
}

func (pw *PagingWriter) isEnough() bool {
	// TODO: implement pagination logic, check limits provided by client
	return false
}

func (pw *PagingWriter) flush() error {
	if pw.buffer == nil {
		return nil
	}

	response, err := pw.buffer.ToResponse()
	if err != nil {
		return fmt.Errorf("build response: %v", err)
	}

	response.Error = NewSuccess()

	DumpReadSplitsResponse(pw.logger, response)

	if err := pw.stream.Send(response); err != nil {
		return fmt.Errorf("send stream")
	}

	pw.buffer.Release()

	pw.buffer = nil

	return nil
}

func (pw *PagingWriter) Finish() (uint64, error) {
	if err := pw.flush(); err != nil {
		return 0, fmt.Errorf("flush: %w", err)
	}

	pw.operational = false

	return pw.rowsReceived, nil
}

func NewPagingWriter(
	logger log.Logger,
	buffer ColumnarBuffer,
	stream api_service.Connector_ReadSplitsServer,
	pagination *api_service_protos.TPagination,
) (*PagingWriter, error) {
	if pagination != nil {
		return nil, fmt.Errorf("pagination settings are not supported yet")
	}

	return &PagingWriter{
		buffer:      buffer,
		logger:      logger,
		stream:      stream,
		pagination:  pagination,
		operational: true,
	}, nil
}
