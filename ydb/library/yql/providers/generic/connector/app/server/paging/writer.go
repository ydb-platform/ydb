package paging

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb/library/go/core/log"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

type Writer struct {
	buffer        ColumnarBuffer                  // accumulates data from rows
	bufferQueue   chan<- ColumnarBuffer           // outgoing buffer queue
	bufferFactory *ColumnarBufferFactory          // creates new buffer
	pagination    *api_service_protos.TPagination // settings
	rowsReceived  uint64                          // simple stats
	logger        log.Logger                      // annotated logger
	operational   bool                            // flag showing if it's ready to return data
	ctx           context.Context                 // client context
}

func (pw *Writer) AddRow(acceptors []any) error {
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

func (pw *Writer) isEnough() bool {
	// TODO: implement pagination logic, check limits provided by client
	return pw.rowsReceived%10000 == 0
}

func (pw *Writer) flush() error {
	select {
	case pw.bufferQueue <- pw.buffer:
	case <-pw.ctx.Done():
		return pw.ctx.Err()
	}

	var err error

	pw.buffer, err = pw.bufferFactory.MakeBuffer()
	if err != nil {
		return fmt.Errorf("make buffer: %w", err)
	}

	return nil
}

func (pw *Writer) Finish() error {
	if err := pw.flush(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}

	pw.operational = false

	// notify reader about end of the stream
	close(pw.bufferQueue)

	return nil
}

func NewWriter(
	ctx context.Context,
	logger log.Logger,
	bufferFactory *ColumnarBufferFactory,
	bufferQueue chan<- ColumnarBuffer,
	pagination *api_service_protos.TPagination,
) (*Writer, error) {
	if pagination != nil {
		return nil, fmt.Errorf("pagination settings are not supported yet")
	}

	buffer, err := bufferFactory.MakeBuffer()
	if err != nil {
		return nil, fmt.Errorf("wrap buffer: %w", err)
	}

	return &Writer{
		bufferFactory: bufferFactory,
		bufferQueue:   bufferQueue,
		buffer:        buffer,
		logger:        logger,
		pagination:    pagination,
		operational:   true,
		ctx:           ctx,
	}, nil
}
