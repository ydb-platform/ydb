package paging

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb/library/go/core/log"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

var _ Writer = (*writerImpl)(nil)

type writerImpl struct {
	buffer        ColumnarBuffer                  // accumulates data from rows
	bufferQueue   chan ColumnarBuffer             // outgoing buffer queue
	bufferFactory ColumnarBufferFactory           // creates new buffer
	pagination    *api_service_protos.TPagination // settings
	rowsReceived  uint64                          // simple stats
	logger        log.Logger                      // annotated logger
	operational   bool                            // flag showing if it's ready to return data
	ctx           context.Context                 // client context
}

func (pw *writerImpl) AddRow(acceptors []any) error {
	if !pw.operational {
		return fmt.Errorf("paging writer is not operational")
	}

	if err := pw.buffer.AddRow(acceptors); err != nil {
		return fmt.Errorf("acceptors to row set: %w", err)
	}

	pw.rowsReceived++

	if pw.isEnough() {
		if err := pw.flush(true); err != nil {
			return fmt.Errorf("flush: %w", err)
		}
	}

	return nil
}

func (pw *writerImpl) isEnough() bool {
	// TODO: implement pagination logic, check limits provided by client
	return pw.rowsReceived%10000 == 0
}

func (pw *writerImpl) flush(makeNewBuffer bool) error {
	select {
	case pw.bufferQueue <- pw.buffer:
	case <-pw.ctx.Done():
		return pw.ctx.Err()
	}

	var err error

	if makeNewBuffer {
		pw.buffer, err = pw.bufferFactory.MakeBuffer()
		if err != nil {
			return fmt.Errorf("make buffer: %w", err)
		}
	}

	return nil
}

func (pw *writerImpl) Finish() error {
	if err := pw.flush(false); err != nil {
		return fmt.Errorf("flush: %w", err)
	}

	pw.operational = false

	// notify reader about end of the stream
	close(pw.bufferQueue)

	return nil
}

func (pw *writerImpl) BufferQueue() <-chan ColumnarBuffer {
	return pw.bufferQueue
}
