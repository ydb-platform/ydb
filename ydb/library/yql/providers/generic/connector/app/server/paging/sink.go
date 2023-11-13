//go:generate stringer -type=sinkState -output=sink_string.go
package paging

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb/library/go/core/log"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

type sinkState int8

const (
	operational sinkState = iota + 1
	failed
	finished
)

var _ Sink = (*sinkImpl)(nil)

type sinkImpl struct {
	currBuffer    ColumnarBuffer                  // accumulates incoming rows
	resultQueue   chan *ReadResult                // outgoing buffer queue
	bufferFactory ColumnarBufferFactory           // creates new buffer
	pagination    *api_service_protos.TPagination // settings
	rowsReceived  uint64                          // simple stats
	rowsPerBuffer uint64                          // TODO: use cfg
	logger        log.Logger                      // annotated logger
	state         sinkState                       // flag showing if it's ready to return data
	ctx           context.Context                 // client context
}

func (s *sinkImpl) AddRow(acceptors []any) error {
	if s.state != operational {
		panic(s.unexpectedState(operational))
	}

	if err := s.currBuffer.AddRow(acceptors); err != nil {
		return fmt.Errorf("acceptors to row set: %w", err)
	}

	s.rowsReceived++

	if s.isEnough() {
		if err := s.flush(true); err != nil {
			return fmt.Errorf("flush: %w", err)
		}
	}

	return nil
}

func (s *sinkImpl) AddError(err error) {
	if s.state != operational {
		panic(s.unexpectedState(operational))
	}

	s.respondWith(nil, err)

	s.state = failed
}

func (s *sinkImpl) isEnough() bool {
	// TODO: implement pagination logic, check limits provided by client or config
	return s.rowsReceived%s.rowsPerBuffer == 0
}

func (s *sinkImpl) flush(makeNewBuffer bool) error {
	if s.currBuffer.TotalRows() == 0 {
		return nil
	}

	s.respondWith(s.currBuffer, nil)

	s.currBuffer = nil

	if makeNewBuffer {
		var err error

		s.currBuffer, err = s.bufferFactory.MakeBuffer()
		if err != nil {
			return fmt.Errorf("make buffer: %w", err)
		}
	}

	return nil
}

func (s *sinkImpl) Finish() {
	if s.state != operational && s.state != failed {
		panic(s.unexpectedState(operational, failed))
	}

	// if there is some data left, send it to the channel
	if s.state == operational {
		err := s.flush(false)
		if err != nil {
			s.respondWith(nil, fmt.Errorf("flush: %w", err))
			s.state = failed
		} else {
			s.state = finished
		}
	}

	// notify reader about the end of data
	close(s.resultQueue)
}

func (s *sinkImpl) ResultQueue() <-chan *ReadResult {
	return s.resultQueue
}

func (s *sinkImpl) respondWith(buf ColumnarBuffer, err error) {
	select {
	case s.resultQueue <- &ReadResult{ColumnarBuffer: buf, Error: err}:
	case <-s.ctx.Done():
	}
}

func (s *sinkImpl) unexpectedState(expected ...sinkState) error {
	return fmt.Errorf(
		"unexpected state '%v' (expected are '%v'): %w",
		s.state, expected, utils.ErrInvariantViolation)
}
