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

var _ Sink[any] = (*sinkImpl[any])(nil)
var _ Sink[string] = (*sinkImpl[string])(nil)

type sinkImpl[T utils.Acceptor] struct {
	currBuffer     ColumnarBuffer[T]        // accumulates incoming rows
	resultQueue    chan *ReadResult[T]      // outgoing buffer queue
	bufferFactory  ColumnarBufferFactory[T] // creates new buffer
	trafficTracker *TrafficTracker[T]       // tracks the amount of data passed through the sink
	readLimiter    ReadLimiter              // helps to restrict the number of rows read in every request
	logger         log.Logger               // annotated logger
	state          sinkState                // flag showing if it's ready to return data
	ctx            context.Context          // client context
}

func (s *sinkImpl[T]) AddRow(rowTransformer utils.RowTransformer[T]) error {
	if s.state != operational {
		panic(s.unexpectedState(operational))
	}

	if err := s.readLimiter.addRow(); err != nil {
		return fmt.Errorf("add row to read limiter: %w", err)
	}

	// Check if we can add one more data row
	// without exceeding page size limit.
	ok, err := s.trafficTracker.tryAddRow(rowTransformer.GetAcceptors())
	if err != nil {
		return fmt.Errorf("add row to traffic tracker: %w", err)
	}

	// If page is already too large, flush buffer to the channel and create a new one
	if !ok {
		if err := s.flush(true); err != nil {
			return fmt.Errorf("flush: %w", err)
		}

		_, err := s.trafficTracker.tryAddRow(rowTransformer.GetAcceptors())
		if err != nil {
			return fmt.Errorf("add row to traffic tracker: %w", err)
		}
	}

	// Append row data to the columnar buffer
	if err := s.currBuffer.addRow(rowTransformer); err != nil {
		return fmt.Errorf("add row to buffer: %w", err)
	}

	return nil
}

func (s *sinkImpl[T]) AddError(err error) {
	if s.state != operational {
		panic(s.unexpectedState(operational))
	}

	s.respondWith(nil, nil, err)

	s.state = failed
}

func (s *sinkImpl[T]) flush(makeNewBuffer bool) error {
	if s.currBuffer.TotalRows() == 0 {
		return nil
	}

	stats := s.trafficTracker.DumpStats(false)

	// enqueue message to GRPC stream
	s.respondWith(s.currBuffer, stats, nil)

	// create empty buffer and reset counters
	s.currBuffer = nil
	s.trafficTracker.refreshCounters()

	if makeNewBuffer {
		var err error

		s.currBuffer, err = s.bufferFactory.MakeBuffer()
		if err != nil {
			return fmt.Errorf("make buffer: %w", err)
		}
	}

	return nil
}

func (s *sinkImpl[T]) Finish() {
	if s.state != operational && s.state != failed {
		panic(s.unexpectedState(operational, failed))
	}

	// if there is some data left, send it to the channel
	if s.state == operational {
		err := s.flush(false)
		if err != nil {
			s.respondWith(nil, nil, fmt.Errorf("flush: %w", err))
			s.state = failed
		} else {
			s.state = finished
		}
	}

	// notify reader about the end of data
	close(s.resultQueue)
}

func (s *sinkImpl[T]) ResultQueue() <-chan *ReadResult[T] {
	return s.resultQueue
}

func (s *sinkImpl[T]) respondWith(
	buf ColumnarBuffer[T],
	stats *api_service_protos.TReadSplitsResponse_TStats,
	err error) {
	select {
	case s.resultQueue <- &ReadResult[T]{ColumnarBuffer: buf, Stats: stats, Error: err}:
	case <-s.ctx.Done():
	}
}

func (s *sinkImpl[T]) unexpectedState(expected ...sinkState) error {
	return fmt.Errorf(
		"unexpected state '%v' (expected are '%v'): %w",
		s.state, expected, utils.ErrInvariantViolation)
}

func NewSink[T utils.Acceptor](
	ctx context.Context,
	logger log.Logger,
	trafficTracker *TrafficTracker[T],
	columnarBufferFactory ColumnarBufferFactory[T],
	readLimiter ReadLimiter,
	resultQueueCapacity int,
) (Sink[T], error) {
	buffer, err := columnarBufferFactory.MakeBuffer()
	if err != nil {
		return nil, fmt.Errorf("wrap buffer: %w", err)
	}

	return &sinkImpl[T]{
		bufferFactory:  columnarBufferFactory,
		readLimiter:    readLimiter,
		resultQueue:    make(chan *ReadResult[T], resultQueueCapacity),
		trafficTracker: trafficTracker,
		currBuffer:     buffer,
		logger:         logger,
		state:          operational,
		ctx:            ctx,
	}, nil
}
