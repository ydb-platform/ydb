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
	currBuffer     ColumnarBuffer        // accumulates incoming rows
	resultQueue    chan *ReadResult      // outgoing buffer queue
	bufferFactory  ColumnarBufferFactory // creates new buffer
	trafficTracker *TrafficTracker       // tracks the amount of data passed through the sink
	readLimiter    ReadLimiter           // helps to restrict the number of rows read in every request
	logger         log.Logger            // annotated logger
	state          sinkState             // flag showing if it's ready to return data
	ctx            context.Context       // client context
}

func (s *sinkImpl) AddRow(acceptors []any) error {
	if s.state != operational {
		panic(s.unexpectedState(operational))
	}

	if err := s.readLimiter.addRow(); err != nil {
		return fmt.Errorf("add row to read limiter: %w", err)
	}

	// Check if we can add one more data row
	// without exceeding page size limit.
	ok, err := s.trafficTracker.tryAddRow(acceptors)
	if err != nil {
		return fmt.Errorf("add row to traffic tracker: %w", err)
	}

	// If page is already too large, flush buffer to the channel and create a new one
	if !ok {
		if err := s.flush(true); err != nil {
			return fmt.Errorf("flush: %w", err)
		}

		_, err := s.trafficTracker.tryAddRow(acceptors)
		if err != nil {
			return fmt.Errorf("add row to traffic tracker: %w", err)
		}
	}

	// Add physical data to the buffer
	if err := s.currBuffer.addRow(acceptors); err != nil {
		return fmt.Errorf("add row to buffer: %w", err)
	}

	return nil
}

func (s *sinkImpl) AddError(err error) {
	if s.state != operational {
		panic(s.unexpectedState(operational))
	}

	s.respondWith(nil, nil, err)

	s.state = failed
}

func (s *sinkImpl) flush(makeNewBuffer bool) error {
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

func (s *sinkImpl) Finish() {
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

func (s *sinkImpl) ResultQueue() <-chan *ReadResult {
	return s.resultQueue
}

func (s *sinkImpl) respondWith(
	buf ColumnarBuffer,
	stats *api_service_protos.TReadSplitsResponse_TStats,
	err error) {
	select {
	case s.resultQueue <- &ReadResult{ColumnarBuffer: buf, Stats: stats, Error: err}:
	case <-s.ctx.Done():
	}
}

func (s *sinkImpl) unexpectedState(expected ...sinkState) error {
	return fmt.Errorf(
		"unexpected state '%v' (expected are '%v'): %w",
		s.state, expected, utils.ErrInvariantViolation)
}

func NewSink(
	ctx context.Context,
	logger log.Logger,
	trafficTracker *TrafficTracker,
	columnarBufferFactory ColumnarBufferFactory,
	readLimiter ReadLimiter,
	resultQueueCapacity int,
) (Sink, error) {
	buffer, err := columnarBufferFactory.MakeBuffer()
	if err != nil {
		return nil, fmt.Errorf("wrap buffer: %w", err)
	}

	return &sinkImpl{
		bufferFactory:  columnarBufferFactory,
		readLimiter:    readLimiter,
		resultQueue:    make(chan *ReadResult, resultQueueCapacity),
		trafficTracker: trafficTracker,
		currBuffer:     buffer,
		logger:         logger,
		state:          operational,
		ctx:            ctx,
	}, nil
}
