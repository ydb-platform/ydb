package paging

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb/library/go/core/log"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

type sinkFactoryImpl struct {
	columnarBufferFactory ColumnarBufferFactory
	resultQueueCapacity   int // TODO: take from config
	rowsPerBuffer         int // TODO: take from config
}

func (sf *sinkFactoryImpl) MakeSink(
	ctx context.Context,
	logger log.Logger,
	pagination *api_service_protos.TPagination,
) (Sink, error) {
	if pagination != nil {
		return nil, fmt.Errorf("pagination settings are not supported yet")
	}

	buffer, err := sf.columnarBufferFactory.MakeBuffer()
	if err != nil {
		return nil, fmt.Errorf("wrap buffer: %w", err)
	}

	return &sinkImpl{
		bufferFactory: sf.columnarBufferFactory,
		resultQueue:   make(chan *ReadResult, sf.resultQueueCapacity),
		rowsPerBuffer: uint64(sf.rowsPerBuffer),
		currBuffer:    buffer,
		logger:        logger,
		pagination:    pagination,
		state:         operational,
		ctx:           ctx,
	}, nil
}

func NewSinkFactory(
	cbf ColumnarBufferFactory,
	resultQueueCapacity int,
	rowsPerBuffer int,
) SinkFactory {
	return &sinkFactoryImpl{
		columnarBufferFactory: cbf,
		resultQueueCapacity:   resultQueueCapacity,
		rowsPerBuffer:         rowsPerBuffer,
	}
}
