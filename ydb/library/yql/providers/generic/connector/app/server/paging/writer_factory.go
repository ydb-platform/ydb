package paging

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb/library/go/core/log"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

type writerFactoryImpl struct {
	columnarBufferFactory ColumnarBufferFactory
}

func (wf *writerFactoryImpl) MakeWriter(
	ctx context.Context,
	logger log.Logger,
	pagination *api_service_protos.TPagination,
) (Writer, error) {
	if pagination != nil {
		return nil, fmt.Errorf("pagination settings are not supported yet")
	}

	buffer, err := wf.columnarBufferFactory.MakeBuffer()
	if err != nil {
		return nil, fmt.Errorf("wrap buffer: %w", err)
	}

	return &writerImpl{
		bufferFactory: wf.columnarBufferFactory,
		bufferQueue:   make(chan ColumnarBuffer, 10), // TODO: use config
		buffer:        buffer,
		logger:        logger,
		pagination:    pagination,
		operational:   true,
		ctx:           ctx,
	}, nil
}

func NewWriterFactory(cbf ColumnarBufferFactory) WriterFactory {
	return &writerFactoryImpl{
		columnarBufferFactory: cbf,
	}
}
