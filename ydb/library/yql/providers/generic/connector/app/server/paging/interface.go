package paging

import (
	"context"

	"github.com/ydb-platform/ydb/library/go/core/log"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

type Writer interface {
	AddRow(acceptors []any) error
	Finish() error
	BufferQueue() <-chan ColumnarBuffer
}

type WriterFactory interface {
	MakeWriter(
		ctx context.Context,
		logger log.Logger,
		pagination *api_service_protos.TPagination,
	) (Writer, error)
}

type ColumnarBuffer interface {
	// AddRow saves a row obtained from the datasource into the buffer
	AddRow(acceptors []any) error
	// ToResponse returns all the accumulated data and clears buffer
	ToResponse() (*api_service_protos.TReadSplitsResponse, error)
	// Frees resources if buffer is no longer used
	Release()
}

type ColumnarBufferFactory interface {
	MakeBuffer() (ColumnarBuffer, error)
}
