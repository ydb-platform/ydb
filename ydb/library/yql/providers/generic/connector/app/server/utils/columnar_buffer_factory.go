package utils

import (
	"fmt"

	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/ydb-platform/ydb/library/go/core/log"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

type ColumnarBuffer interface {
	// AddRow saves a row obtained from the datasource into the buffer
	AddRow(acceptors []any) error
	// ToResponse returns all the accumulated data and clears buffer
	ToResponse() (*api_service_protos.TReadSplitsResponse, error)
	// Frees resources if buffer is no longer used
	Release()
}

type ColumnarBufferFactory struct {
	arrowAllocator     memory.Allocator
	readLimiterFactory *ReadLimiterFactory
}

func (cbf *ColumnarBufferFactory) MakeBuffer(
	logger log.Logger,
	format api_service_protos.TReadSplitsRequest_EFormat,
	selectWhat *api_service_protos.TSelect_TWhat,
	typeMapper TypeMapper,
) (ColumnarBuffer, error) {
	switch format {
	case api_service_protos.TReadSplitsRequest_ARROW_IPC_STREAMING:
		schema, builders, err := SelectWhatToArrow(selectWhat, cbf.arrowAllocator)
		if err != nil {
			return nil, fmt.Errorf("convert Select.What to arrow.Schema: %w", err)
		}

		ydbTypes, err := SelectWhatToYDBTypes(selectWhat)
		if err != nil {
			return nil, fmt.Errorf("convert Select.What to Ydb.Types: %w", err)
		}

		return &columnarBufferArrowIPCStreaming{
			arrowAllocator: cbf.arrowAllocator,
			schema:         schema,
			builders:       builders,
			readLimiter:    cbf.readLimiterFactory.MakeReadLimiter(logger),
			typeMapper:     typeMapper,
			ydbTypes:       ydbTypes,
		}, nil
	default:
		return nil, fmt.Errorf("unknown format: %v", format)
	}
}

func NewColumnarBufferFactory(
	arrowAllocator memory.Allocator,
	readLimiterFactory *ReadLimiterFactory,
) *ColumnarBufferFactory {
	return &ColumnarBufferFactory{
		arrowAllocator:     arrowAllocator,
		readLimiterFactory: readLimiterFactory,
	}
}
