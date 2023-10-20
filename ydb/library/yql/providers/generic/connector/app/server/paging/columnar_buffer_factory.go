package paging

import (
	"fmt"

	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/ydb-platform/ydb/library/go/core/log"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
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
	logger             log.Logger
	format             api_service_protos.TReadSplitsRequest_EFormat
	selectWhat         *api_service_protos.TSelect_TWhat
	typeMapper         utils.TypeMapper
}

func (cbf *ColumnarBufferFactory) MakeBuffer() (ColumnarBuffer, error) {
	switch cbf.format {
	case api_service_protos.TReadSplitsRequest_ARROW_IPC_STREAMING:
		schema, builders, err := utils.SelectWhatToArrow(cbf.selectWhat, cbf.arrowAllocator)
		if err != nil {
			return nil, fmt.Errorf("convert Select.What to arrow.Schema: %w", err)
		}

		ydbTypes, err := utils.SelectWhatToYDBTypes(cbf.selectWhat)
		if err != nil {
			return nil, fmt.Errorf("convert Select.What to Ydb.Types: %w", err)
		}

		if len(ydbTypes) == 0 {
			return &columnarBufferArrowIPCStreamingEmptyColumns{
				arrowAllocator: cbf.arrowAllocator,
				schema:         schema,
				readLimiter:    cbf.readLimiterFactory.MakeReadLimiter(cbf.logger),
				typeMapper:     cbf.typeMapper,
				rowsAdded:      0,
			}, nil
		}

		return &columnarBufferArrowIPCStreaming{
			arrowAllocator: cbf.arrowAllocator,
			schema:         schema,
			builders:       builders,
			readLimiter:    cbf.readLimiterFactory.MakeReadLimiter(cbf.logger),
			typeMapper:     cbf.typeMapper,
			ydbTypes:       ydbTypes,
		}, nil
	default:
		return nil, fmt.Errorf("unknown format: %v", cbf.format)
	}
}

func NewColumnarBufferFactory(
	logger log.Logger,
	arrowAllocator memory.Allocator,
	readLimiterFactory *ReadLimiterFactory,
	format api_service_protos.TReadSplitsRequest_EFormat,
	selectWhat *api_service_protos.TSelect_TWhat,
	typeMapper utils.TypeMapper,
) *ColumnarBufferFactory {
	return &ColumnarBufferFactory{
		logger:             logger,
		arrowAllocator:     arrowAllocator,
		readLimiterFactory: readLimiterFactory,
		format:             format,
		selectWhat:         selectWhat,
		typeMapper:         typeMapper,
	}
}
