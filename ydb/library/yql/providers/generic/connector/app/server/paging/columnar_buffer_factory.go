package paging

import (
	"fmt"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb/library/go/core/log"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

type columnarBufferFactoryImpl struct {
	arrowAllocator     memory.Allocator
	readLimiterFactory *ReadLimiterFactory
	logger             log.Logger
	format             api_service_protos.TReadSplitsRequest_EFormat
	schema             *arrow.Schema
	ydbTypes           []*Ydb.Type
	typeMapper         utils.TypeMapper
}

func (cbf *columnarBufferFactoryImpl) MakeBuffer() (ColumnarBuffer, error) {
	switch cbf.format {
	case api_service_protos.TReadSplitsRequest_ARROW_IPC_STREAMING:
		builders, err := utils.YdbTypesToArrowBuilders(cbf.ydbTypes, cbf.arrowAllocator)
		if err != nil {
			return nil, fmt.Errorf("convert Select.What to arrow.Schema: %w", err)
		}

		if len(cbf.ydbTypes) == 0 {
			return &columnarBufferArrowIPCStreamingEmptyColumns{
				arrowAllocator: cbf.arrowAllocator,
				typeMapper:     cbf.typeMapper,
				schema:         cbf.schema,
				rowsAdded:      0,
			}, nil
		}

		return &columnarBufferArrowIPCStreamingDefault{
			arrowAllocator: cbf.arrowAllocator,
			builders:       builders,
			typeMapper:     cbf.typeMapper,
			ydbTypes:       cbf.ydbTypes,
			schema:         cbf.schema,
			logger:         cbf.logger,
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
) (ColumnarBufferFactory, error) {
	ydbTypes, err := utils.SelectWhatToYDBTypes(selectWhat)
	if err != nil {
		return nil, fmt.Errorf("convert Select.What to Ydb types: %w", err)
	}

	schema, err := utils.SelectWhatToArrowSchema(selectWhat)
	if err != nil {
		return nil, fmt.Errorf("convert Select.What to Arrow schema: %w", err)
	}

	cbf := &columnarBufferFactoryImpl{
		logger:             logger,
		arrowAllocator:     arrowAllocator,
		readLimiterFactory: readLimiterFactory,
		format:             format,
		schema:             schema,
		typeMapper:         typeMapper,
		ydbTypes:           ydbTypes,
	}

	return cbf, nil
}
