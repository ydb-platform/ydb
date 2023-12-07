package paging

import (
	"bytes"
	"fmt"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/ipc"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

// special implementation for buffer that writes schema with empty columns set
type columnarBufferArrowIPCStreamingEmptyColumns struct {
	arrowAllocator memory.Allocator
	schema         *arrow.Schema
	rowsAdded      int
}

// AddRow saves a row obtained from the datasource into the buffer
func (cb *columnarBufferArrowIPCStreamingEmptyColumns) addRow(transformer utils.Transformer) error {
	if len(transformer.GetAcceptors()) != 1 {
		return fmt.Errorf("expected 1 value, got %v", len(transformer.GetAcceptors()))
	}

	cb.rowsAdded++

	return nil
}

// ToResponse returns all the accumulated data and clears buffer
func (cb *columnarBufferArrowIPCStreamingEmptyColumns) ToResponse() (*api_service_protos.TReadSplitsResponse, error) {
	columns := make([]arrow.Array, 0)

	record := array.NewRecord(cb.schema, columns, int64(cb.rowsAdded))

	// prepare arrow writer
	var buf bytes.Buffer

	writer := ipc.NewWriter(&buf, ipc.WithSchema(cb.schema), ipc.WithAllocator(cb.arrowAllocator))

	if err := writer.Write(record); err != nil {
		return nil, fmt.Errorf("write record: %w", err)
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("close arrow writer: %w", err)
	}

	out := &api_service_protos.TReadSplitsResponse{
		Payload: &api_service_protos.TReadSplitsResponse_ArrowIpcStreaming{
			ArrowIpcStreaming: buf.Bytes(),
		},
	}

	return out, nil
}

func (cb *columnarBufferArrowIPCStreamingEmptyColumns) TotalRows() int { return int(cb.rowsAdded) }

// Frees resources if buffer is no longer used
func (cb *columnarBufferArrowIPCStreamingEmptyColumns) Release() {
}
