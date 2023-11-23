package paging

import (
	"bytes"
	"fmt"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/ipc"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb/library/go/core/log"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

type columnarBufferArrowIPCStreamingDefault struct {
	arrowAllocator memory.Allocator
	builders       []array.Builder
	schema         *arrow.Schema
	typeMapper     utils.TypeMapper
	logger         log.Logger
	ydbTypes       []*Ydb.Type
}

// AddRow saves a row obtained from the datasource into the buffer
func (cb *columnarBufferArrowIPCStreamingDefault) addRow(acceptors []any) error {
	if len(cb.builders) != len(acceptors) {
		return fmt.Errorf("expected row %v values, got %v", len(cb.builders), len(acceptors))
	}

	if err := cb.typeMapper.AddRowToArrowIPCStreaming(cb.ydbTypes, acceptors, cb.builders); err != nil {
		return fmt.Errorf("add row to arrow IPC Streaming: %w", err)
	}

	return nil
}

// ToResponse returns all the accumulated data and clears buffer
func (cb *columnarBufferArrowIPCStreamingDefault) ToResponse() (*api_service_protos.TReadSplitsResponse, error) {
	chunk := make([]arrow.Array, 0, len(cb.builders))

	// prepare arrow record
	for _, builder := range cb.builders {
		chunk = append(chunk, builder.NewArray())
	}

	record := array.NewRecord(cb.schema, chunk, -1)

	for _, col := range chunk {
		col.Release()
	}

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

func (cb *columnarBufferArrowIPCStreamingDefault) TotalRows() int { return cb.builders[0].Len() }

// Frees resources if buffer is no longer used
func (cb *columnarBufferArrowIPCStreamingDefault) Release() {
	// cleanup builders
	for _, b := range cb.builders {
		b.Release()
	}
}
