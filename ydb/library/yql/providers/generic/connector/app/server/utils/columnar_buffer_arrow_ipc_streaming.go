package utils

import (
	"bytes"
	"fmt"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/ipc"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

type columnarBufferArrowIPCStreaming struct {
	arrowAllocator memory.Allocator
	builders       []array.Builder
	readLimiter    ReadLimiter
	schema         *arrow.Schema
	typeMapper     TypeMapper
	ydbTypes       []*Ydb.Type
}

// AddRow saves a row obtained from the datasource into the buffer
func (cb *columnarBufferArrowIPCStreaming) AddRow(acceptors []any) error {
	if len(cb.builders) != len(acceptors) {
		return fmt.Errorf("expected row %v values, got %v", len(cb.builders), len(acceptors))
	}

	if err := cb.readLimiter.AddRow(); err != nil {
		return fmt.Errorf("check read limiter: %w", err)
	}

	if err := cb.typeMapper.AddRowToArrowIPCStreaming(cb.ydbTypes, acceptors, cb.builders); err != nil {
		return fmt.Errorf("add row to arrow IPC Streaming: %w", err)
	}

	return nil
}

// ToResponse returns all the accumulated data and clears buffer
func (cb *columnarBufferArrowIPCStreaming) ToResponse() (*api_service_protos.TReadSplitsResponse, error) {
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

// Frees resources if buffer is no longer used
func (cb *columnarBufferArrowIPCStreaming) Release() {
	// cleanup builders
	for _, b := range cb.builders {
		b.Release()
	}
}
