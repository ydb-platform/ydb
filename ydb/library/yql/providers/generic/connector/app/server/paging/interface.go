package paging

import (
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

type ColumnarBuffer[T utils.Acceptor] interface {
	// addRow saves a row obtained from the datasource into the columnar buffer
	addRow(rowTransformer utils.RowTransformer[T]) error
	// ToResponse returns all the accumulated data and clears buffer
	ToResponse() (*api_service_protos.TReadSplitsResponse, error)
	// Release frees resources if buffer is no longer used
	Release()
	// TotalRows return the number of rows accumulated
	TotalRows() int
}

type ColumnarBufferFactory[T utils.Acceptor] interface {
	MakeBuffer() (ColumnarBuffer[T], error)
}

// ReadResult is an algebraic data type containing:
// 1. a buffer (e. g. page) packed with data
// 2. result of read operation (potentially with error)
type ReadResult[T utils.Acceptor] struct {
	ColumnarBuffer ColumnarBuffer[T]
	Stats          *api_service_protos.TReadSplitsResponse_TStats
	Error          error
}

// Sink is a destination for a data stream that is read out of an external data source.
type Sink[T utils.Acceptor] interface {
	// AddRow saves the row obtained from a stream incoming from an external data source.
	AddRow(rowTransformer utils.RowTransformer[T]) error
	// AddError propagates an error occured during the reading from the external data source.
	AddError(err error)
	// Finish reports the successful completion of reading the data stream.
	Finish()
	// ResultQueue returns a channel with results
	ResultQueue() <-chan *ReadResult[T]
}
