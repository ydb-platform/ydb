package datasource

import (
	"context"

	"github.com/ydb-platform/ydb/library/go/core/log"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/paging"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

// DataSource describes things that any data source may do
type DataSource interface {
	// DescribeTable returns metadata about a table (or similair entity in non-relational data sources)
	// located within a particular database in a data source cluster.
	DescribeTable(
		ctx context.Context,
		logger log.Logger,
		request *api_service_protos.TDescribeTableRequest,
	) (*api_service_protos.TDescribeTableResponse, error)

	// ReadSplit is a main method for reading data from the table.
	ReadSplit(
		ctx context.Context,
		logger log.Logger,
		split *api_service_protos.TSplit,
		sink paging.Sink,
	)

	// TypeMapper helps to match external data source type system with YDB/YQL types
	TypeMapper() utils.TypeMapper
}
