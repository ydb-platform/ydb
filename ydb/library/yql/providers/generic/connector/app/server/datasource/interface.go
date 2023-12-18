package datasource

import (
	"context"

	"github.com/ydb-platform/ydb/library/go/core/log"
	api_common "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/api/common"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/paging"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

type DataSourceFactory[T utils.Acceptor] interface {
	Make(
		logger log.Logger,
		dataSourceType api_common.EDataSourceKind,
	) (DataSource[T], error)
}

// DataSource is an abstraction over external data storage that is available for data and metadata extraction.
// All new data sources must implement this interface.
// The types of data extracted from the data source are parametrized via [T utils.Acceptor] interface.
type DataSource[T utils.Acceptor] interface {
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
		sink paging.Sink[T],
	)
}
