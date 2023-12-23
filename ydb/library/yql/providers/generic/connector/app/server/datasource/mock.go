package datasource

import (
	"context"

	"github.com/stretchr/testify/mock"
	"github.com/ydb-platform/ydb/library/go/core/log"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/paging"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

var _ DataSource[any] = (*DataSourceMock[any])(nil)

type DataSourceMock[T utils.Acceptor] struct {
	mock.Mock
}

func (m *DataSourceMock[T]) DescribeTable(
	ctx context.Context,
	logger log.Logger,
	request *api_service_protos.TDescribeTableRequest,
) (*api_service_protos.TDescribeTableResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *DataSourceMock[T]) ReadSplit(
	ctx context.Context,
	logger log.Logger,
	split *api_service_protos.TSplit,
	pagingWriter paging.Sink[T],
) {
	m.Called(split, pagingWriter)
}

func (m *DataSourceMock[T]) TypeMapper() utils.TypeMapper {
	panic("not implemented") // TODO: Implement
}
