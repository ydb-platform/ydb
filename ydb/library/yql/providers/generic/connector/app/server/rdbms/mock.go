package rdbms

import (
	"context"

	"github.com/stretchr/testify/mock"
	"github.com/ydb-platform/ydb/library/go/core/log"
	api_common "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/api/common"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/paging"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

var _ Handler = (*HandlerMock)(nil)

type HandlerMock struct {
	mock.Mock
}

func (m *HandlerMock) DescribeTable(
	ctx context.Context,
	logger log.Logger,
	request *api_service_protos.TDescribeTableRequest,
) (*api_service_protos.TDescribeTableResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *HandlerMock) ReadSplit(
	ctx context.Context,
	logger log.Logger,
	split *api_service_protos.TSplit,
	pagingWriter paging.Sink,
) {
	m.Called(split, pagingWriter)
}

func (m *HandlerMock) TypeMapper() utils.TypeMapper {
	panic("not implemented") // TODO: Implement
}

var _ HandlerFactory = (*HandlerFactoryMock)(nil)

type HandlerFactoryMock struct {
	QueryExecutor     utils.QueryExecutor
	ConnectionManager utils.ConnectionManager
	TypeMapper        utils.TypeMapper
}

func (m *HandlerFactoryMock) Make(logger log.Logger, dataSourceType api_common.EDataSourceKind) (Handler, error) {
	handler := newHandler(
		logger,
		&handlerPreset{
			queryExecutor:     m.QueryExecutor,
			connectionManager: m.ConnectionManager,
			typeMapper:        m.TypeMapper,
		},
	)

	return handler, nil
}
