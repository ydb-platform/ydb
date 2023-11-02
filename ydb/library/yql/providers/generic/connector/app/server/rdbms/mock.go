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
	ReadFinished chan struct{} // close this channel to allow
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
	dataSourceInstance *api_common.TDataSourceInstance,
	split *api_service_protos.TSplit,
	pagingWriter paging.Writer,
) error {
	<-m.ReadFinished

	args := m.Called(dataSourceInstance, split, pagingWriter)

	return args.Error(0)
}

func (m *HandlerMock) TypeMapper() utils.TypeMapper {
	panic("not implemented") // TODO: Implement
}
