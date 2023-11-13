package paging

import (
	"context"

	"github.com/stretchr/testify/mock"
	"github.com/ydb-platform/ydb/library/go/core/log"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

var _ Sink = (*SinkMock)(nil)

type SinkMock struct {
	mock.Mock
}

func (m *SinkMock) AddRow(acceptors []any) error {
	args := m.Called(acceptors...)

	return args.Error(0)
}

func (m *SinkMock) AddError(err error) {
	m.Called(err)
}

func (m *SinkMock) Finish() {
	m.Called()
}

func (m *SinkMock) ResultQueue() <-chan *ReadResult {
	return m.Called().Get(0).(chan *ReadResult)
}

var _ SinkFactory = (*SinkFactoryMock)(nil)

type SinkFactoryMock struct {
	mock.Mock
}

func (m *SinkFactoryMock) MakeSink(
	ctx context.Context,
	logger log.Logger,
	pagination *api_service_protos.TPagination,
) (Sink, error) {
	args := m.Called(pagination)

	return args.Get(0).(Sink), args.Error(1)
}

var _ ColumnarBuffer = (*ColumnarBufferMock)(nil)

type ColumnarBufferMock struct {
	mock.Mock
}

func (m *ColumnarBufferMock) AddRow(acceptors []any) error {
	panic("not implemented") // TODO: Implement
}

func (m *ColumnarBufferMock) ToResponse() (*api_service_protos.TReadSplitsResponse, error) {
	args := m.Called()

	return args.Get(0).(*api_service_protos.TReadSplitsResponse), args.Error(1)
}

func (m *ColumnarBufferMock) Release() {
	m.Called()
}

func (m *ColumnarBufferMock) TotalRows() int {
	panic("not implemented") // TODO: Implement
}
