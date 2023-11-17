package paging

import (
	"github.com/stretchr/testify/mock"
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

var _ ColumnarBuffer = (*ColumnarBufferMock)(nil)

type ColumnarBufferMock struct {
	mock.Mock
}

func (m *ColumnarBufferMock) addRow(acceptors []any) error {
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
