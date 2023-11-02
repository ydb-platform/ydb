package paging

import (
	"context"

	"github.com/stretchr/testify/mock"
	"github.com/ydb-platform/ydb/library/go/core/log"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

var _ Writer = (*WriterMock)(nil)

type WriterMock struct {
	ColumnarBufferChan chan ColumnarBuffer
	mock.Mock
}

func (m *WriterMock) AddRow(acceptors []any) error {
	panic("not implemented") // TODO: Implement
}

func (m *WriterMock) Finish() error {
	args := m.Called()

	close(m.ColumnarBufferChan)

	return args.Error(0)
}

func (m *WriterMock) BufferQueue() <-chan ColumnarBuffer {
	return m.ColumnarBufferChan
}

var _ WriterFactory = (*WriterFactoryMock)(nil)

type WriterFactoryMock struct {
	mock.Mock
}

func (m *WriterFactoryMock) MakeWriter(
	ctx context.Context,
	logger log.Logger,
	pagination *api_service_protos.TPagination,
) (Writer, error) {
	args := m.Called(pagination)

	return args.Get(0).(Writer), args.Error(1)
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
