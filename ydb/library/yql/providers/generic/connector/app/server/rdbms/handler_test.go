package rdbms

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	api_common "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/api/common"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/paging"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/postgresql"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

func TestReadSplit(t *testing.T) {
	ctx := context.Background()
	split := &api_service_protos.TSplit{
		Select: &api_service_protos.TSelect{
			DataSourceInstance: &api_common.TDataSourceInstance{},
			What: &api_service_protos.TSelect_TWhat{
				Items: []*api_service_protos.TSelect_TWhat_TItem{
					{
						Payload: &api_service_protos.TSelect_TWhat_TItem_Column{
							Column: &Ydb.Column{
								Name: "col1",
								Type: &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT32}},
							},
						},
					},
					{
						Payload: &api_service_protos.TSelect_TWhat_TItem_Column{
							Column: &Ydb.Column{
								Name: "col2",
								Type: &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UTF8}},
							},
						},
					},
				},
			},
			From: &api_service_protos.TSelect_TFrom{
				Table: "example_1",
			},
		},
	}

	t.Run("positive", func(t *testing.T) {
		logger := utils.NewTestLogger(t)

		connectionManager := &utils.ConnectionManagerMock{}

		p := &handlerPreset{
			connectionManager: connectionManager,
			sqlFormatter:      postgresql.NewSQLFormatter(), // TODO: parametrize
		}

		connection := &utils.ConnectionMock{}
		connectionManager.On("Make", split.Select.DataSourceInstance).Return(connection, nil).Once()
		connectionManager.On("Release", connection).Return().Once()

		rows := &utils.RowsMock{
			PredefinedData: [][]any{
				{int32(1), "a"},
				{int32(2), "b"},
			},
		}
		connection.On("Query", `SELECT "col1", "col2" FROM "example_1"`).Return(rows, nil).Once()

		col1 := new(int32)
		col2 := new(string)
		acceptors := []any{col1, col2}
		rows.On("MakeAcceptors").Return(acceptors, nil).Once()
		rows.On("Next").Return(true).Times(2)
		rows.On("Next").Return(false).Once()
		rows.On("Scan", acceptors...).Return(nil).Times(2)
		rows.On("Err").Return(nil).Once()
		rows.On("Close").Return(nil).Once()

		sink := &paging.SinkMock{}
		sink.On("AddRow", col1, col2).Return(nil).Times(2)
		sink.On("Finish").Return().Once()

		handler := newHandler(logger, p)
		handler.ReadSplit(ctx, logger, split, sink)

		mock.AssertExpectationsForObjects(t, connectionManager, connection, rows, sink)
	})

	t.Run("scan error", func(t *testing.T) {
		logger := utils.NewTestLogger(t)

		connectionManager := &utils.ConnectionManagerMock{}

		p := &handlerPreset{
			connectionManager: connectionManager,
			sqlFormatter:      postgresql.NewSQLFormatter(), // TODO: parametrize
		}

		connection := &utils.ConnectionMock{}
		connectionManager.On("Make", split.Select.DataSourceInstance).Return(connection, nil).Once()
		connectionManager.On("Release", connection).Return().Once()

		rows := &utils.RowsMock{
			PredefinedData: [][]any{
				{int32(1), "a"},
				{int32(2), "b"},
			},
		}
		connection.On("Query", `SELECT "col1", "col2" FROM "example_1"`).Return(rows, nil).Once()

		col1 := new(int32)
		col2 := new(string)
		acceptors := []any{col1, col2}
		scanErr := fmt.Errorf("scan failed")

		rows.On("MakeAcceptors").Return(acceptors, nil).Once()
		rows.On("Next").Return(true).Times(2)
		rows.On("Scan", acceptors...).Return(nil).Once()
		rows.On("Scan", acceptors...).Return(scanErr).Once()
		rows.On("Close").Return(nil).Once()

		sink := &paging.SinkMock{}
		sink.On("AddRow", col1, col2).Return(nil).Once()
		sink.On("AddError", mock.MatchedBy(func(err error) bool {
			return errors.Is(err, scanErr)
		})).Return().Once()
		sink.On("Finish").Return().Once()

		handler := newHandler(logger, p)
		handler.ReadSplit(ctx, logger, split, sink)

		mock.AssertExpectationsForObjects(t, connectionManager, connection, rows, sink)
	})
}
