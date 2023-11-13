package utils

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	api_common "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/api/common"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

func MakeTestSplit() *api_service_protos.TSplit {
	return &api_service_protos.TSplit{
		Select: &api_service_protos.TSelect{
			DataSourceInstance: &api_common.TDataSourceInstance{},
			What: &api_service_protos.TSelect_TWhat{
				Items: []*api_service_protos.TSelect_TWhat_TItem{
					{
						Payload: &api_service_protos.TSelect_TWhat_TItem_Column{
							Column: &Ydb.Column{
								Name: "col0",
								Type: &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT32}},
							},
						},
					},
					{
						Payload: &api_service_protos.TSelect_TWhat_TItem_Column{
							Column: &Ydb.Column{
								Name: "col1",
								Type: &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_STRING}},
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
}

// DataConverter should be used only from unit tests
type DataConverter struct{}

func (dc DataConverter) RowsToColumnBlocks(input [][]any, rowsPerBlock int) [][][]any {
	var (
		totalColumns = len(input[0])
		results      [][][]any
	)

	for i := 0; i < len(input); i += rowsPerBlock {
		start := i

		end := start + rowsPerBlock
		if end > len(input) {
			end = len(input)
		}

		result := dc.rowGroupToColumnBlock(input, totalColumns, start, end)

		results = append(results, result)
	}

	return results
}

func (dc DataConverter) rowGroupToColumnBlock(input [][]any, totalColumns, start, end int) [][]any {
	columnarData := make([][]any, totalColumns)

	for columnID := range columnarData {
		for rowID := range input[start:end] {
			columnarData[columnID] = append(columnarData[columnID], input[rowID+start][columnID])
		}
	}

	return columnarData
}
