package utils

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	api_common "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/api/common"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

func NewPrimitiveType(t Ydb.Type_PrimitiveTypeId) *Ydb.Type {
	return &Ydb.Type{
		Type: &Ydb.Type_TypeId{
			TypeId: t,
		},
	}
}

// NewDefaultWhat generates default What field with a pair of columns
func NewDefaultWhat() *api_service_protos.TSelect_TWhat {
	return &api_service_protos.TSelect_TWhat{
		Items: []*api_service_protos.TSelect_TWhat_TItem{
			&api_service_protos.TSelect_TWhat_TItem{
				Payload: &api_service_protos.TSelect_TWhat_TItem_Column{
					Column: &Ydb.Column{
						Name: "col0",
						Type: NewPrimitiveType(Ydb.Type_INT32),
					},
				},
			},
			&api_service_protos.TSelect_TWhat_TItem{
				Payload: &api_service_protos.TSelect_TWhat_TItem_Column{
					Column: &Ydb.Column{
						Name: "col1",
						Type: NewPrimitiveType(Ydb.Type_STRING),
					},
				},
			},
		},
	}
}

func NewColumnExpression(name string) *api_service_protos.TExpression {
	return &api_service_protos.TExpression{
		Payload: &api_service_protos.TExpression_Column{
			Column: name,
		},
	}
}

func NewInt32ValueExpression(val int32) *api_service_protos.TExpression {
	return &api_service_protos.TExpression{
		Payload: &api_service_protos.TExpression_TypedValue{
			TypedValue: &Ydb.TypedValue{
				Type: NewPrimitiveType(Ydb.Type_INT32),
				Value: &Ydb.Value{
					Value: &Ydb.Value_Int32Value{
						Int32Value: val,
					},
				},
			},
		},
	}
}

func NewUint64ValueExpression(val uint64) *api_service_protos.TExpression {
	return &api_service_protos.TExpression{
		Payload: &api_service_protos.TExpression_TypedValue{
			TypedValue: &Ydb.TypedValue{
				Type: NewPrimitiveType(Ydb.Type_UINT64),
				Value: &Ydb.Value{
					Value: &Ydb.Value_Uint64Value{
						Uint64Value: val,
					},
				},
			},
		},
	}
}

func NewTextValueExpression(val string) *api_service_protos.TExpression {
	return &api_service_protos.TExpression{
		Payload: &api_service_protos.TExpression_TypedValue{
			TypedValue: &Ydb.TypedValue{
				Type: NewPrimitiveType(Ydb.Type_UTF8),
				Value: &Ydb.Value{
					Value: &Ydb.Value_TextValue{
						TextValue: val,
					},
				},
			},
		},
	}
}

func MakeTestSplit() *api_service_protos.TSplit {
	return &api_service_protos.TSplit{
		Select: &api_service_protos.TSelect{
			DataSourceInstance: &api_common.TDataSourceInstance{},
			What:               NewDefaultWhat(),
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
