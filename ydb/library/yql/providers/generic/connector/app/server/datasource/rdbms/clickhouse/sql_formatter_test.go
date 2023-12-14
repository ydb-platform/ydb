package clickhouse

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	ydb "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/api/common"
	rdbms_utils "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
	api "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

func TestMakeDescribeTableQuery(t *testing.T) {
	logger := utils.NewTestLogger(t)
	formatter := NewSQLFormatter()
	request := &api.TDescribeTableRequest{Table: "table", DataSourceInstance: &common.TDataSourceInstance{Database: "db"}}

	output, args := rdbms_utils.MakeDescribeTableQuery(logger, formatter, request)
	require.Equal(t, "SELECT name, type FROM system.columns WHERE table = ? and database = ?", output)
	require.Equal(t, args, []any{"table", "db"})
}

func TestMakeSQLFormatterQuery(t *testing.T) {
	type testCase struct {
		testName    string
		selectReq   *api.TSelect
		outputQuery string
		outputArgs  []any
		err         error
	}

	logger := utils.NewTestLogger(t)
	formatter := NewSQLFormatter()

	tcs := []testCase{
		{
			testName: "empty_table_name",
			selectReq: &api.TSelect{
				From: &api.TSelect_TFrom{
					Table: "",
				},
				What: &api.TSelect_TWhat{},
			},
			outputQuery: "",
			outputArgs:  nil,
			err:         utils.ErrEmptyTableName,
		},
		{
			testName: "empty_no columns",
			selectReq: &api.TSelect{
				From: &api.TSelect_TFrom{
					Table: "tab",
				},
				What: &api.TSelect_TWhat{},
			},
			outputQuery: `SELECT 0 FROM "tab"`,
			outputArgs:  []any{},
			err:         nil,
		},
		{
			testName: "select_col",
			selectReq: &api.TSelect{
				From: &api.TSelect_TFrom{
					Table: "tab",
				},
				What: &api.TSelect_TWhat{
					Items: []*api.TSelect_TWhat_TItem{
						&api.TSelect_TWhat_TItem{
							Payload: &api.TSelect_TWhat_TItem_Column{
								Column: &ydb.Column{
									Name: "col",
									Type: utils.NewPrimitiveType(ydb.Type_INT32),
								},
							},
						},
					},
				},
			},
			outputQuery: `SELECT "col" FROM "tab"`,
			outputArgs:  []any{},
			err:         nil,
		},
		{
			testName: "is_null",
			selectReq: &api.TSelect{
				From: &api.TSelect_TFrom{
					Table: "tab",
				},
				What: utils.NewDefaultWhat(),
				Where: &api.TSelect_TWhere{
					FilterTyped: &api.TPredicate{
						Payload: &api.TPredicate_IsNull{
							IsNull: &api.TPredicate_TIsNull{
								Value: utils.NewColumnExpression("col1"),
							},
						},
					},
				},
			},
			outputQuery: `SELECT "col0", "col1" FROM "tab" WHERE ("col1" IS NULL)`,
			outputArgs:  []any{},
			err:         nil,
		},
		{
			testName: "is_not_null",
			selectReq: &api.TSelect{
				From: &api.TSelect_TFrom{
					Table: "tab",
				},
				What: utils.NewDefaultWhat(),
				Where: &api.TSelect_TWhere{
					FilterTyped: &api.TPredicate{
						Payload: &api.TPredicate_IsNotNull{
							IsNotNull: &api.TPredicate_TIsNotNull{
								Value: utils.NewColumnExpression("col2"),
							},
						},
					},
				},
			},
			outputQuery: `SELECT "col0", "col1" FROM "tab" WHERE ("col2" IS NOT NULL)`,
			outputArgs:  []any{},
			err:         nil,
		},
		{
			testName: "bool_column",
			selectReq: &api.TSelect{
				From: &api.TSelect_TFrom{
					Table: "tab",
				},
				What: utils.NewDefaultWhat(),
				Where: &api.TSelect_TWhere{
					FilterTyped: &api.TPredicate{
						Payload: &api.TPredicate_BoolExpression{
							BoolExpression: &api.TPredicate_TBoolExpression{
								Value: utils.NewColumnExpression("col2"),
							},
						},
					},
				},
			},
			outputQuery: `SELECT "col0", "col1" FROM "tab" WHERE "col2"`,
			outputArgs:  []any{},
			err:         nil,
		},
		{
			testName: "complex_filter",
			selectReq: &api.TSelect{
				From: &api.TSelect_TFrom{
					Table: "tab",
				},
				What: utils.NewDefaultWhat(),
				Where: &api.TSelect_TWhere{
					FilterTyped: &api.TPredicate{
						Payload: &api.TPredicate_Disjunction{
							Disjunction: &api.TPredicate_TDisjunction{
								Operands: []*api.TPredicate{
									&api.TPredicate{
										Payload: &api.TPredicate_Negation{
											Negation: &api.TPredicate_TNegation{
												Operand: &api.TPredicate{
													Payload: &api.TPredicate_Comparison{
														Comparison: &api.TPredicate_TComparison{
															Operation:  api.TPredicate_TComparison_LE,
															LeftValue:  utils.NewColumnExpression("col2"),
															RightValue: utils.NewInt32ValueExpression(42),
														},
													},
												},
											},
										},
									},
									&api.TPredicate{
										Payload: &api.TPredicate_Conjunction{
											Conjunction: &api.TPredicate_TConjunction{
												Operands: []*api.TPredicate{
													&api.TPredicate{
														Payload: &api.TPredicate_Comparison{
															Comparison: &api.TPredicate_TComparison{
																Operation:  api.TPredicate_TComparison_NE,
																LeftValue:  utils.NewColumnExpression("col1"),
																RightValue: utils.NewUint64ValueExpression(0),
															},
														},
													},
													&api.TPredicate{
														Payload: &api.TPredicate_IsNull{
															IsNull: &api.TPredicate_TIsNull{
																Value: utils.NewColumnExpression("col3"),
															},
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			outputQuery: `SELECT "col0", "col1" FROM "tab" WHERE ((NOT ("col2" <= ?)) OR (("col1" <> ?) AND ("col3" IS NULL)))`,
			outputArgs:  []any{int32(42), uint64(0)},
			err:         nil,
		},
		{
			testName: "unsupported_predicate",
			selectReq: &api.TSelect{
				From: &api.TSelect_TFrom{
					Table: "tab",
				},
				What: utils.NewDefaultWhat(),
				Where: &api.TSelect_TWhere{
					FilterTyped: &api.TPredicate{
						Payload: &api.TPredicate_Between{
							Between: &api.TPredicate_TBetween{
								Value:    utils.NewColumnExpression("col2"),
								Least:    utils.NewColumnExpression("col1"),
								Greatest: utils.NewColumnExpression("col3"),
							},
						},
					},
				},
			},
			outputQuery: `SELECT "col0", "col1" FROM "tab"`,
			outputArgs:  []any{},
			err:         nil,
		},
		{
			testName: "unsupported_type",
			selectReq: &api.TSelect{
				From: &api.TSelect_TFrom{
					Table: "tab",
				},
				What: utils.NewDefaultWhat(),
				Where: &api.TSelect_TWhere{
					FilterTyped: &api.TPredicate{
						Payload: &api.TPredicate_Comparison{
							Comparison: &api.TPredicate_TComparison{
								Operation:  api.TPredicate_TComparison_EQ,
								LeftValue:  utils.NewColumnExpression("col2"),
								RightValue: utils.NewTextValueExpression("text"),
							},
						},
					},
				},
			},
			outputQuery: `SELECT "col0", "col1" FROM "tab"`,
			outputArgs:  []any{},
			err:         nil,
		},
		{
			testName: "partial_filter_removes_and",
			selectReq: &api.TSelect{
				From: &api.TSelect_TFrom{
					Table: "tab",
				},
				What: utils.NewDefaultWhat(),
				Where: &api.TSelect_TWhere{
					FilterTyped: &api.TPredicate{
						Payload: &api.TPredicate_Conjunction{
							Conjunction: &api.TPredicate_TConjunction{
								Operands: []*api.TPredicate{
									&api.TPredicate{
										Payload: &api.TPredicate_Comparison{
											Comparison: &api.TPredicate_TComparison{
												Operation:  api.TPredicate_TComparison_EQ,
												LeftValue:  utils.NewColumnExpression("col1"),
												RightValue: utils.NewInt32ValueExpression(32),
											},
										},
									},
									&api.TPredicate{
										// Not supported
										Payload: &api.TPredicate_Comparison{
											Comparison: &api.TPredicate_TComparison{
												Operation:  api.TPredicate_TComparison_EQ,
												LeftValue:  utils.NewColumnExpression("col2"),
												RightValue: utils.NewTextValueExpression("text"),
											},
										},
									},
								},
							},
						},
					},
				},
			},
			outputQuery: `SELECT "col0", "col1" FROM "tab" WHERE ("col1" = ?)`,
			outputArgs:  []any{int32(32)},
			err:         nil,
		},
		{
			testName: "partial_filter",
			selectReq: &api.TSelect{
				From: &api.TSelect_TFrom{
					Table: "tab",
				},
				What: utils.NewDefaultWhat(),
				Where: &api.TSelect_TWhere{
					FilterTyped: &api.TPredicate{
						Payload: &api.TPredicate_Conjunction{
							Conjunction: &api.TPredicate_TConjunction{
								Operands: []*api.TPredicate{
									&api.TPredicate{
										Payload: &api.TPredicate_Comparison{
											Comparison: &api.TPredicate_TComparison{
												Operation:  api.TPredicate_TComparison_EQ,
												LeftValue:  utils.NewColumnExpression("col1"),
												RightValue: utils.NewInt32ValueExpression(32),
											},
										},
									},
									&api.TPredicate{
										// Not supported
										Payload: &api.TPredicate_Comparison{
											Comparison: &api.TPredicate_TComparison{
												Operation:  api.TPredicate_TComparison_EQ,
												LeftValue:  utils.NewColumnExpression("col2"),
												RightValue: utils.NewTextValueExpression("text"),
											},
										},
									},
									&api.TPredicate{
										Payload: &api.TPredicate_IsNull{
											IsNull: &api.TPredicate_TIsNull{
												Value: utils.NewColumnExpression("col3"),
											},
										},
									},
									&api.TPredicate{
										Payload: &api.TPredicate_IsNotNull{
											IsNotNull: &api.TPredicate_TIsNotNull{
												Value: utils.NewColumnExpression("col4"),
											},
										},
									},
								},
							},
						},
					},
				},
			},
			outputQuery: `SELECT "col0", "col1" FROM "tab" WHERE (("col1" = ?) AND ("col3" IS NULL) AND ("col4" IS NOT NULL))`,
			outputArgs:  []any{int32(32)},
			err:         nil,
		},
		{
			testName: "negative_sql_injection_by_table",
			selectReq: &api.TSelect{
				From: &api.TSelect_TFrom{
					Table: `information_schema.columns; DROP TABLE information_schema.columns`,
				},
				What: &api.TSelect_TWhat{},
			},
			outputQuery: `SELECT 0 FROM "information_schema.columns; DROP TABLE information_schema.columns"`,
			outputArgs:  []any{},
			err:         nil,
		},
		{
			testName: "negative_sql_injection_by_col",
			selectReq: &api.TSelect{
				From: &api.TSelect_TFrom{
					Table: "tab",
				},
				What: &api.TSelect_TWhat{
					Items: []*api.TSelect_TWhat_TItem{
						&api.TSelect_TWhat_TItem{
							Payload: &api.TSelect_TWhat_TItem_Column{
								Column: &ydb.Column{
									Name: `0; DROP TABLE information_schema.columns`,
									Type: utils.NewPrimitiveType(ydb.Type_INT32),
								},
							},
						},
					},
				},
			},
			outputQuery: `SELECT "0; DROP TABLE information_schema.columns" FROM "tab"`,
			outputArgs:  []any{},
			err:         nil,
		},
		{
			testName: "negative_sql_injection_fake_quotes",
			selectReq: &api.TSelect{
				From: &api.TSelect_TFrom{
					Table: "tab",
				},
				What: &api.TSelect_TWhat{
					Items: []*api.TSelect_TWhat_TItem{
						&api.TSelect_TWhat_TItem{
							Payload: &api.TSelect_TWhat_TItem_Column{
								Column: &ydb.Column{
									Name: `0"; DROP TABLE information_schema.columns;`,
									Type: utils.NewPrimitiveType(ydb.Type_INT32),
								},
							},
						},
					},
				},
			},
			outputQuery: `SELECT "0""; DROP TABLE information_schema.columns;" FROM "tab"`,
			outputArgs:  []any{},
			err:         nil,
		},
	}

	for _, tc := range tcs {
		tc := tc

		t.Run(tc.testName, func(t *testing.T) {
			outputQuery, outputArgs, err := rdbms_utils.MakeReadSplitQuery(logger, formatter, tc.selectReq)
			require.Equal(t, tc.outputQuery, outputQuery)
			require.Equal(t, tc.outputArgs, outputArgs)

			if tc.err != nil {
				require.True(t, errors.Is(err, tc.err))
			} else {
				require.NoError(t, err)
			}
		})
	}
}
