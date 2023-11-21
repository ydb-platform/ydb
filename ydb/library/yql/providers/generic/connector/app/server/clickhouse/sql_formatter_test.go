package clickhouse

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	ydb "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/api/common"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
	api "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

func TestMakeDescribeTableQuery(t *testing.T) {
	logger := utils.NewTestLogger(t)
	formatter := NewSQLFormatter()
	request := &api.TDescribeTableRequest{Table: "table", DataSourceInstance: &common.TDataSourceInstance{Database: "db"}}

	output, args := utils.MakeDescribeTableQuery(logger, formatter, request)
	require.Equal(t, "SELECT name, type FROM system.columns WHERE table = ? and database = ?", output)
	require.Equal(t, args, []any{"table", "db"})
}

func TestMakeSQLFormatterQuery(t *testing.T) {
	type testCase struct {
		testName  string
		selectReq *api.TSelect
		output    string
		err       error
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
			output: "",
			err:    utils.ErrEmptyTableName,
		},
		{
			testName: "empty_no columns",
			selectReq: &api.TSelect{
				From: &api.TSelect_TFrom{
					Table: "tab",
				},
				What: &api.TSelect_TWhat{},
			},
			output: `SELECT 0 FROM "tab"`,
			err:    nil,
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
			output: `SELECT "col" FROM "tab"`,
			err:    nil,
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
			output: `SELECT "col0", "col1" FROM "tab" WHERE ("col1" IS NULL)`,
			err:    nil,
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
			output: `SELECT "col0", "col1" FROM "tab" WHERE ("col2" IS NOT NULL)`,
			err:    nil,
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
			output: `SELECT "col0", "col1" FROM "tab" WHERE "col2"`,
			err:    nil,
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
			output: `SELECT "col0", "col1" FROM "tab" WHERE ((NOT ("col2" <= 42)) OR (("col1" <> 0) AND ("col3" IS NULL)))`,
			err:    nil,
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
			output: `SELECT "col0", "col1" FROM "tab"`,
			err:    nil,
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
			output: `SELECT "col0", "col1" FROM "tab"`,
			err:    nil,
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
			output: `SELECT "col0", "col1" FROM "tab" WHERE ("col1" = 32)`,
			err:    nil,
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
			output: `SELECT "col0", "col1" FROM "tab" WHERE (("col1" = 32) AND ("col3" IS NULL) AND ("col4" IS NOT NULL))`,
			err:    nil,
		},
		{
			testName: "negative_sql_injection_by_table",
			selectReq: &api.TSelect{
				From: &api.TSelect_TFrom{
					Table: `system.columns; DROP DATABASE system`,
				},
				What: &api.TSelect_TWhat{},
			},
			output: `SELECT 0 FROM "system.columns; DROP DATABASE system"`,
			err:    nil,
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
									Name: `0; DROP DATABASE system`,
									Type: utils.NewPrimitiveType(ydb.Type_INT32),
								},
							},
						},
					},
				},
			},
			output: `SELECT "0; DROP DATABASE system" FROM "tab"`,
			err:    nil,
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
									Name: `0"; DROP DATABASE system;`,
									Type: utils.NewPrimitiveType(ydb.Type_INT32),
								},
							},
						},
					},
				},
			},
			output: `SELECT "0""; DROP DATABASE system;" FROM "tab"`,
			err:    nil,
		},
	}

	for _, tc := range tcs {
		tc := tc

		t.Run(tc.testName, func(t *testing.T) {
			output, _, err := utils.MakeReadSplitQuery(logger, formatter, tc.selectReq)
			require.Equal(t, tc.output, output)

			if tc.err != nil {
				require.True(t, errors.Is(err, tc.err))
			} else {
				require.NoError(t, err)
			}
		})
	}
}
