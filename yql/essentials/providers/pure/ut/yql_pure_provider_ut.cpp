#include "yql_pure_provider.h"

#include <library/cpp/testing/unittest/registar.h>
#include <yql/essentials/core/facade/yql_facade.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/public/result_format/yql_result_format_response.h>

#include <library/cpp/yson/node/node_io.h>

#include <util/system/user.h>
#include <util/string/strip.h>

namespace NYql {

namespace {

struct TSettings {
    bool SExpr = false;
    bool Pretty = false;
};

TString Run(const TString& query, TSettings settings = {}, TString* statistics = nullptr) {
    auto functionRegistry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
    TVector<TDataProviderInitializer> dataProvidersInit;
    dataProvidersInit.push_back(GetPureDataProviderInitializer());
    TProgramFactory factory(/*useRepeatableRandomAndTimeProviders=*/true, functionRegistry.Get(), 0ULL, dataProvidersInit, "ut");
    TProgramPtr program = factory.Create("-stdin-", query);
    program->ConfigureYsonResultFormat(settings.Pretty ? NYson::EYsonFormat::Pretty : NYson::EYsonFormat::Text);
    bool parseRes;
    if (settings.SExpr) {
        parseRes = program->ParseYql();
    } else {
        parseRes = program->ParseSql();
    }

    if (!parseRes) {
        TStringStream err;
        program->PrintErrorsTo(err);
        UNIT_FAIL(err.Str());
    }

    if (!program->Compile(GetUsername())) {
        TStringStream err;
        program->PrintErrorsTo(err);
        UNIT_FAIL(err.Str());
    }

    TProgram::TStatus status = program->Run(GetUsername());
    if (status == TProgram::TStatus::Error) {
        TStringStream err;
        program->PrintErrorsTo(err);
        UNIT_FAIL(err.Str());
    }

    if (statistics) {
        auto stats = program->GetStatistics(/*totalOnly=*/true);
        UNIT_ASSERT(stats);
        *statistics = *stats;
    }

    return program->ResultsAsString();
}

} // namespace

Y_UNIT_TEST_SUITE(TPureProviderTests) {
Y_UNIT_TEST(SExpr) {
    const auto s = R"(
            (
            (let result_sink (DataSink 'result))
            (let output (Int32 '1))
            (let world (Write! world result_sink (Key) output '('('type))))
            (return (Commit! world result_sink))
            )

            )";
    const auto expectedRes = R"(
[
    {
        "Write" = [
            {
                "Type" = [
                    "DataType";
                    "Int32"
                ];
                "Data" = "1"
            }
        ]
    }
]
        )";
    auto res = Run(s, TSettings{.SExpr = true, .Pretty = true});
    UNIT_ASSERT_NO_DIFF(res, Strip(expectedRes));
}

Y_UNIT_TEST(Sql0Rows) {
    const auto s = "select * from (select 1 as x) limit 0";
    const auto expectedRes = R"(
[
    {
        "Write" = [
            {
                "Type" = [
                    "ListType";
                    [
                        "StructType";
                        [
                            [
                                "x";
                                [
                                    "DataType";
                                    "Int32"
                                ]
                            ]
                        ]
                    ]
                ];
                "Data" = []
            }
        ]
    }
]
        )";
    auto res = Run(s, TSettings{.Pretty = true});
    UNIT_ASSERT_NO_DIFF(res, Strip(expectedRes));
}

void Sql1RowImpl(const TString& query) {
    const auto expectedRes = R"(
[
    {
        "Write" = [
            {
                "Type" = [
                    "ListType";
                    [
                        "StructType";
                        [
                            [
                                "x";
                                [
                                    "DataType";
                                    "Int32"
                                ]
                            ]
                        ]
                    ]
                ];
                "Data" = [
                    [
                        "1"
                    ]
                ]
            }
        ]
    }
]
        )";
    auto res = Run(query, TSettings{.Pretty = true});
    UNIT_ASSERT_NO_DIFF(res, Strip(expectedRes));
}

Y_UNIT_TEST(Sql1Row_LLVM_On) {
    const auto s = R"(pragma config.flags("LLVM","--dump-stats");select 1 as x)";
    Sql1RowImpl(s);
}

Y_UNIT_TEST(Sql1Row_LLVM_Off) {
    const auto s = R"(pragma config.flags("LLVM","OFF");select 1 as x)";
    Sql1RowImpl(s);
}

Y_UNIT_TEST(Sql2Rows) {
    const auto s = "select 1 as x union all select 2 as x order by x";
    const auto expectedRes = R"(
[
    {
        "Write" = [
            {
                "Type" = [
                    "ListType";
                    [
                        "StructType";
                        [
                            [
                                "x";
                                [
                                    "DataType";
                                    "Int32"
                                ]
                            ]
                        ]
                    ]
                ];
                "Data" = [
                    [
                        "1"
                    ];
                    [
                        "2"
                    ]
                ]
            }
        ]
    }
]
        )";
    auto res = Run(s, TSettings{.Pretty = true});
    UNIT_ASSERT_NO_DIFF(res, Strip(expectedRes));
}

Y_UNIT_TEST(EvaluateExprStatistics) {
    const auto cacheEnabledQuery = R"sql(
        PRAGMA EvaluateExprCache;

        $v1 = EvaluateExpr(10 + 20);
        $v2 = EvaluateExpr(10 + 20);

        SELECT AsList($v1, $v2);
    )sql";

    TString statistics;
    Run(cacheEnabledQuery, {}, &statistics);

    auto statisticsNode = NYT::NodeFromYsonString(statistics);
    auto evaluation = statisticsNode["ExecutionStatistics"]["Evaluation"];
    UNIT_ASSERT_VALUES_EQUAL(evaluation["Count"]["count"].AsInt64(), 2);
    UNIT_ASSERT_VALUES_EQUAL(evaluation["CacheHits"]["count"].AsInt64(), 1);
    // Calc provider may finish within one microsecond, so its duration can be zero.
    UNIT_ASSERT_VALUES_EQUAL(evaluation["CalcProviderCalls"]["count"].AsInt64(), 1);
    UNIT_ASSERT(evaluation["CalcProviderDurationUs"].HasKey("sum"));

    const auto cacheDisabledByDefaultQuery = R"sql(
        $v1 = EvaluateExpr(10 + 20);
        $v2 = EvaluateExpr(10 + 20);

        SELECT AsList($v1, $v2);
    )sql";

    Run(cacheDisabledByDefaultQuery, {}, &statistics);

    statisticsNode = NYT::NodeFromYsonString(statistics);
    evaluation = statisticsNode["ExecutionStatistics"]["Evaluation"];
    UNIT_ASSERT_VALUES_EQUAL(evaluation["Count"]["count"].AsInt64(), 2);
    UNIT_ASSERT_VALUES_EQUAL(evaluation["CacheHits"]["count"].AsInt64(), 0);
    UNIT_ASSERT_VALUES_EQUAL(evaluation["CalcProviderCalls"]["count"].AsInt64(), 2);
    UNIT_ASSERT(evaluation["CalcProviderDurationUs"].HasKey("sum"));
}

Y_UNIT_TEST(EvaluateExprCacheSurvivesTransformCalls) {
    // EvaluateCode adds the second EvaluateExpr after the current transform call.
    const auto query = R"sql(
        PRAGMA EvaluateExprCache;

        SELECT EvaluateExpr(10 + 20);
        SELECT EvaluateCode(FuncCode("EvaluateExpr", QuoteCode(10 + 20)));
    )sql";

    TString statistics;
    Run(query, {}, &statistics);

    const auto statisticsNode = NYT::NodeFromYsonString(statistics);
    const auto evaluation = statisticsNode["ExecutionStatistics"]["Evaluation"];
    UNIT_ASSERT_VALUES_EQUAL(evaluation["Count"]["count"].AsInt64(), 3);
    UNIT_ASSERT_VALUES_EQUAL(evaluation["CacheHits"]["count"].AsInt64(), 1);
}

Y_UNIT_TEST(InnerEvaluateExprUsesSharedCache) {
    // The inner type annotation turns EvaluateExprIfPure into EvaluateExpr.
    const auto query = R"sql(
        PRAGMA EvaluateExprCache;

        SELECT EvaluateCode(
            FuncCode("EvaluateExprIfPure", QuoteCode(10 + 20)));
        SELECT EvaluateExpr(
            EvaluateCode(FuncCode("EvaluateExprIfPure", QuoteCode(10 + 20))));
    )sql";

    TString statistics;
    Run(query, {}, &statistics);

    const auto statisticsNode = NYT::NodeFromYsonString(statistics);
    const auto evaluation = statisticsNode["ExecutionStatistics"]["Evaluation"];
    UNIT_ASSERT_VALUES_EQUAL(evaluation["Count"]["count"].AsInt64(), 5);
    UNIT_ASSERT_VALUES_EQUAL(evaluation["CacheHits"]["count"].AsInt64(), 2);
}

Y_UNIT_TEST(TruncateRows) {
    const auto s = "select x from (select ListFromRange(1,2000) as x) flatten by x";
    auto res = Run(s);
    auto respList = NResult::ParseResponse(NYT::NodeFromYsonString(res));
    UNIT_ASSERT_VALUES_EQUAL(respList.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(respList[0].Writes.size(), 1);
    UNIT_ASSERT(respList[0].Writes[0].IsTruncated);
}

Y_UNIT_TEST(TruncateBytes) {
    const auto s = "select '" + TString(1000000, 'a') + "' as x, 1 as y union all select '' as x, 2 as y order by y";
    auto res = Run(s);
    auto respList = NResult::ParseResponse(NYT::NodeFromYsonString(res));
    UNIT_ASSERT_VALUES_EQUAL(respList.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(respList[0].Writes.size(), 1);
    UNIT_ASSERT(respList[0].Writes[0].IsTruncated);
}

Y_UNIT_TEST(ColumnOrder) {
    const auto s = "pragma OrderedColumns;select 1 as y, 2 as x";
    const auto expectedRes = R"(
[
    {
        "Write" = [
            {
                "Type" = [
                    "ListType";
                    [
                        "StructType";
                        [
                            [
                                "y";
                                [
                                    "DataType";
                                    "Int32"
                                ]
                            ];
                            [
                                "x";
                                [
                                    "DataType";
                                    "Int32"
                                ]
                            ]
                        ]
                    ]
                ];
                "Data" = [
                    [
                        "1";
                        "2"
                    ]
                ]
            }
        ]
    }
]
        )";
    auto res = Run(s, TSettings{.Pretty = true});
    UNIT_ASSERT_NO_DIFF(res, Strip(expectedRes));
}
} // Y_UNIT_TEST_SUITE(TPureProviderTests)

} // namespace NYql
