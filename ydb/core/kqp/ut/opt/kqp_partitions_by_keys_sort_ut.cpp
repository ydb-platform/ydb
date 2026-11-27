#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <util/folder/dirut.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

namespace {

void CheckWindowFunctionAst(const TString& selectBody, bool useSortForPartitionsByKeys) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableWindowFunctionsV2(useSortForPartitionsByKeys);
    TKikimrRunner kikimr(appConfig);
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    TStringBuilder query;
    query << "--!syntax_v1\n"
          << "PRAGMA ydb.HashJoinMode = 'grace';\n"
          << selectBody;

    auto result = session.ExplainDataQuery(query).GetValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    const TString ast{result.GetAst()};

    Cerr << "=== Explain AST, UseSortForPartitionsByKeys="
         << (useSortForPartitionsByKeys ? "true" : "false")
         << " ===\n" << ast << Endl;

    if (useSortForPartitionsByKeys) {
        UNIT_ASSERT_C(ast.Contains("WideSort"), ast);
        UNIT_ASSERT_C(ast.Contains("Chopper"), ast);
        UNIT_ASSERT_C(ast.Contains("HashShuffle"), ast);
        UNIT_ASSERT_C(!ast.Contains("SqueezeToDict"), ast);
        UNIT_ASSERT_C(!ast.Contains("SqueezeToList"), ast);
        UNIT_ASSERT_C(!ast.Contains("(Sort "), ast);
    } else {
        UNIT_ASSERT_C(ast.Contains("SqueezeToDict"), ast);
    }
}

void CheckFullFrameSumPlan(const TString& selectBody, bool windowFunctionsV2, bool fromJoin = false) {
    NKikimrConfig::TAppConfig appConfig;
    auto* tableServiceConfig = appConfig.MutableTableServiceConfig();
    tableServiceConfig->SetEnableWindowFunctionsV2(windowFunctionsV2);
    if (fromJoin) {
        tableServiceConfig->SetEnableQueryServiceSpilling(true);
        auto* localFileConfig = tableServiceConfig->MutableSpillingServiceConfig()->MutableLocalFileConfig();
        localFileConfig->SetEnable(true);
        localFileConfig->SetRoot("./spilling/");
        MakeDirIfNotExist("./spilling");
    }

    TKikimrRunner kikimr(appConfig);
    auto db = kikimr.GetQueryClient();
    auto session = db.GetSession().GetValueSync().GetSession();
    const TString query = TStringBuilder() << "--!syntax_v1\n"
        << "PRAGMA ydb.HashJoinMode = 'grace';\n"
        << selectBody;

    auto explain = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
        NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain)).GetValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(explain.GetStatus(), EStatus::SUCCESS, explain.GetIssues().ToString());
    const TString ast = *explain.GetStats()->GetAst();

    Cerr << "=== Full-frame SUM AST, WindowFunctionsV2="
         << (windowFunctionsV2 ? "true" : "false")
         << ", fromJoin=" << (fromJoin ? "true" : "false")
         << " ===\n" << ast << Endl;

    if (windowFunctionsV2) {
        UNIT_ASSERT_C(!ast.Contains("WinFramesCollector"), ast);
        UNIT_ASSERT_C(ast.Contains("WideSort"), ast);
        UNIT_ASSERT_C(!ast.Contains("SqueezeToList"), ast);
        UNIT_ASSERT_C(ast.Contains("WideCondense1") || ast.Contains("Condense1"), ast);
        UNIT_ASSERT_C(ast.Contains("MapJoin") || ast.Contains("EquiJoin")
            || ast.Contains("GraceJoin") || ast.Contains("BlockHashJoin"), ast);
        if (fromJoin) {
            UNIT_ASSERT_C(ast.Contains("DqReplicate") || ast.Contains("Switch") || ast.Contains("MultiMap"), ast);
            ui32 streamLookups = 0;
            for (size_t pos = 0; (pos = ast.find("KqpCnStreamLookup", pos)) != TString::npos; ++pos) {
                ++streamLookups;
            }
            UNIT_ASSERT_VALUES_EQUAL_C(streamLookups, 1, ast);
        }
    } else {
        UNIT_ASSERT_C(ast.Contains("WinFramesCollector"), ast);
    }

    auto exec = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(exec.GetStatus(), EStatus::SUCCESS, exec.GetIssues().ToString());
    CompareYsonUnordered(R"([
        [[101u];["Value1"];[1];[15]];
        [[201u];["Value1"];[2];[15]];
        [[301u];["Value1"];[3];[15]];
        [[401u];["Value1"];[1];[15]];
        [[501u];["Value1"];[2];[15]];
        [[601u];["Value1"];[3];[15]];
        [[701u];["Value1"];[1];[15]];
        [[801u];["Value1"];[2];[15]];
        [[102u];["Value2"];[3];[16]];
        [[202u];["Value2"];[1];[16]];
        [[302u];["Value2"];[2];[16]];
        [[402u];["Value2"];[3];[16]];
        [[502u];["Value2"];[1];[16]];
        [[602u];["Value2"];[2];[16]];
        [[702u];["Value2"];[3];[16]];
        [[802u];["Value2"];[1];[16]];
        [[103u];["Value3"];[2];[17]];
        [[203u];["Value3"];[3];[17]];
        [[303u];["Value3"];[1];[17]];
        [[403u];["Value3"];[2];[17]];
        [[503u];["Value3"];[3];[17]];
        [[603u];["Value3"];[1];[17]];
        [[703u];["Value3"];[2];[17]];
        [[803u];["Value3"];[3];[17]]
    ])", FormatResultSetYson(exec.GetResultSet(0)));
}

TString BalancedFrom(const TString& ast, size_t open) {
    if (open >= ast.size() || ast[open] != '(') {
        return {};
    }
    int depth = 0;
    for (size_t i = open; i < ast.size(); ++i) {
        if (ast[i] == '(') {
            ++depth;
        } else if (ast[i] == ')') {
            if (--depth == 0) {
                return ast.substr(open, i + 1 - open);
            }
        }
    }
    return {};
}

// The aggregate ExpandMap lambda may be bound outside the stage. Follow that binding.
TString AggregateInputLambda(const TString& ast, const TString& stage) {
    const auto expand = stage.find("ExpandMap");
    if (expand == TString::npos) {
        return {};
    }
    const auto first = stage.find('$', expand);
    const auto second = first == TString::npos ? TString::npos : stage.find('$', first + 1);
    if (second == TString::npos) {
        return {};
    }
    size_t end = second + 1;
    while (end < stage.size() && stage[end] >= '0' && stage[end] <= '9') {
        ++end;
    }
    const auto name = stage.substr(second, end - second);
    const auto letPos = ast.find("(let " + name + " (lambda");
    if (letPos == TString::npos) {
        return {};
    }
    return BalancedFrom(ast, letPos);
}

TString CallableContaining(const TString& ast, TStringBuf marker, TStringBuf callable) {
    const auto markerPos = ast.find(marker);
    if (markerPos == TString::npos) {
        return {};
    }
    const auto namePos = ast.rfind(callable, markerPos);
    if (namePos == TString::npos) {
        return {};
    }
    const auto open = ast.rfind('(', namePos);
    if (open == TString::npos) {
        return {};
    }

    int depth = 0;
    for (size_t i = open; i < ast.size(); ++i) {
        if (ast[i] == '(') {
            ++depth;
        } else if (ast[i] == ')') {
            if (--depth == 0) {
                return ast.substr(open, i + 1 - open);
            }
        }
    }
    return {};
}

TString FullFrameSumWithFatColumnExpected() {
    struct TRow {
        ui64 Key;
        TStringBuf Text;
        i32 Data;
        i32 Total;
    };
    const TRow rows[] = {
        {101, "Value1", 1, 15}, {201, "Value1", 2, 15}, {301, "Value1", 3, 15}, {401, "Value1", 1, 15},
        {501, "Value1", 2, 15}, {601, "Value1", 3, 15}, {701, "Value1", 1, 15}, {801, "Value1", 2, 15},
        {102, "Value2", 3, 16}, {202, "Value2", 1, 16}, {302, "Value2", 2, 16}, {402, "Value2", 3, 16},
        {502, "Value2", 1, 16}, {602, "Value2", 2, 16}, {702, "Value2", 3, 16}, {802, "Value2", 1, 16},
        {103, "Value3", 2, 17}, {203, "Value3", 3, 17}, {303, "Value3", 1, 17}, {403, "Value3", 2, 17},
        {503, "Value3", 3, 17}, {603, "Value3", 1, 17}, {703, "Value3", 2, 17}, {803, "Value3", 3, 17},
    };

    TStringBuilder expected;
    expected << "[";
    bool first = true;
    for (const auto& row : rows) {
        if (!first) {
            expected << ";";
        }
        first = false;
        // Unwrap makes unused_fat_col a non-optional string, so its YSON cell is not a list.
        expected << "[[" << row.Key << "u];[\"" << row.Text << "\"];[" << row.Data
            << "];\"" << row.Key << row.Text << row.Data << "\";[" << row.Total << "]]";
    }
    expected << "]";
    return expected;
}

const char* FullFrameSumWithFatColumnQuery(bool ordered) {
    if (ordered) {
        return R"(
            $input = SELECT Key, Text, Data,
                Unwrap(CAST(Key AS String) || Text || CAST(Data AS String)) AS unused_fat_col
            FROM `/Root/EightShard`;
            SELECT Key, Text, Data, unused_fat_col,
                SUM(Data) OVER (
                    PARTITION BY Text
                    ORDER BY Key
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS total
            FROM $input;
        )";
    }
    return R"(
        $input = SELECT Key, Text, Data,
            Unwrap(CAST(Key AS String) || Text || CAST(Data AS String)) AS unused_fat_col
        FROM `/Root/EightShard`;
        SELECT Key, Text, Data, unused_fat_col,
            SUM(Data) OVER (PARTITION BY Text) AS total
        FROM $input;
    )";
}

void CheckFullFrameDropsUnusedColumn(bool narrow, bool ordered) {
    NKikimrConfig::TAppConfig appConfig;
    auto* tableServiceConfig = appConfig.MutableTableServiceConfig();
    tableServiceConfig->SetEnableWindowFunctionsV2(true);
    tableServiceConfig->SetEnableNarrowWindowAggregateInput(narrow);

    TKikimrRunner kikimr(appConfig);
    auto db = kikimr.GetQueryClient();
    auto session = db.GetSession().GetValueSync().GetSession();
    const TString query = TStringBuilder() << "--!syntax_v1\n"
        << "PRAGMA ydb.HashJoinMode = 'grace';\n"
        << FullFrameSumWithFatColumnQuery(ordered);

    auto explain = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
        NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain)).GetValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(explain.GetStatus(), EStatus::SUCCESS, explain.GetIssues().ToString());
    const TString ast = *explain.GetStats()->GetAst();
    Cerr << "=== Full-frame fat column AST, narrow=" << narrow << ", ordered=" << ordered
        << " ===\n" << ast << Endl;

    UNIT_ASSERT_C(ast.Contains("unused_fat_col"), ast);
    UNIT_ASSERT_C(ast.Contains("Condense1") || ast.Contains("WideCondense1"), ast);
    const auto aggregateStage = CallableContaining(ast, "AggrAdd", "DqPhyStage");
    UNIT_ASSERT_C(aggregateStage, ast);
    const auto inputLambda = AggregateInputLambda(ast, aggregateStage);
    UNIT_ASSERT_C(inputLambda, aggregateStage);
    if (narrow) {
        UNIT_ASSERT_C(!aggregateStage.Contains("unused_fat_col"), aggregateStage);
        UNIT_ASSERT_C(inputLambda.Contains("\"Data\""), inputLambda);
        UNIT_ASSERT_C(!inputLambda.Contains("unused_fat_col"), inputLambda);
    } else {
        UNIT_ASSERT_C(inputLambda.Contains("unused_fat_col"), inputLambda);
    }

    auto exec = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(exec.GetStatus(), EStatus::SUCCESS, exec.GetIssues().ToString());
    CompareYsonUnordered(FullFrameSumWithFatColumnExpected(), FormatResultSetYson(exec.GetResultSet(0)));
}

void CheckStandardWindowFunctionAst(const TString& projection, bool useSortForPartitionsByKeys) {
    CheckWindowFunctionAst(
        TStringBuilder()
            << "SELECT Key, Text, Data,\n"
            << "    " << projection << "\n"
            << "FROM `/Root/EightShard`\n"
            << "WINDOW w AS (\n"
            << "    PARTITION BY Text\n"
            << "    ORDER BY Key\n"
            << ");\n",
        useSortForPartitionsByKeys);
}

} // namespace

Y_UNIT_TEST_SUITE(KqpPartitionsByKeysSort) {

    Y_UNIT_TEST_TWIN(WindowFunctionAst, UseSortForPartitionsByKeys) {
        CheckWindowFunctionAst(
            "SELECT Key, Text,\n"
            "    row_number() OVER (PARTITION BY Text ORDER BY Key) AS rn\n"
            "FROM `/Root/EightShard`\n"
            "WHERE Text = 'Value2';\n",
            UseSortForPartitionsByKeys);
    }

    Y_UNIT_TEST_TWIN(WindowFunctionMultiSortKeyAst, UseSortForPartitionsByKeys) {
        CheckWindowFunctionAst(
            "SELECT Key, Text, Data,\n"
            "    row_number() OVER (PARTITION BY Text ORDER BY Data, Key) AS rn\n"
            "FROM `/Root/EightShard`;\n",
            UseSortForPartitionsByKeys);
    }

    Y_UNIT_TEST_TWIN(WindowFunctionExpressionSortKeyAst, UseSortForPartitionsByKeys) {
        CheckWindowFunctionAst(
            "SELECT Key, Text, Data,\n"
            "    row_number() OVER (PARTITION BY Text ORDER BY Key + 1) AS rn\n"
            "FROM `/Root/EightShard`;\n",
            UseSortForPartitionsByKeys);
    }

    Y_UNIT_TEST_TWIN(WindowFunctionMixedDirectionsAst, UseSortForPartitionsByKeys) {
        CheckWindowFunctionAst(
            "SELECT Key, Text, Data,\n"
            "    row_number() OVER (PARTITION BY Text ORDER BY Data DESC, Key) AS rn\n"
            "FROM `/Root/EightShard`;\n",
            UseSortForPartitionsByKeys);
    }

    Y_UNIT_TEST_TWIN(WindowFunctionRepeatedKeyAst, UseSortForPartitionsByKeys) {
        CheckWindowFunctionAst(
            "SELECT Key, Text, Data,\n"
            "    row_number() OVER (PARTITION BY Text ORDER BY Text DESC, Key) AS rn\n"
            "FROM `/Root/EightShard`;\n",
            UseSortForPartitionsByKeys);
    }

    Y_UNIT_TEST_TWIN(WindowFunctionLagMultiPartitionKeyAst, UseSortForPartitionsByKeys) {
        CheckWindowFunctionAst(
            "SELECT Key, Text, Data,\n"
            "    LAG(Data) OVER w AS prev_data\n"
            "FROM `/Root/EightShard`\n"
            "WINDOW w AS (\n"
            "    PARTITION BY Key, Text\n"
            "    ORDER BY Data\n"
            ");\n",
            UseSortForPartitionsByKeys);
    }

    Y_UNIT_TEST_TWIN(WindowFunctionAggregateSumAst, UseSortForPartitionsByKeys) {
        CheckWindowFunctionAst(
            "SELECT Key, Text, Data,\n"
            "    SUM(Data) OVER w AS running_total\n"
            "FROM `/Root/EightShard`\n"
            "WINDOW w AS (\n"
            "    PARTITION BY Text\n"
            "    ORDER BY Key\n"
            "    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n"
            ");\n",
            UseSortForPartitionsByKeys);
    }

    Y_UNIT_TEST_TWIN(WindowFunctionLeadAst, UseSortForPartitionsByKeys) {
        CheckStandardWindowFunctionAst("LEAD(Data) OVER w AS next_data", UseSortForPartitionsByKeys);
    }

    Y_UNIT_TEST_TWIN(WindowFunctionFirstValueAst, UseSortForPartitionsByKeys) {
        CheckStandardWindowFunctionAst("FIRST_VALUE(Data) OVER w AS first_data", UseSortForPartitionsByKeys);
    }

    Y_UNIT_TEST_TWIN(WindowFunctionLastValueAst, UseSortForPartitionsByKeys) {
        CheckStandardWindowFunctionAst("LAST_VALUE(Data) IGNORE NULLS OVER w AS last_data", UseSortForPartitionsByKeys);
    }

    Y_UNIT_TEST_TWIN(WindowFunctionNthValueAst, UseSortForPartitionsByKeys) {
        CheckStandardWindowFunctionAst("NTH_VALUE(Data, 2) OVER w AS second_data", UseSortForPartitionsByKeys);
    }

    Y_UNIT_TEST_TWIN(WindowFunctionRankAst, UseSortForPartitionsByKeys) {
        CheckStandardWindowFunctionAst("RANK() OVER w AS r", UseSortForPartitionsByKeys);
    }

    Y_UNIT_TEST_TWIN(WindowFunctionDenseRankAst, UseSortForPartitionsByKeys) {
        CheckStandardWindowFunctionAst("DENSE_RANK() OVER w AS dr", UseSortForPartitionsByKeys);
    }

    Y_UNIT_TEST_TWIN(WindowFunctionPercentRankAst, UseSortForPartitionsByKeys) {
        CheckStandardWindowFunctionAst("PERCENT_RANK() OVER w AS pr", UseSortForPartitionsByKeys);
    }

    Y_UNIT_TEST_TWIN(WindowFunctionNtileAst, UseSortForPartitionsByKeys) {
        CheckStandardWindowFunctionAst("NTILE(10) OVER w AS group_num", UseSortForPartitionsByKeys);
    }

    Y_UNIT_TEST_TWIN(WindowFunctionCumeDistAst, UseSortForPartitionsByKeys) {
        CheckStandardWindowFunctionAst("CUME_DIST() OVER w AS dist", UseSortForPartitionsByKeys);
    }

    Y_UNIT_TEST_TWIN(WindowFunctionFullFrameAggregateAst, WindowFunctionsV2) {
        CheckFullFrameSumPlan(
            "SELECT Key, Text, Data,\n"
            "    SUM(Data) OVER (PARTITION BY Text) AS tot\n"
            "FROM `/Root/EightShard`;\n",
            WindowFunctionsV2);
    }

    Y_UNIT_TEST(WindowFunctionFullFrameAggregateDropsUnusedColumn) {
        CheckFullFrameDropsUnusedColumn(true, false);
    }

    Y_UNIT_TEST(WindowFunctionFullFrameAggregateKeepsUnusedColumn) {
        CheckFullFrameDropsUnusedColumn(false, false);
    }

    Y_UNIT_TEST(WindowFunctionFullFrameOrderedAggregateDropsUnusedColumn) {
        CheckFullFrameDropsUnusedColumn(true, true);
    }

    Y_UNIT_TEST(WindowFunctionFullFrameFromJoinAst) {
        CheckFullFrameSumPlan(
            "SELECT a.Key, a.Text, a.Data, SUM(a.Data) OVER (PARTITION BY a.Text) AS tot\n"
            "FROM `/Root/EightShard` AS a\n"
            "INNER JOIN `/Root/EightShard` AS b ON a.Key = b.Key;\n",
            true,
            true);
    }
}

} // namespace NKikimr::NKqp
