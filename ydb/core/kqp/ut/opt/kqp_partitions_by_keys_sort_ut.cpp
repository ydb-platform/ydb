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
