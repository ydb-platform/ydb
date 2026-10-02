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

    Y_UNIT_TEST(WindowFunctionFullFrameAggregateInputDropsUnusedColumn) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableWindowFunctionsV2(true);
        TKikimrRunner kikimr(appConfig);
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        const TString query =
            "--!syntax_v1\n"
            "$input = SELECT Key, Text, Data,\n"
            "    Unwrap(CAST(Key AS String) || Text || CAST(Data AS String)) AS unused_fat_col\n"
            "FROM `/Root/EightShard`;\n"
            "SELECT Key, Text, Data, unused_fat_col,\n"
            "    SUM(Data) OVER (PARTITION BY Text) AS total\n"
            "FROM $input;\n";

        auto explain = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain)).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(explain.GetStatus(), EStatus::SUCCESS, explain.GetIssues().ToString());
        const TString ast = *explain.GetStats()->GetAst();

        UNIT_ASSERT_C(ast.Contains("\"unused_fat_col\""), ast);
        TString sumStage;
        for (size_t pos = 0; (pos = ast.find("(DqPhyStage", pos)) != TString::npos; ++pos) {
            int depth = 0;
            size_t end = pos;
            for (; end < ast.size(); ++end) {
                if (ast[end] == '(') {
                    ++depth;
                } else if (ast[end] == ')' && --depth == 0) {
                    ++end;
                    break;
                }
            }
            const auto stage = ast.substr(pos, end - pos);
            if (stage.Contains("AggrAdd")) {
                sumStage = stage;
                break;
            }
        }
        UNIT_ASSERT_C(!sumStage.empty(), ast);
        UNIT_ASSERT_C(sumStage.Contains("\"Data\""), sumStage);
        UNIT_ASSERT_C(!sumStage.Contains("unused_fat_col"), sumStage);

        auto exec = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(exec.GetStatus(), EStatus::SUCCESS, exec.GetIssues().ToString());
        CompareYsonUnordered(R"([
            [[101u];["Value1"];[1];"101Value11";[15]];
            [[201u];["Value1"];[2];"201Value12";[15]];
            [[301u];["Value1"];[3];"301Value13";[15]];
            [[401u];["Value1"];[1];"401Value11";[15]];
            [[501u];["Value1"];[2];"501Value12";[15]];
            [[601u];["Value1"];[3];"601Value13";[15]];
            [[701u];["Value1"];[1];"701Value11";[15]];
            [[801u];["Value1"];[2];"801Value12";[15]];
            [[102u];["Value2"];[3];"102Value23";[16]];
            [[202u];["Value2"];[1];"202Value21";[16]];
            [[302u];["Value2"];[2];"302Value22";[16]];
            [[402u];["Value2"];[3];"402Value23";[16]];
            [[502u];["Value2"];[1];"502Value21";[16]];
            [[602u];["Value2"];[2];"602Value22";[16]];
            [[702u];["Value2"];[3];"702Value23";[16]];
            [[802u];["Value2"];[1];"802Value21";[16]];
            [[103u];["Value3"];[2];"103Value32";[17]];
            [[203u];["Value3"];[3];"203Value33";[17]];
            [[303u];["Value3"];[1];"303Value31";[17]];
            [[403u];["Value3"];[2];"403Value32";[17]];
            [[503u];["Value3"];[3];"503Value33";[17]];
            [[603u];["Value3"];[1];"603Value31";[17]];
            [[703u];["Value3"];[2];"703Value32";[17]];
            [[803u];["Value3"];[3];"803Value33";[17]]
        ])", FormatResultSetYson(exec.GetResultSet(0)));
    }
}

} // namespace NKikimr::NKqp
