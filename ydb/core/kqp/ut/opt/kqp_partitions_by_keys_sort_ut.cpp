#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

namespace {

void CheckWindowFunctionAst(
    const TString& selectBody,
    bool useSortForPartitionsByKeys,
    bool rejectSqueezeToList = false,
    bool allowSqueezeToDict = false,
    bool rejectNarrowSort = false)
{
    TKikimrRunner kikimr;
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    TStringBuilder query;
    query << "--!syntax_v1\n"
          << "PRAGMA ydb.WindowFunctionsV2 = \""
          << (useSortForPartitionsByKeys ? "true" : "false") << "\";\n\n"
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
        if (!allowSqueezeToDict) {
            UNIT_ASSERT_C(!ast.Contains("SqueezeToDict"), ast);
        }
        if (rejectSqueezeToList) {
            UNIT_ASSERT_C(!ast.Contains("SqueezeToList"), ast);
        }
        if (rejectNarrowSort) {
            UNIT_ASSERT_C(!ast.Contains("(Sort "), ast);
        }
    } else {
        UNIT_ASSERT_C(ast.Contains("SqueezeToDict"), ast);
    }
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

    Y_UNIT_TEST_TWIN(WindowFunctionFullFrameAfterRowNumbersAst, UseSortForPartitionsByKeys) {
        CheckWindowFunctionAst(
            "SELECT Key, Text, Data,\n"
            "    ROW_NUMBER() OVER (PARTITION BY Text ORDER BY Key) AS rn1,\n"
            "    ROW_NUMBER() OVER (PARTITION BY Data ORDER BY Key) AS rn2,\n"
            "    SUM(Data) OVER (PARTITION BY Text) AS total\n"
            "FROM `/Root/EightShard`;\n",
            UseSortForPartitionsByKeys,
            true,
            true);
    }

    Y_UNIT_TEST_TWIN(WindowFunctionComputedSortKeyWithFullFrameAst, UseSortForPartitionsByKeys) {
        CheckWindowFunctionAst(
            "SELECT Key, Text, Data,\n"
            "    SUM(Data) OVER (PARTITION BY Text) AS total,\n"
            "    ROW_NUMBER() OVER (PARTITION BY Text ORDER BY Abs(Data) DESC, Key) AS rn\n"
            "FROM `/Root/EightShard`;\n",
            UseSortForPartitionsByKeys,
            true,
            true);
    }

    Y_UNIT_TEST_TWIN(WindowFunctionRebuildPlanAst, UseSortForPartitionsByKeys) {
        CheckWindowFunctionAst(
            "$input = SELECT\n"
            "    premium.Text AS grp_agr,\n"
            "    intermediary.Text AS grp_type,\n"
            "    premium.Key AS grp_id,\n"
            "    intermediary.Data AS grp_perc,\n"
            "    intermediary.Key % 3 AS perc_10000,\n"
            "    CAST(premium.Data AS Int64) AS premium_cents,\n"
            "    CAST(premium.Key AS Int64) AS wo_cents,\n"
            "    premium.Text AS pr_nk\n"
            "FROM `/Root/EightShard` AS premium\n"
            "JOIN `/Root/EightShard` AS intermediary\n"
            "ON intermediary.Key = premium.Key;\n"
            "\n"
            "SELECT\n"
            "    input.*,\n"
            "    SUM(COALESCE(premium_cents, 0)) OVER wgrp AS grp_prem,\n"
            "    SUM(COALESCE(wo_cents, 0)) OVER wgrp AS grp_fact,\n"
            "    SUM(IF(wo_cents != 0, wo_cents, 0)) OVER wgrp AS grp_wo_prem,\n"
            "    SUM(IF(premium_cents != 0, premium_cents, 0)) OVER wgrp AS grp_fact_wo,\n"
            "    ROW_NUMBER() OVER (\n"
            "        PARTITION BY grp_agr, grp_type, grp_id, grp_perc\n"
            "        ORDER BY ABS(COALESCE(premium_cents, 0)) DESC, pr_nk\n"
            "    ) AS rn,\n"
            "    ROW_NUMBER() OVER (\n"
            "        PARTITION BY grp_agr, grp_type, grp_id, grp_perc\n"
            "        ORDER BY ABS(COALESCE(wo_cents, 0)) DESC, pr_nk\n"
            "    ) AS rn_off\n"
            "FROM $input AS input\n"
            "WINDOW wgrp AS (\n"
            "    PARTITION BY grp_agr, grp_type, grp_id, grp_perc, perc_10000\n"
            ");\n",
            UseSortForPartitionsByKeys,
            true,
            true,
            true);
    }
}

} // namespace NKikimr::NKqp
