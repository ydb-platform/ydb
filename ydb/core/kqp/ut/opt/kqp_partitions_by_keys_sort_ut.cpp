#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

namespace {

void CheckWindowFunctionAst(const TString& selectBody, bool useSortForPartitionsByKeys) {
    TKikimrRunner kikimr;
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    TStringBuilder query;
    query << "--!syntax_v1\n"
          << "PRAGMA ydb.OptUseSortForPartitionsByKeys = \""
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
        UNIT_ASSERT_C(!ast.Contains("SqueezeToDict"), ast);
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
}

} // namespace NKikimr::NKqp
