#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpPartitionsByKeysSort) {

    Y_UNIT_TEST_TWIN(WindowFunctionAst, UseSortForPartitionsByKeys) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TStringBuilder query;
        query << "--!syntax_v1\n"
              << "PRAGMA ydb.OptUseSortForPartitionsByKeys = \""
              << (UseSortForPartitionsByKeys ? "true" : "false") << "\";\n\n"
              << "SELECT Key, Text,\n"
              << "    row_number() OVER (PARTITION BY Text ORDER BY Key) AS rn\n"
              << "FROM `/Root/EightShard`\n"
              << "WHERE Text = 'Value2';\n";

        auto result = session.ExplainDataQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        const TString ast{result.GetAst()};

        Cerr << "=== Explain AST, UseSortForPartitionsByKeys="
             << (UseSortForPartitionsByKeys ? "true" : "false")
             << " ===\n" << ast << Endl;

        if (UseSortForPartitionsByKeys) {
            UNIT_ASSERT_C(ast.Contains("WideSort"), ast);
            UNIT_ASSERT_C(ast.Contains("Chopper"), ast);
            UNIT_ASSERT_C(ast.Contains("HashShuffle"), ast);
            UNIT_ASSERT_C(!ast.Contains("SqueezeToDict"), ast);
        } else {
            UNIT_ASSERT_C(ast.Contains("SqueezeToDict"), ast);
        }
    }

    Y_UNIT_TEST_TWIN(WindowFunctionMultiSortKeyAst, UseSortForPartitionsByKeys) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TStringBuilder query;
        query << "--!syntax_v1\n"
              << "PRAGMA ydb.OptUseSortForPartitionsByKeys = \""
              << (UseSortForPartitionsByKeys ? "true" : "false") << "\";\n\n"
              << "SELECT Key, Text, Data,\n"
              << "    row_number() OVER (PARTITION BY Text ORDER BY Data, Key) AS rn\n"
              << "FROM `/Root/EightShard`;\n";

        auto result = session.ExplainDataQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        const TString ast{result.GetAst()};

        Cerr << "=== Explain AST (MultiSortKey), UseSortForPartitionsByKeys="
             << (UseSortForPartitionsByKeys ? "true" : "false")
             << " ===\n" << ast << Endl;

        if (UseSortForPartitionsByKeys) {
            UNIT_ASSERT_C(ast.Contains("WideSort"), ast);
            UNIT_ASSERT_C(ast.Contains("Chopper"), ast);
            UNIT_ASSERT_C(ast.Contains("HashShuffle"), ast);
            UNIT_ASSERT_C(!ast.Contains("SqueezeToDict"), ast);
        } else {
            UNIT_ASSERT_C(ast.Contains("SqueezeToDict"), ast);
        }
    }
}

} // namespace NKikimr::NKqp
