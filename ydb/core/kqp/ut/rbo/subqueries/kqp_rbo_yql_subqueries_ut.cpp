#include <ydb/core/kqp/ut/olap/helpers/get_value.h>
#include <ydb/core/kqp/ut/olap/helpers/query_executor.h>
#include <ydb/core/kqp/ut/olap/helpers/local.h>
#include <ydb/core/kqp/ut/olap/helpers/writer.h>
#include <ydb/core/kqp/ut/olap/helpers/test_case.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/query_id.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/common/simple/settings.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/kqp/opt/rbo/kqp_operator.h>
#include <ydb/core/kqp/opt/rbo/kqp_plan_conversion_utils.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_utils.h>
#include <ydb/core/kqp/ut/rbo/kqp_rbo_test_helpers.h>
#include <ydb/core/kqp/opt/rbo/traces/kqp_rbo_trace_output.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>
#include <ydb/core/kqp/query_data/kqp_prepared_query.h>
#include <ydb/core/statistics/ut_common/ut_common.h>
#include <ydb/core/tx/columnshard/engines/scheme/objects_cache.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/aclib/aclib.h>
#include <yql/essentials/core/pg_settings/guc_settings.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/parser/pg_catalog/catalog.h>
#include <yql/essentials/parser/pg_wrapper/interface/codec.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <ydb/public/lib/ut_helpers/ut_helpers_query.h>
#include <ydb/public/lib/ydb_cli/common/format.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>

#include <util/string/split.h>

#include <algorithm>
#include <array>
#include <ctime>
#include <optional>
#include <regex>
#include <fstream>
#include <utility>

// These headers import NKikimr::NKqp into the global namespace, so include them last
#include <ydb/core/kqp/opt/rbo/physical_conversion/kqp_rbo_physical_aggregation_builder.h>
#include <ydb/core/kqp/opt/rbo/physical_conversion/kqp_rbo_physical_join_builder.h>


namespace NKikimr::NKqp {
namespace {

using namespace NKikimr;
using namespace NKikimr::NKqp;
using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYql::NNodes;
using namespace NStat;

void TestDecorrelation(bool columnTables, bool equalNullsJoinKeys = false) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
    appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
    appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
    appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
    appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
    appConfig.MutableTableServiceConfig()->SetEnableBlockHashJoinEqualNulls(equalNullsJoinKeys);
    TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    TString schemeQuery;
    for (const auto* table : {"t1", "t2", "t3"}) {
        schemeQuery += TStringBuilder() << R"(
            CREATE TABLE `/Root/)" << table << R"(` (
                a   Int64   NOT NULL,
                b   Int64   NOT NULL,
                c   Int64   NOT NULL,
                d   String,
                e   Int64,
                primary key(a)
            ))" << (columnTables ? " WITH (STORE = column)" : "") << ";\n";
    }

    auto schemeResult = session.ExecuteSchemeQuery(schemeQuery).GetValueSync();
    UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

    //   t1: a in [1, 12], b = a % 4, c = 10 * a, d = 'g' || (a % 3), e = a % 4 except null for a == 1
    //   a | 1   2   3   4   5   6   7   8   9   10  11  12
    //   b | 1   2   3   0   1   2   3   0   1   2   3   0
    //   c | 10  20  30  40  50  60  70  80  90  100 110 120
    //   d | g1  g2  g0  g1  g2  g0  g1  g2  g0  g1  g2  g0
    //   e | -   2   3   0   1   2   3   0   1   2   3   0
    //
    //   t2: a in [1, 12], b = a % 3, c = a * a, d = 'g' || (a % 4), e = a % 4 except null for a == 1
    //   a | 1   2   3   4   5   6   7   8   9   10  11  12
    //   b | 1   2   0   1   2   0   1   2   0   1   2   0
    //   c | 1   4   9   16  25  36  49  64  81  100 121 144
    //   d | g1  g2  g3  g0  g1  g2  g3  g0  g1  g2  g3  g0
    //   e | -   2   3   0   1   2   3   0   1   2   3   0
    //
    //   t3: a in [1, 6], b = a % 2, c = a, d = 'g' || (a % 2), e as above
    auto fill = [&](const TString& table, i64 rowCount, auto&& b, auto&& c, auto&& d) {
        NYdb::TValueBuilder rows;
        rows.BeginList();
        for (i64 a = 1; a <= rowCount; ++a) {
            rows.AddListItem()
                .BeginStruct()
                .AddMember("a").Int64(a)
                .AddMember("b").Int64(b(a))
                .AddMember("c").Int64(c(a))
                .AddMember("d").String(d(a))
                .AddMember("e").OptionalInt64(a == 1 ? std::nullopt : std::make_optional(a % 4))
                .EndStruct();
        }
        rows.EndList();

        auto result = db.BulkUpsert(table, rows.Build()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    };

    auto group = [](i64 n) { return TString("g") + std::to_string(n); };
    fill("/Root/t1", 12, [](i64 a) { return a % 4; }, [](i64 a) { return 10 * a; }, [&](i64 a) { return group(a % 3); });
    fill("/Root/t2", 12, [](i64 a) { return a % 3; }, [](i64 a) { return a * a; }, [&](i64 a) { return group(a % 4); });
    fill("/Root/t3", 6, [](i64 a) { return a % 2; }, [](i64 a) { return a; }, [&](i64 a) { return group(a % 2); });

    auto queryClient = kikimr.GetQueryClient();

    std::vector<std::pair<std::string, std::string>> cases = {
        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE t1.c > (SELECT max(t2.c) - 44 FROM `/Root/t2` as t2)
            ORDER BY t1.a;
         )",
         R"([[11];[12]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE t1.c * 2 >= (SELECT max(t2.c) FROM `/Root/t2` as t2 WHERE t2.b == t1.b)
            ORDER BY t1.a;
         )",
         R"([[5];[8];[9];[10];[12]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE t1.c > (SELECT max(t2.c) FROM `/Root/t2` as t2 WHERE t2.a <= t1.a)
            ORDER BY t1.a;
         )",
         R"([[1];[2];[3];[4];[5];[6];[7];[8];[9]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE t1.c > (SELECT max(t2.c - t1.b) FROM `/Root/t2` as t2 WHERE t2.a <= t1.a)
            ORDER BY t1.a;
         )",
         R"([[1];[2];[3];[4];[5];[6];[7];[8];[9];[10]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE t1.b IN (SELECT t2.a - t1.a FROM `/Root/t2` as t2 WHERE t2.a >= t1.a)
            ORDER BY t1.a;
         )",
         R"([[1];[2];[3];[4];[5];[6];[7];[8];[9];[10];[12]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE t1.a IN (SELECT t2.a + t1.b FROM `/Root/t2` as t2 WHERE t2.b == t1.b)
            ORDER BY t1.a;
         )",
         R"([[5];[10];[12]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE t1.b IN (SELECT CAST(count(*) AS Int64) FROM `/Root/t2` as t2 WHERE t2.a <= t1.a GROUP BY t2.b)
            ORDER BY t1.a;
         )",
         R"([[1];[5];[6];[7];[11]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE EXISTS (SELECT t2.b FROM `/Root/t2` as t2 WHERE t2.a <= t1.a GROUP BY t2.b HAVING count(*) > 3)
            ORDER BY t1.a;
         )",
         R"([[10];[11];[12]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE (NOT EXISTS (SELECT t2.a FROM `/Root/t2` as t2 WHERE t2.a == t1.a AND t2.b == 0)) OR t1.a == 1
            ORDER BY t1.a;
         )",
         R"([[1];[2];[4];[5];[7];[8];[10];[11]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE (t1.a NOT IN (SELECT t2.a FROM `/Root/t2` as t2 WHERE t2.a == t1.a AND t2.b == 0)) OR t1.a == 3
            ORDER BY t1.a;
         )",
         R"([[1];[2];[3];[4];[5];[7];[8];[10];[11]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE t1.a IN (
                SELECT t2.a + t1.b FROM `/Root/t2` as t2 WHERE t2.b == 0
                UNION ALL
                SELECT t3.a * 2 FROM `/Root/t3` as t3 WHERE t3.a <= t1.b
            )
            ORDER BY t1.a;
         )",
         R"([[2];[12]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE EXISTS (
                SELECT t2.a FROM `/Root/t2` as t2 JOIN `/Root/t3` as t3 ON t2.b == t3.b
                WHERE t2.a > t1.a * 2 GROUP BY t2.a
            )
            ORDER BY t1.a;
         )",
         R"([[1];[2];[3];[4];[5]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE t1.c > (
                SELECT max(t2.c) FROM `/Root/t2` as t2
                WHERE t2.a <= t1.a AND t2.b IN (SELECT t3.b FROM `/Root/t3` as t3 WHERE t3.a <= t2.a)
            )
            ORDER BY t1.a;
         )",
         R"([[1];[2];[3];[4];[5];[6];[7];[8];[9];[11]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE t1.d IN (SELECT t2.d FROM `/Root/t2` as t2 WHERE t2.a == t1.a + 1)
            ORDER BY t1.a;
         )",
         R"([[3];[4];[5]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE (t1.d NOT IN (SELECT t2.d FROM `/Root/t2` as t2 WHERE t2.a == t1.a + 1)) OR t1.a == 3
            ORDER BY t1.a;
         )",
         R"([[1];[2];[3];[6];[7];[8];[9];[10];[11];[12]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE (t1.b NOT IN (SELECT IF(t2.a == 1, NULL, t2.b) FROM `/Root/t2` as t2)) OR t1.a == 3
            ORDER BY t1.a;
         )",
         R"([[3]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE (t1.b NOT IN (SELECT IF(t2.a == 3, NULL, t2.b) FROM `/Root/t2` as t2 WHERE t2.a == t1.a)) OR t1.a == 1
            ORDER BY t1.a;
         )",
         R"([[1];[4];[5];[6];[7];[8];[9];[10];[11]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE (t1.e NOT IN (SELECT t2.b FROM `/Root/t2` as t2)) OR t1.a == 12
            ORDER BY t1.a;
         )",
         R"([[3];[7];[11];[12]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE (t1.e NOT IN (SELECT t2.b FROM `/Root/t2` as t2 WHERE t2.a > 100)) OR t1.a == 12
            ORDER BY t1.a;
         )",
         R"([[1];[2];[3];[4];[5];[6];[7];[8];[9];[10];[11];[12]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE (t1.e NOT IN (SELECT t2.b FROM `/Root/t2` as t2 WHERE t2.a == t1.a)) OR t1.a == 12
            ORDER BY t1.a;
         )",
         R"([[3];[4];[5];[6];[7];[8];[9];[10];[11];[12]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE EXISTS (
                SELECT t2.a FROM `/Root/t2` as t2 WHERE t2.a > t1.a * 2 GROUP BY t2.a
            )
            ORDER BY t1.a;
         )",
         R"([[1];[2];[3];[4];[5]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE EXISTS (
                SELECT t2.d FROM `/Root/t2` as t2 WHERE t2.b == t1.b GROUP BY t2.d HAVING count(*) > 0
            )
            ORDER BY t1.a;
         )",
         R"([[1];[2];[4];[5];[6];[8];[9];[10];[12]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE t1.c > (SELECT t2.c FROM `/Root/t2` as t2 WHERE t2.a == t1.a)
            ORDER BY t1.a;
         )",
         R"([[1];[2];[3];[4];[5];[6];[7];[8];[9]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE (SELECT max(t2.c) FROM `/Root/t2` as t2 WHERE t2.b == t1.e) IS NULL
            ORDER BY t1.a;
         )",
         R"([[1];[3];[7];[11]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE (SELECT max(t2.c) FROM `/Root/t2` as t2 WHERE t1.e IS NULL OR t2.b == t1.e) == 144
            ORDER BY t1.a;
         )",
         R"([[1];[4];[8];[12]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE (SELECT count(*) FROM `/Root/t2` as t2 WHERE t2.b == t1.b) == 4
            ORDER BY t1.a;
         )",
         R"([[1];[2];[4];[5];[6];[8];[9];[10];[12]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE (SELECT count(*) FROM `/Root/t2` as t2 WHERE t2.b == t1.b) == 0
            ORDER BY t1.a;
         )",
         R"([[3];[7];[11]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE EXISTS (SELECT 1 FROM `/Root/t2` as t2 WHERE t2.b == t1.b)
            ORDER BY t1.a;
         )",
         R"([[1];[2];[4];[5];[6];[8];[9];[10];[12]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE EXISTS (SELECT 1 FROM `/Root/t2` as t2 WHERE t2.e == t1.e)
            ORDER BY t1.a;
         )",
         R"([[2];[3];[4];[5];[6];[7];[8];[9];[10];[11];[12]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE EXISTS (SELECT 1 FROM `/Root/t2` as t2 WHERE t2.b == COALESCE(t1.e, 0) AND t2.a <= 3)
            ORDER BY t1.a;
         )",
         R"([[1];[2];[4];[5];[6];[8];[9];[10];[12]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE NOT EXISTS (SELECT 1 FROM `/Root/t2` as t2 WHERE t2.b == COALESCE(t1.e, 0) AND t2.a <= 3)
            ORDER BY t1.a;
         )",
         R"([[3];[7];[11]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE t1.b IN (SELECT t2.b FROM `/Root/t2` as t2 WHERE t2.b == COALESCE(t1.e, 1) AND t2.a <= 3)
            ORDER BY t1.a;
         )",
         R"([[1];[2];[4];[5];[6];[8];[9];[10];[12]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE t1.b NOT IN (SELECT t2.b FROM `/Root/t2` as t2 WHERE t2.b == COALESCE(t1.e, 1) AND t2.a <= 3)
            ORDER BY t1.a;
         )",
         R"([[3];[7];[11]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE (SELECT max(t2.c) FROM `/Root/t2` as t2 WHERE t2.e == t1.e) IS NULL
            ORDER BY t1.a;
         )",
         R"([[1]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE (SELECT count(*) FROM `/Root/t2` as t2 WHERE t2.e == t1.e) == 0
            ORDER BY t1.a;
         )",
         R"([[1]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE NOT (t1.c > 100) AND t1.a IN (SELECT t2.a FROM `/Root/t2` as t2 WHERE t2.a <= 3)
            ORDER BY t1.a;
         )",
         R"([[1];[2];[3]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE NOT EXISTS (SELECT 1 FROM `/Root/t2` as t2 WHERE t2.a == t1.a + 9)
              AND t1.a IN (SELECT t2.a FROM `/Root/t2` as t2 WHERE t2.a <= 5)
            ORDER BY t1.a;
         )",
         R"([[4];[5]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE t1.a NOT IN (SELECT t2.e FROM `/Root/t2` as t2)
            ORDER BY t1.a;
         )",
         R"([])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE t1.e NOT IN (SELECT t2.a FROM `/Root/t2` as t2 WHERE t2.a <= 2)
            ORDER BY t1.a;
         )",
         R"([[3];[4];[7];[8];[11];[12]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE t1.a NOT IN (SELECT t2.a FROM `/Root/t2` as t2 WHERE t2.a <= 10)
            ORDER BY t1.a;
         )",
         R"([[11];[12]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE t1.c > (SELECT min(t2.c) FROM `/Root/t2` as t2 WHERE t2.b == COALESCE(t1.e, 0))
            ORDER BY t1.a;
         )",
         R"([[1];[2];[4];[5];[6];[8];[9];[10];[12]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE EXISTS (SELECT 1 FROM `/Root/t2` as t2 RIGHT JOIN `/Root/t3` as t3 ON t2.a == t3.a
                          WHERE t3.c == t1.a)
            ORDER BY t1.a;
         )",
         R"([[1];[2];[3];[4];[5];[6]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE EXISTS (SELECT 1 FROM `/Root/t2` as t2 RIGHT JOIN `/Root/t3` as t3 ON t2.b == t3.b
                          WHERE t3.a == t1.e)
            ORDER BY t1.a;
         )",
         R"([[2];[3];[5];[6];[7];[9];[10];[11]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE NOT EXISTS (SELECT 1 FROM `/Root/t2` as t2 RIGHT JOIN `/Root/t3` as t3 ON t2.a == t3.a
                              WHERE t2.c == t1.a)
            ORDER BY t1.a;
         )",
         R"([[2];[3];[5];[6];[7];[8];[10];[11];[12]])"},

        // ORDER BY without LIMIT does not change the subquery result.
        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE t1.b IN (SELECT t2.b FROM `/Root/t2` as t2 WHERE t2.a < t1.a ORDER BY t2.a DESC)
            ORDER BY t1.a;
         )",
         R"([[4];[5];[6];[8];[9];[10];[12]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE EXISTS (SELECT 1 FROM `/Root/t2` as t2 WHERE t2.b == t1.b ORDER BY t2.c)
            ORDER BY t1.a;
         )",
         R"([[1];[2];[4];[5];[6];[8];[9];[10];[12]])"},

        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE t1.c > (SELECT t2.c FROM `/Root/t2` as t2 WHERE t2.a == t1.a ORDER BY t2.c)
            ORDER BY t1.a;
         )",
         R"([[1];[2];[3];[4];[5];[6];[7];[8];[9]])"},

        // ORDER BY ... LIMIT applies per outer row.
        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE EXISTS (SELECT 1 FROM `/Root/t2` as t2 WHERE t2.b == t1.b LIMIT 1)
            ORDER BY t1.a;
         )",
         R"([[1];[2];[4];[5];[6];[8];[9];[10];[12]])"},

        // Without the limit 12 qualifies too: t2.b == 0 for t2.a == 9.
        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE t1.b IN (SELECT t2.b FROM `/Root/t2` as t2 WHERE t2.a < t1.a ORDER BY t2.a DESC LIMIT 2)
            ORDER BY t1.a;
         )",
         R"([[4];[5];[6];[8];[9];[10]])"},

        // The second largest t2.c per t2.b: 81, 49, 64.
        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE t1.c > (SELECT t2.c FROM `/Root/t2` as t2 WHERE t2.b == t1.b ORDER BY t2.c DESC LIMIT 1 OFFSET 1)
            ORDER BY t1.a;
         )",
         R"([[5];[9];[10];[12]])"},

        {R"(
            SELECT t1.a, (SELECT t2.a FROM `/Root/t2` as t2 WHERE t2.b == t1.b AND t2.a > t1.a ORDER BY t2.a LIMIT 1) AS next
            FROM `/Root/t1` as t1
            ORDER BY t1.a;
         )",
         R"([[1;[4]];[2;[5]];[3;#];[4;[6]];[5;[7]];[6;[8]];[7;#];[8;[9]];[9;[10]];[10;[11]];[11;#];[12;#]])"},

        // Not shared, so not copied: the aggregate some of the inner scalar subquery is decorrelated in place.
        // The inner subquery keeps t2.a <= 6.
        {R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE t1.b IN (
                SELECT t2.b FROM `/Root/t2` as t2
                WHERE t2.a < t1.a AND t2.c >= (SELECT t3.c FROM `/Root/t3` as t3 WHERE t3.a == t2.a)
                GROUP BY t2.b, t2.d
            )
            ORDER BY t1.a;
         )",
         R"([[4];[5];[6];[8];[9];[10];[12]])"},
    };

    for (ui32 i = 0; i < cases.size(); ++i) {
        const auto& [query, expected] = cases[i];
        auto querySession = queryClient.GetSession().GetValueSync().GetSession();
        auto result = querySession.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), TStringBuilder() << "query " << i << ": " << result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(FormatResultSetYson(result.GetResultSet(0)), expected, "query " << i);
    }

    const std::vector<std::string> multiRowQueries = {
        R"(SELECT t1.a FROM `/Root/t1` as t1 WHERE t1.c > (SELECT t2.c FROM `/Root/t2` as t2) ORDER BY t1.a;)",

        R"(SELECT t1.a FROM `/Root/t1` as t1
           WHERE t1.c > (SELECT t2.c FROM `/Root/t2` as t2 WHERE t2.b == t1.b)
           ORDER BY t1.a;)",

        R"(SELECT t1.a FROM `/Root/t1` as t1
           WHERE t1.c > (SELECT t2.c FROM `/Root/t2` as t2 WHERE t2.b == t1.b GROUP BY t2.c)
           ORDER BY t1.a;)",

        // Dropping the sort keeps the check.
        R"(SELECT t1.a FROM `/Root/t1` as t1
           WHERE t1.c > (SELECT t2.c FROM `/Root/t2` as t2 WHERE t2.b == t1.b ORDER BY t2.c)
           ORDER BY t1.a;)",
    };

    for (ui32 i = 0; i < multiRowQueries.size(); ++i) {
        auto errorSession = queryClient.GetSession().GetValueSync().GetSession();
        auto result =
            errorSession.ExecuteQuery(TString(multiRowQueries[i]), NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(!result.IsSuccess(), "multi row query " << i << " unexpectedly succeeded");
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Scalar subquery returned more than one row",
                                      "multi row query " << i);
    }

    // The grouping set branches share the correlated filter through a Replicate. Decorrelation copies a shared
    // correlated operator for each branch, but a copy is evaluated again, so with a nondeterministic expression
    // the branches could see other rows. Every predicate below keeps every row: once such a filter is supported,
    // the expected result is [[4];[5];[6];[8];[9];[10];[12]].
    const std::vector<std::string> nondeterministicPredicates = {
        // Random returns a value below 1.
        "Random(t2.a) < 2.0",
        // Only the peephole path turns a call without arguments into a parameter.
        R"(CurrentUtcTimestamp() > Timestamp("2000-01-01T00:00:00Z"))",
        R"(CurrentUtcDate(t2.a) > Date("2000-01-01"))",
        // This UDF is deterministic, but nothing says so for UDFs in general.
        "Digest::IntHash64(CAST(t2.a AS Uint64)) >= 0",
    };

    for (const auto& predicate : nondeterministicPredicates) {
        const TString query = TStringBuilder() << R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE t1.b IN (
                SELECT t2.b FROM `/Root/t2` as t2 WHERE t2.a < t1.a AND )" << predicate << R"(
                GROUP BY ROLLUP(t2.b, t2.d)
            )
            ORDER BY t1.a;
        )";
        const auto status = queryClient.RetryQuerySync([&](NYdb::NQuery::TSession session) -> NYdb::TStatus {
            return session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        });
        UNIT_ASSERT_C(!status.IsSuccess(), predicate << " unexpectedly succeeded");
        UNIT_ASSERT_STRING_CONTAINS_C(status.GetIssues().ToString(), "correlation cannot be pushed through Replicate", predicate);
    }
}

} // namespace

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpRboYql) {

    Y_UNIT_TEST(CorrelatedScalarAggregateReuseDoesNotDuplicateVisibleColumns) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto tableClient = kikimr.GetTableClient();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();

        auto schemeResult = tableSession.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/sales` (
                id Int64 NOT NULL,
                k Int64 NOT NULL,
                v Double,
                PRIMARY KEY(id)
            ) WITH (STORE = COLUMN);
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        auto queryClient = kikimr.GetQueryClient();
        auto querySession = queryClient.GetSession().GetValueSync().GetSession();
        auto result = querySession.ExecuteQuery(R"(
            PRAGMA YqlSelect = 'force';
            PRAGMA AnsiImplicitCrossJoin;

            $totals = (
                SELECT
                    k AS group_key,
                    Sum(v) AS total
                FROM `/Root/sales`
                GROUP BY k
            );

            SELECT
                a.group_key
            FROM $totals AS a
            WHERE a.total > (
                SELECT
                    Avg(total)
                FROM $totals AS b
                WHERE a.group_key == b.group_key
            )
            ORDER BY a.group_key;
        )",
            NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
            .ExtractValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    Y_UNIT_TEST_TWIN(DecorrelationEqualNullsJoinKeys, EqualNullsJoinKeys) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        appConfig.MutableTableServiceConfig()->SetEnableBlockHashJoinEqualNulls(EqualNullsJoinKeys);

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        auto schemeResult = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (a Int64 NOT NULL, e Int64, primary key(a)) WITH (STORE = column);
            CREATE TABLE `/Root/t2` (a Int64 NOT NULL, b Int64 NOT NULL, c Int64 NOT NULL, primary key(a)) WITH (STORE = column);
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        NYdb::TValueBuilder t1Rows;
        t1Rows.BeginList();
        for (i64 a = 1; a <= 4; ++a) {
            t1Rows.AddListItem().BeginStruct()
                .AddMember("a").Int64(a)
                .AddMember("e").OptionalInt64(a == 1 ? std::nullopt : std::make_optional(a % 4))
                .EndStruct();
        }
        t1Rows.EndList();
        auto upsertResult = db.BulkUpsert("/Root/t1", t1Rows.Build()).GetValueSync();
        UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());

        NYdb::TValueBuilder t2Rows;
        t2Rows.BeginList();
        for (i64 a = 1; a <= 4; ++a) {
            t2Rows.AddListItem().BeginStruct()
                .AddMember("a").Int64(a)
                .AddMember("b").Int64(a % 3)
                .AddMember("c").Int64(a * a)
                .EndStruct();
        }
        t2Rows.EndList();
        upsertResult = db.BulkUpsert("/Root/t2", t2Rows.Build()).GetValueSync();
        UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());

        const TString query = R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE (SELECT max(t2.c) FROM `/Root/t2` as t2 WHERE t1.e IS NULL OR t2.b == t1.e) == 16
            ORDER BY t1.a;
        )";

        auto queryClient = kikimr.GetQueryClient();
        auto querySession = queryClient.GetSession().GetValueSync().GetSession();

        auto explainResult = querySession.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).ExtractValueSync();
        UNIT_ASSERT_C(explainResult.IsSuccess(), explainResult.GetIssues().ToString());
        const auto ast = TString{*explainResult.GetStats()->GetAst()};

        if (EqualNullsJoinKeys) {
            UNIT_ASSERT_C(ast.Contains("EqualNulls"), "expected the join to carry EqualNulls settings, ast:\n" << ast);
            UNIT_ASSERT_C(!ast.Contains("StablePickle"), "expected no StablePickle encoding, ast:\n" << ast);
        } else {
            UNIT_ASSERT_C(ast.Contains("StablePickle"), "expected StablePickle encoded join keys, ast:\n" << ast);
            UNIT_ASSERT_C(!ast.Contains("EqualNulls"), "expected no EqualNulls settings, ast:\n" << ast);
        }

        auto result = querySession.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), R"([[1]])");
    }

    Y_UNIT_TEST(PushFilterBeforeInlining) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto tableClient = kikimr.GetTableClient();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();

        auto schemeResult = tableSession.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (
                id Int64 NOT NULL,
                a Int64 NOT NULL,
                b Int64 NOT NULL,
                c Double,
                d Int64,
                PRIMARY KEY(id)
            ) WITH (STORE = COLUMN);

            CREATE TABLE `/Root/t2` (
                a Int64 NOT NULL,
                PRIMARY KEY(a)
            ) WITH (STORE = COLUMN);
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        struct TRow {
            i64 Id;
            i64 A;
            i64 B;
            double C;
            i64 D;
        };
        const TVector<TRow> t1Rows = {
            {1, 1, 1, 10.0, 100},
            {2, 1, 2, 30.0, 200},
            {3, 2, 1, 50.0, 1000},
            {4, 2, 2, 100.0, 2000},
        };

        NYdb::TValueBuilder t1Builder;
        t1Builder.BeginList();
        for (const auto& row : t1Rows) {
            t1Builder.AddListItem().BeginStruct()
                .AddMember("id").Int64(row.Id)
                .AddMember("a").Int64(row.A)
                .AddMember("b").Int64(row.B)
                .AddMember("c").Double(row.C)
                .AddMember("d").Int64(row.D)
                .EndStruct();
        }
        t1Builder.EndList();
        auto upsertResult = tableClient.BulkUpsert("/Root/t1", t1Builder.Build()).GetValueSync();
        UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());

        NYdb::TValueBuilder t2Builder;
        t2Builder.BeginList();
        for (const auto a : {1, 2}) {
            t2Builder.AddListItem().BeginStruct().AddMember("a").Int64(a).EndStruct();
        }
        t2Builder.EndList();
        upsertResult = tableClient.BulkUpsert("/Root/t2", t2Builder.Build()).GetValueSync();
        UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());

        auto queryClient = kikimr.GetQueryClient();
        auto querySession = queryClient.GetSession().GetValueSync().GetSession();

        const auto check = [&](const TString& name, const TString& query, const TString& expected) {
            auto explainResult = querySession.ExecuteQuery(query,
                NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)
            ).ExtractValueSync();
            UNIT_ASSERT_C(explainResult.IsSuccess(), name + ": " + explainResult.GetIssues().ToString());

            const auto plan = TString{*explainResult.GetStats()->GetPlan()};
            UNIT_ASSERT_C(!plan.Contains("CrossJoin"), name + ":\n" + plan);

            auto result = querySession.ExecuteQuery(query,
                NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute)
            ).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), name + ": " + result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL_C(FormatResultSetYson(result.GetResultSet(0)), expected, name);
        };

        check("uncorrelated scalar subquery in ==", R"(
            PRAGMA YqlSelect = 'force';
            PRAGMA AnsiImplicitCrossJoin;

            SELECT SUM(d) AS total
            FROM `/Root/t1` AS t1, `/Root/t2` AS t2
            WHERE t1.a == t2.a
              AND d == (SELECT MAX(d) FROM `/Root/t1` AS t3);
        )", R"([[[2000]]])");

        check("correlated scalar subquery with non-eliminable domain", R"(
            PRAGMA YqlSelect = 'force';
            PRAGMA AnsiImplicitCrossJoin;

            SELECT SUM(d) AS total
            FROM `/Root/t1` AS t1, `/Root/t2` AS t2
            WHERE t1.a == t2.a
              AND c < (SELECT AVG(t3.c) FROM `/Root/t1` AS t3
                       WHERE t3.a == t2.a AND t3.b != t1.b);
        )", R"([[[1100]]])");

        check("correlated exists and not exists with non-eliminable domains", R"(
            PRAGMA YqlSelect = 'force';
            PRAGMA AnsiImplicitCrossJoin;

            SELECT SUM(d) AS total
            FROM `/Root/t1` AS t1, `/Root/t2` AS t2
            WHERE t1.a == t2.a
              AND EXISTS (SELECT * FROM `/Root/t1` AS t3
                          WHERE t3.a == t1.a AND t3.b != t1.b AND t3.c > t1.c)
              AND NOT EXISTS (SELECT * FROM `/Root/t1` AS t4
                              WHERE t4.a == t1.a AND t4.b != t1.b AND t4.d > 1500);
        )", R"([[[100]]])");
    }

    Y_UNIT_TEST(CorrelatedScalarSubqueryCBO4KeepsOuterJoinKey) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        appConfig.MutableTableServiceConfig()->SetDefaultCostBasedOptimizationLevel(4);

        NKikimrKqp::TKqpSetting statsSetting;
        statsSetting.SetName("OptOverrideStatistics");
        statsSetting.SetValue(R"({
            "/Root/lineitem": {"n_rows": 4, "byte_size": 128},
            "/Root/part": {"n_rows": 2, "byte_size": 64}
        })");

        auto settings = NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false);
        settings.SetKqpSettings({statsSetting});
        TKikimrRunner kikimr(settings);

        auto tableClient = kikimr.GetTableClient();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();
        auto schemeResult = tableSession.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/lineitem` (
                id Int64 NOT NULL,
                l_partkey Int64 NOT NULL,
                l_quantity Double,
                l_extendedprice Int64,
                PRIMARY KEY(id)
            ) WITH (STORE = COLUMN);

            CREATE TABLE `/Root/part` (
                p_partkey Int64 NOT NULL,
                p_brand String,
                p_container String,
                PRIMARY KEY(p_partkey)
            ) WITH (STORE = COLUMN);
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        NYdb::TValueBuilder lineitemRows;
        lineitemRows.BeginList();
        lineitemRows.AddListItem().BeginStruct()
            .AddMember("id").Int64(1)
            .AddMember("l_partkey").Int64(1)
            .AddMember("l_quantity").Double(10.0)
            .AddMember("l_extendedprice").Int64(100)
            .EndStruct();
        lineitemRows.AddListItem().BeginStruct()
            .AddMember("id").Int64(2)
            .AddMember("l_partkey").Int64(1)
            .AddMember("l_quantity").Double(20.0)
            .AddMember("l_extendedprice").Int64(200)
            .EndStruct();
        lineitemRows.AddListItem().BeginStruct()
            .AddMember("id").Int64(3)
            .AddMember("l_partkey").Int64(2)
            .AddMember("l_quantity").Double(100.0)
            .AddMember("l_extendedprice").Int64(1000)
            .EndStruct();
        lineitemRows.AddListItem().BeginStruct()
            .AddMember("id").Int64(4)
            .AddMember("l_partkey").Int64(2)
            .AddMember("l_quantity").Double(200.0)
            .AddMember("l_extendedprice").Int64(2000)
            .EndStruct();
        lineitemRows.EndList();

        auto upsertResult = tableClient.BulkUpsert("/Root/lineitem", lineitemRows.Build()).GetValueSync();
        UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());

        NYdb::TValueBuilder partRows;
        partRows.BeginList();
        for (const auto partKey : {1, 2}) {
            partRows.AddListItem().BeginStruct()
                .AddMember("p_partkey").Int64(partKey)
                .AddMember("p_brand").String("Brand#23")
                .AddMember("p_container").String("MED BOX")
                .EndStruct();
        }
        partRows.EndList();

        upsertResult = tableClient.BulkUpsert("/Root/part", partRows.Build()).GetValueSync();
        UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());

        const TString query = R"(
            PRAGMA YqlSelect = 'force';
            PRAGMA AnsiImplicitCrossJoin;

            SELECT
                SUM(l_extendedprice) AS total
            FROM
                `/Root/lineitem` AS lineitem,
                `/Root/part` AS part
            WHERE
                p_partkey == l_partkey
                AND p_brand == 'Brand#23'
                AND p_container == 'MED BOX'
                AND l_quantity < (
                    SELECT
                        AVG(l_quantity)
                    FROM
                        `/Root/lineitem` AS lineitem
                    WHERE
                        l_partkey == p_partkey
                );
        )";

        auto queryClient = kikimr.GetQueryClient();
        auto querySession = queryClient.GetSession().GetValueSync().GetSession();

        auto explainResult = querySession.ExecuteQuery(query,
            NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)
        ).ExtractValueSync();
        UNIT_ASSERT_C(explainResult.IsSuccess(), explainResult.GetIssues().ToString());

        const auto plan = TString{*explainResult.GetStats()->GetPlan()};
        UNIT_ASSERT_C(!plan.Contains("CrossJoin"), plan);

        auto result = querySession.ExecuteQuery(query,
            NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute)
        ).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), R"([[[1100]]])");
    }

    Y_UNIT_TEST(ExpressionSubquery) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/foo` (
                id	Int64	NOT NULL,
                name	String,
                primary key(id)
            ) with (Store = Column);

            CREATE TABLE `/Root/bar` (
                id	Int64	NOT NULL,
                lastname	String,
                primary key(id)
            ) with (Store = Column);
        )").GetValueSync();

        NYdb::TValueBuilder rowsTableFoo;
        rowsTableFoo.BeginList();
        for (size_t i = 0; i < 4; ++i) {
            rowsTableFoo.AddListItem()
                .BeginStruct()
                .AddMember("id").Int64(i)
                .AddMember("name").String(std::to_string(i) + "_name")
                .EndStruct();
        }
        rowsTableFoo.EndList();

        auto resultUpsert = db.BulkUpsert("/Root/foo", rowsTableFoo.Build()).GetValueSync();
        UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

        NYdb::TValueBuilder rowsTableBar;
        rowsTableBar.BeginList();
        for (size_t i = 0; i < 4; ++i) {
            rowsTableBar.AddListItem()
                .BeginStruct()
                .AddMember("id").Int64(i)
                .AddMember("lastname").String(std::to_string(i) + "_name")
                .EndStruct();
        }
        rowsTableBar.EndList();

        resultUpsert = db.BulkUpsert("/Root/bar", rowsTableBar.Build()).GetValueSync();
        UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

        db = kikimr.GetTableClient();
        auto session2 = db.CreateSession().GetValueSync().GetSession();

        std::vector<std::string> queries = {
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT bar.id FROM `/Root/bar` as bar where bar.id = (SELECT max(foo.id) FROM `/Root/foo` as foo);
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT bar.id FROM `/Root/bar` as bar where bar.id IN (SELECT foo.id FROM `/Root/foo` as foo WHERE foo.id == 0);
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT bar.id FROM `/Root/bar` as bar where bar.id == 0 AND bar.id NOT IN (SELECT foo.id FROM `/Root/foo` as foo WHERE foo.id != 0);
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT bar.id FROM `/Root/bar` as bar where bar.id == 0 AND EXISTS (SELECT foo.id FROM `/Root/foo` as foo WHERE foo.id != 0);
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT bar.id FROM `/Root/bar` as bar where bar.id == 0 AND NOT EXISTS (SELECT foo.id FROM `/Root/foo` as foo WHERE foo.id == 6);
            )",
            R"(
                SELECT bar.id FROM `/Root/bar` as bar where bar.id == 0 AND (SELECT foo.id FROM `/Root/foo` as foo where foo.id == -1) IS NULL;
            )",
        };

        // TODO: The order of result is not defined, we need order by to add more interesting tests.
        std::vector<std::string> results = {
            R"([[3]])",
            R"([[0]])",
            R"([[0]])",
            R"([[0]])",
            R"([[0]])",
            R"([[0]])",
        };

        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto &query = queries[i];
            auto result = session2.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            //Cout << FormatResultSetYson(result.GetResultSet(0)) << Endl;
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
        }
    }

    Y_UNIT_TEST(CorrelatedSubquery) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/foo` (
                id	Int64	NOT NULL,
                id2 Int64 NOT NULL,
                name	String,
                primary key(id)
            ) with (Store = Column);

            CREATE TABLE `/Root/bar` (
                id	Int64	NOT NULL,
                id2 Int64 NOT NULL,
                lastname	String,
                primary key(id)
            ) with (Store = Column);
        )").GetValueSync();

        NYdb::TValueBuilder rowsTableFoo;
        rowsTableFoo.BeginList();
        for (size_t i = 0; i < 4; ++i) {
            rowsTableFoo.AddListItem()
                .BeginStruct()
                .AddMember("id").Int64(i)
                .AddMember("id2").Int64(i)
                .AddMember("name").String(std::to_string(i) + "_name")
                .EndStruct();
        }
        rowsTableFoo.EndList();

        auto resultUpsert = db.BulkUpsert("/Root/foo", rowsTableFoo.Build()).GetValueSync();
        UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

        NYdb::TValueBuilder rowsTableBar;
        rowsTableBar.BeginList();
        for (size_t i = 0; i < 4; ++i) {
            rowsTableBar.AddListItem()
                .BeginStruct()
                .AddMember("id").Int64(i)
                .AddMember("id2").Int64(i)
                .AddMember("lastname").String(std::to_string(i) + "_name")
                .EndStruct();
        }
        rowsTableBar.EndList();

        resultUpsert = db.BulkUpsert("/Root/bar", rowsTableBar.Build()).GetValueSync();
        UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

        db = kikimr.GetTableClient();
        auto session2 = db.CreateSession().GetValueSync().GetSession();

        std::vector<std::string> queries = {
            R"(
                SELECT bar.id FROM `/Root/bar` as bar where bar.id == (SELECT max(foo.id) FROM `/Root/foo` as foo WHERE foo.id == bar.id AND foo.name == lastname AND foo.id==1);
            )",
             R"(
                SELECT bar.id FROM `/Root/bar` as bar where EXISTS (SELECT foo.id FROM `/Root/foo` as foo WHERE foo.id == bar.id AND foo.name == lastname AND foo.id==1);
            )",
            R"(
                SELECT bar.id FROM `/Root/bar` as bar where bar.lastname IN (SELECT foo.name FROM `/Root/foo` as foo WHERE foo.id == bar.id AND foo.id==1);
            )",
            R"(
                SELECT bar.id FROM `/Root/bar` as bar where bar.lastname IN (SELECT foo.name FROM `/Root/foo` as foo WHERE foo.id == bar.id AND foo.id2 >= bar.id2 AND foo.id==1);
            )",
            R"(
                SELECT bar.id FROM `/Root/bar` as bar where bar.lastname NOT IN (SELECT foo.name FROM `/Root/foo` as foo WHERE foo.id > bar.id ) order by bar.id;
            )",
            R"(
                SELECT bar.id FROM `/Root/bar` as bar where (NOT EXISTS(SELECT foo.id FROM `/Root/foo` as foo)) OR bar.id == (SELECT max(foo.id) FROM `/Root/foo` as foo);
            )",
            R"(
                SELECT bar.id FROM `/Root/bar` as bar where (NOT EXISTS(SELECT foo.id FROM `/Root/foo` as foo where foo.id == bar.id)) OR bar.id == 1;
            )",
            R"(
                SELECT bar.id FROM `/Root/bar` as bar where (NOT EXISTS(SELECT foo.id FROM `/Root/foo` as foo where foo.id == bar.id AND foo.id == 1)) OR bar.id == 2 order by bar.id;
            )",
            R"(
                SELECT bar.id FROM `/Root/bar` as bar where (NOT EXISTS(SELECT foo.id FROM `/Root/foo` as foo where foo.id == bar.id AND foo.id2 > bar.id2)) OR bar.id == 0 order by bar.id;
            )",
            R"(
                SELECT bar.id FROM `/Root/bar` as bar where (bar.id2 NOT IN (SELECT foo.id2 FROM `/Root/foo` as foo where foo.id == bar.id AND foo.id == 1)) OR bar.id == 2 order by bar.id;
            )",
        };

        // TODO: The order of result is not defined, we need order by to add more interesting tests.
        std::vector<std::string> results = {
            R"([[1]])",
            R"([[1]])",
            R"([[1]])",
            R"([[1]])",
            R"([[0];[1];[2];[3]])",
            R"([[3]])",
            R"([[1]])",
            R"([[0];[2];[3]])",
            R"([[0];[1];[2];[3]])",
            R"([[0];[2];[3]])",
        };

        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto &query = queries[i];
            auto result = session2.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), TStringBuilder() << "query " << i << ": " << result.GetIssues().ToString());
            //Cout << FormatResultSetYson(result.GetResultSet(0)) << Endl;
            UNIT_ASSERT_VALUES_EQUAL_C(FormatResultSetYson(result.GetResultSet(0)), results[i], "query " << i);
        }
    }

    Y_UNIT_TEST_QUAD(Decorrelation, ColumnStore, EqualNullsJoinKeys) {
        TestDecorrelation(ColumnStore, EqualNullsJoinKeys);
    }

}

} // namespace NKikimr::NKqp
