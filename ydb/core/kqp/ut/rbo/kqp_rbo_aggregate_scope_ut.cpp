#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <yql/essentials/public/langver/yql_langver.h>

#include <initializer_list>
#include <optional>
#include <tuple>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpRboAggregateScope) {
    Y_UNIT_TEST(NestedQueryAndLambdaBindingsStayDistinct) {
        NKikimrConfig::TAppConfig config;
        config.MutableTableServiceConfig()->SetEnableNewRBO(true);
        config.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        config.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        config.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        TKikimrRunner kikimr(TKikimrSettings(config).SetWithSampleTables(false));
        auto tableSession = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        const auto scheme = tableSession.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/ScopeOuter` (
                id Int64 NOT NULL, g Int64 NOT NULL, v Int64, PRIMARY KEY (id)
            );
            CREATE TABLE `/Root/ScopeInner` (
                id Int64 NOT NULL, g Int64 NOT NULL, v Int64, PRIMARY KEY (id)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(scheme.IsSuccess(), scheme.GetIssues().ToString());
        const auto load = [&](const TString& path,
                              std::initializer_list<std::tuple<i64, i64, std::optional<i64>>> data) {
            NYdb::TValueBuilder rows;
            rows.BeginList();
            for (const auto& [id, group, value] : data) {
                rows.AddListItem().BeginStruct()
                    .AddMember("id").Int64(id)
                    .AddMember("g").Int64(group)
                    .AddMember("v").OptionalInt64(value)
                    .EndStruct();
            }
            rows.EndList();
            const auto upsert = kikimr.GetTableClient().BulkUpsert(path, rows.Build()).GetValueSync();
            UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());
        };
        load("/Root/ScopeOuter", {{1, 1, 2}, {2, 1, 3}, {3, 2, 7}, {4, 3, std::nullopt}, {5, 4, 9}});
        load("/Root/ScopeInner", {{1, 1, 1}, {2, 1, 3}, {3, 2, 6}, {4, 3, std::nullopt}});

        auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        const auto execute = [&](const TString& query, const TString& expected) {
            const auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), expected);
        };
        const auto check = [&](const TString& scalarQuery, const TString& expected) {
            const TString query = TString(R"(
                PRAGMA YqlSelect = 'force';
                SELECT id FROM `/Root/ScopeOuter`
                WHERE g IN (
                    SELECT o.g AS g FROM `/Root/ScopeOuter` AS o
                    GROUP BY o.g
                    HAVING SUM(o.v) > (
            )") + scalarQuery + R"(
                    )
                )
                ORDER BY id;
            )";
            execute(query, expected);
        };

        // Inner totals are 4, 6, NULL; only outer totals 7 and 9 exceed MAX=6.
        // Neither the inner SUM nor MAX may be collected by the outer HAVING.
        check(R"(
            SELECT MAX(s) FROM (
                SELECT SUM(i.v) AS s FROM `/Root/ScopeInner` AS i GROUP BY i.g
            )
        )", "[[3];[5]]");

        // Correlation still belongs to the outer group. NULL-only and absent
        // inner groups must not pass; totals 5>4 and 7>6 select the other rows.
        check(R"(
            SELECT SUM(i.v) FROM `/Root/ScopeInner` AS i WHERE i.g = o.g
        )", "[[1];[2];[3]]");

        // The scalar's internal output must not collide when its shared
        // consumer reaches both sides of a later join.
        execute(R"(
            PRAGMA YqlSelect = 'force';
            $filtered = SELECT g, SUM(v) AS s FROM `/Root/ScopeOuter`
                GROUP BY g HAVING SUM(v) > (SELECT MAX(v) FROM `/Root/ScopeInner`);
            SELECT a.g AS a, b.g AS b FROM $filtered AS a CROSS JOIN $filtered AS b
            ORDER BY a, b;
        )", "[[2;2];[2;4];[4;2];[4;4]]");

        // Lambda argument declarations are bindings, not GROUP BY expressions.
        load("/Root/ScopeOuter", {{6, 5, -1}});
        execute(R"(
            PRAGMA YqlSelect = 'force';
            $null_negative = ($value) -> {
                RETURN CASE WHEN $value < 0 THEN NULL ELSE $value END;
            };
            SELECT g, $null_negative(SUM(v)) AS total FROM `/Root/ScopeOuter`
            GROUP BY g ORDER BY g;
        )", "[[1;[5]];[2;[7]];[3;#];[4;[9]];[5;#]]");
    }
}

} // namespace NKikimr::NKqp
