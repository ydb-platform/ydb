#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <yql/essentials/public/langver/yql_langver.h>

#include <initializer_list>
#include <optional>
#include <string>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpRboRankContract) {
    Y_UNIT_TEST(ForcedYqlSelectDecimalPeers) {
        for (const bool enableRbo : {false, true}) {
            NKikimrConfig::TAppConfig config;
            // Both paths use native ANSI Rank semantics, including NULL/NaN peers.
            config.MutableTableServiceConfig()->SetEnableNewRBO(enableRbo);
            config.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
            config.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
            config.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
            TKikimrRunner kikimr(TKikimrSettings(config).SetWithSampleTables(false));
            auto tableSession = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
            const auto scheme = tableSession
                                    .ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/RankValues` (
                id Int64 NOT NULL, v String, PRIMARY KEY (id)
            );
        )")
                                    .GetValueSync();
            UNIT_ASSERT_C(scheme.IsSuccess(), scheme.GetIssues().ToString());
            NYdb::TValueBuilder rows;
            rows.BeginList();
            i64 id = 0;
            for (const auto& value : std::initializer_list<std::optional<std::string>>{"nan", "nan", std::nullopt,
                                                                                       std::nullopt, "1", "1", "2"}) {
                rows.AddListItem()
                    .BeginStruct()
                    .AddMember("id")
                    .Int64(++id)
                    .AddMember("v")
                    .OptionalString(value)
                    .EndStruct();
            }
            rows.EndList();
            const auto upsert = kikimr.GetTableClient().BulkUpsert("/Root/RankValues", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());

            auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
            const auto check = [&](const TString& input, const TString& order, const TString& expected) {
                const TString query = "PRAGMA YqlSelect = 'force';\n"
                                      "SELECT id, RANK() OVER w AS r1, RANK() OVER w AS r2 FROM (" +
                                      input +
                                      ") "
                                      "WINDOW w AS (ORDER BY d " +
                                      order + ") ORDER BY id;";
                const auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                const auto& resultSet = result.GetResultSet(0);
                UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(resultSet), expected);
                for (size_t column = 1; column != 3; ++column) {
                    NYdb::TTypeParser type(resultSet.GetColumnsMeta()[column].Type);
                    UNIT_ASSERT_VALUES_EQUAL(type.GetPrimitive(), NYdb::EPrimitiveType::Uint64);
                }
            };
            const TString nullable = "SELECT id, CAST(v AS Decimal(15,4)) AS d FROM `/Root/RankValues`";
            const TString required = "SELECT id, Unwrap(CAST(v AS Decimal(15,4))) AS d "
                                     "FROM `/Root/RankValues` WHERE v IS NOT NULL";

            // ANSI peers include duplicate NaNs and NULLs, with competition gaps.
            // Repeated uses of one named definition share the same rank result.
            check(nullable, "ASC",
                  "[[1;6u;6u];[2;6u;6u];[3;1u;1u];[4;1u;1u];[5;3u;3u];[6;3u;3u];[7;"
                  "5u;5u]]");
            check(nullable, "DESC ROWS BETWEEN CURRENT ROW AND CURRENT ROW",
                  "[[1;1u;1u];[2;1u;1u];[3;6u;6u];[4;6u;6u];[5;4u;4u];[6;4u;4u];[7;"
                  "3u;3u]]");
            // Non-null keys match the original q49 model's admitted key type.
            check(required, "ASC", "[[1;4u;4u];[2;4u;4u];[5;1u;1u];[6;1u;1u];[7;3u;3u]]");
            check(required, "DESC", "[[1;1u;1u];[2;1u;1u];[5;4u;4u];[6;4u;4u];[7;3u;3u]]");

            const auto partitioned = session
                                         .ExecuteQuery(R"(
            PRAGMA YqlSelect = 'force';
            SELECT id, RANK() OVER w AS r1, RANK() OVER w AS r2
            FROM (
                SELECT id, CAST(v AS Decimal(15,4)) AS d,
                    IF(id <= 4, 'a', 'b') AS p FROM `/Root/RankValues`
            )
            WINDOW w AS (PARTITION BY p ORDER BY d ASC, id DESC)
            ORDER BY id;
        )",
                                                       NYdb::NQuery::TTxControl::NoTx())
                                         .ExtractValueSync();
            UNIT_ASSERT_C(partitioned.IsSuccess(), partitioned.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(partitioned.GetResultSet(0)),
                                     "[[1;4u;4u];[2;3u;3u];[3;2u;2u];[4;1u;1u];[5;2u;"
                                     "2u];[6;1u;1u];[7;3u;3u]]");

            const auto mixed = session
                                   .ExecuteQuery(R"(
            PRAGMA YqlSelect = 'force';
            SELECT id,
                AVG(SUM(d)) OVER (PARTITION BY p ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) IS NOT NULL AS present,
                RANK() OVER (PARTITION BY p ORDER BY SUM(d)) AS r
            FROM (
                SELECT id, CAST(v AS Decimal(35,2)) AS d,
                    IF(id > 0, 'p', CAST(NULL AS String)) AS p
                FROM `/Root/RankValues` WHERE id >= 5
            ) GROUP BY id, p ORDER BY id;
        )",
                                                 NYdb::NQuery::TTxControl::NoTx())
                                   .ExtractValueSync();
            UNIT_ASSERT_C(mixed.IsSuccess(), mixed.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(mixed.GetResultSet(0)),
                                     "[[5;%true;1u];[6;%true;1u];[7;%true;3u]]");
        }
    }
}

} // namespace NKikimr::NKqp
