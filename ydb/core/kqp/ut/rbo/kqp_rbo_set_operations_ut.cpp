#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <yql/essentials/public/langver/yql_langver.h>

#include <initializer_list>
#include <optional>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpRboSetOperations) {
    Y_UNIT_TEST(ByNameAlignmentAndNullEqualSets) {
        NKikimrConfig::TAppConfig config;
        config.MutableTableServiceConfig()->SetEnableNewRBO(true);
        config.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        config.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        config.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        TKikimrRunner kikimr(TKikimrSettings(config).SetWithSampleTables(false));
        auto tableSession = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        const auto scheme = tableSession.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/SetValues` (
                id Int64 NOT NULL, v Int32, PRIMARY KEY (id)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(scheme.IsSuccess(), scheme.GetIssues().ToString());
        NYdb::TValueBuilder rows;
        rows.BeginList();
        i64 id = 0;
        // Left: NULL, NULL, 1, 1, 2. Right: NULL, 1, 1, 3.
        for (const auto value : std::initializer_list<std::optional<i32>>{
                 std::nullopt, std::nullopt, 1, 1, 2, std::nullopt, 1, 1, 3}) {
            rows.AddListItem().BeginStruct()
                .AddMember("id").Int64(++id)
                .AddMember("v").OptionalInt32(value)
                .EndStruct();
        }
        rows.EndList();
        const auto upsert = kikimr.GetTableClient().BulkUpsert("/Root/SetValues", rows.Build()).GetValueSync();
        UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());

        auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        const auto check = [&](const TString& query, const TString& expected,
                               std::initializer_list<NYdb::EPrimitiveType> types) {
            const auto result = session.ExecuteQuery("PRAGMA YqlSelect = 'force';\n" + query,
                NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            const auto& resultSet = result.GetResultSet(0);
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(resultSet), expected);
            UNIT_ASSERT_VALUES_EQUAL(resultSet.GetColumnsMeta().size(), types.size());
            size_t column = 0;
            for (const auto primitive : types) {
                NYdb::TTypeParser type(resultSet.GetColumnsMeta()[column++].Type);
                UNIT_ASSERT_VALUES_EQUAL(type.GetKind(), NYdb::TTypeParser::ETypeKind::Optional);
                type.OpenOptional();
                UNIT_ASSERT_VALUES_EQUAL(type.GetPrimitive(), primitive);
            }
        };
        const TString left = "SELECT v AS x FROM `/Root/SetValues` WHERE id <= 5";
        const TString right = "SELECT v AS x FROM `/Root/SetValues` WHERE id > 5";
        const auto int32 = NYdb::EPrimitiveType::Int32;

        // Column names, not positions, determine alignment and NULL padding.
        check(R"(
            SELECT a, b FROM (
                SELECT v AS a FROM `/Root/SetValues` WHERE id = 5
                UNION ALL
                SELECT v AS b FROM `/Root/SetValues` WHERE id = 9
            ) ORDER BY a, b;
        )", "[[#;[3]];[[2];#]]", {int32, int32});

        // Dots in public output names are not source aliases to discard.
        check(R"(
            SELECT q.`left.x` AS a, q.`right.x` AS b FROM (
                SELECT v AS `left.x` FROM `/Root/SetValues` WHERE id = 5
                UNION ALL
                SELECT v AS `right.x` FROM `/Root/SetValues` WHERE id = 9
            ) AS q ORDER BY a, b;
        )", "[[#;[3]];[[2];#]]", {int32, int32});

        // Both NULL and repeated non-NULL values compare as set elements.
        check("SELECT * FROM (" + left + " INTERSECT " + right + ") ORDER BY x;",
            "[[#];[[1]]]", {int32});
        check("SELECT * FROM (" + left + " EXCEPT " + right + ") ORDER BY x;",
            "[[[2]]]", {int32});
        check("SELECT * FROM (" + left + " UNION " + right + ") ORDER BY x;",
            "[[#];[[1]];[[2]];[[3]]]", {int32});

        // Non-null ordinary keys use semi/anti joins plus deduplication.
        const TString requiredLeft = "SELECT COALESCE(v, 0) AS x FROM `/Root/SetValues` WHERE id <= 5";
        const TString requiredRight = "SELECT COALESCE(v, 0) AS x FROM `/Root/SetValues` WHERE id > 5";
        check("SELECT IF(x >= 0, x, NULL) AS x FROM (" + requiredLeft + " INTERSECT " + requiredRight + ") ORDER BY x;",
            "[[[0]];[[1]]]", {int32});
        check("SELECT IF(x >= 0, x, NULL) AS x FROM (" + requiredLeft + " EXCEPT " + requiredRight + ") ORDER BY x;",
            "[[[2]]]", {int32});

        // Required Decimal is NOT an ordinary key: duplicate NaNs are peers.
        const TString nanRows = "SELECT Unwrap(CAST('nan' AS Decimal(15,4))) AS x FROM `/Root/SetValues` WHERE ";
        check("SELECT IF(c > 0, c, NULL) AS n FROM (SELECT COUNT(*) AS c FROM (" +
            nanRows + "id <= 2 INTERSECT " + nanRows + "id = 3));",
            "[[[1u]]]", {NYdb::EPrimitiveType::Uint64});

        // Only the padded all-NULL row is common when field names differ.
        check(R"(
            SELECT a, b FROM (
                SELECT v AS a FROM `/Root/SetValues` WHERE id <= 5
                INTERSECT
                SELECT v AS b FROM `/Root/SetValues` WHERE id > 5
            ) ORDER BY a, b;
        )", "[[#;#]]", {int32, int32});

        // Int64 (required) and Int32? share Optional<Int64>, without losing NULL.
        check(R"(
            SELECT * FROM (
                SELECT id AS x FROM `/Root/SetValues` WHERE id = 5
                UNION ALL
                SELECT v AS x FROM `/Root/SetValues` WHERE id IN (6, 9)
            ) ORDER BY x;
        )", "[[#];[[3]];[[5]]]", {NYdb::EPrimitiveType::Int64});
    }
}

} // namespace NKikimr::NKqp
