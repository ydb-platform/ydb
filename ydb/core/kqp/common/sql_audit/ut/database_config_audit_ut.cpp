#include <ydb/core/kqp/common/sql_audit/database_config_audit.h>

#include <yql/essentials/sql/settings/translation_settings.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpDatabaseConfigAuditSql) {
    Y_UNIT_TEST(ExplicitSyntaxV1) {
        const auto changes = CollectDatabaseConfigAuditSettingsFromSql(R"sql(--!syntax_v1
ALTER DATABASE `/Root/db` RESET (ENABLE_CLIENT_MODIFICATIONS, ENABLE_CLIENT_MODIFICATIONS);
)sql");
        UNIT_ASSERT_VALUES_EQUAL(changes.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(changes[0].Changes, "ENABLE_CLIENT_MODIFICATIONS=INHERIT, ENABLE_CLIENT_MODIFICATIONS=INHERIT");
    }

    Y_UNIT_TEST(DuplicateSetAndReset) {
        const auto changes = CollectDatabaseConfigAuditSettingsFromSql(R"sql(
ALTER DATABASE `/Root/a` SET (ENABLE_CLIENT_MODIFICATIONS = false, enable_client_modifications = true);
ALTER DATABASE `/Root/b` RESET (ENABLE_CLIENT_MODIFICATIONS, `ENABLE_CLIENT_MODIFICATIONS`);
)sql");
        UNIT_ASSERT_VALUES_EQUAL(changes.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(changes[0].Database, "`/Root/a`");
        UNIT_ASSERT(changes[0].DatabaseIsExpression);
        UNIT_ASSERT_VALUES_EQUAL(changes[0].Changes, "ENABLE_CLIENT_MODIFICATIONS=false, enable_client_modifications=true");
        UNIT_ASSERT_VALUES_EQUAL(changes[1].Changes, "ENABLE_CLIENT_MODIFICATIONS=INHERIT, `ENABLE_CLIENT_MODIFICATIONS`=INHERIT");
    }

    Y_UNIT_TEST(UnknownAndInvalidValues) {
        const auto changes = CollectDatabaseConfigAuditSettingsFromSql(R"sql(
ALTER DATABASE db SET (`UNKNOWN_FLAG` = 1, ENABLE_CLIENT_MODIFICATIONS = 'false', MAX_SHARDS = 10);
)sql");
        UNIT_ASSERT_VALUES_EQUAL(changes.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(changes[0].Database, "db");
        UNIT_ASSERT_VALUES_EQUAL(changes[0].Changes, "`UNKNOWN_FLAG`=1, ENABLE_CLIENT_MODIFICATIONS='false'");
    }

    Y_UNIT_TEST(NestedInlineActionsPreserveStatementOrder) {
        const auto changes = CollectDatabaseConfigAuditSettingsFromSql(R"sql(
ALTER DATABASE `/Root/first` SET (ENABLE_CLIENT_MODIFICATIONS = true);
DO BEGIN
    ALTER DATABASE `/Root/second` RESET (ENABLE_CLIENT_MODIFICATIONS, ENABLE_CLIENT_MODIFICATIONS);
    DO BEGIN
        ALTER DATABASE `/Root/third` SET (ENABLE_CLIENT_MODIFICATIONS = false, ENABLE_CLIENT_MODIFICATIONS = true);
    END DO;
END DO;
ALTER DATABASE `/Root/fourth` RESET (ENABLE_CLIENT_MODIFICATIONS);
)sql");
        UNIT_ASSERT_VALUES_EQUAL(changes.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(changes[0].Database, "`/Root/first`");
        UNIT_ASSERT_VALUES_EQUAL(changes[1].Database, "`/Root/second`");
        UNIT_ASSERT_VALUES_EQUAL(changes[1].Changes,
            "ENABLE_CLIENT_MODIFICATIONS=INHERIT, ENABLE_CLIENT_MODIFICATIONS=INHERIT");
        UNIT_ASSERT_VALUES_EQUAL(changes[2].Database, "`/Root/third`");
        UNIT_ASSERT_VALUES_EQUAL(changes[2].Changes,
            "ENABLE_CLIENT_MODIFICATIONS=false, ENABLE_CLIENT_MODIFICATIONS=true");
        UNIT_ASSERT_VALUES_EQUAL(changes[3].Database, "`/Root/fourth`");
    }

    Y_UNIT_TEST(ConditionalAndLoopBodies) {
        const auto changes = CollectDatabaseConfigAuditSettingsFromSql(R"sql(
EVALUATE IF true DO BEGIN
    ALTER DATABASE `/Root/if` RESET (ENABLE_CLIENT_MODIFICATIONS, ENABLE_CLIENT_MODIFICATIONS);
END DO ELSE DO BEGIN
    ALTER DATABASE `/Root/else` SET (ENABLE_CLIENT_MODIFICATIONS = 'invalid');
END DO;
EVALUATE FOR $item IN [1] DO BEGIN
    ALTER DATABASE `/Root/for` RESET (UNKNOWN_FLAG);
END DO ELSE DO BEGIN
    ALTER DATABASE `/Root/empty` SET (ENABLE_CLIENT_MODIFICATIONS = false);
END DO;
)sql");
        UNIT_ASSERT_VALUES_EQUAL(changes.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(changes[0].Database, "`/Root/if`");
        UNIT_ASSERT_VALUES_EQUAL(changes[1].Database, "`/Root/else`");
        UNIT_ASSERT_VALUES_EQUAL(changes[2].Database, "`/Root/for`");
        UNIT_ASSERT_VALUES_EQUAL(changes[3].Database, "`/Root/empty`");
    }

    Y_UNIT_TEST(ExcludesInertDefinitionsWithinExecutableBodies) {
        const auto changes = CollectDatabaseConfigAuditSettingsFromSql(R"sql(
DEFINE ACTION $unused() AS
    ALTER DATABASE `/Root/unused` SET (ENABLE_CLIENT_MODIFICATIONS = false);
END DEFINE;
DO BEGIN
    DEFINE ACTION $nested_unused() AS
        DO BEGIN
            ALTER DATABASE `/Root/nested_unused` RESET (ENABLE_CLIENT_MODIFICATIONS);
        END DO;
    END DEFINE;
    SELECT 'ALTER DATABASE `/Root/string` RESET (ENABLE_CLIENT_MODIFICATIONS);';
    -- ALTER DATABASE `/Root/comment` RESET (ENABLE_CLIENT_MODIFICATIONS);
    ALTER DATABASE `/Root/executed` RESET (ENABLE_CLIENT_MODIFICATIONS);
END DO;
)sql");
        UNIT_ASSERT_VALUES_EQUAL(changes.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(changes[0].Database, "`/Root/executed`");
    }

    Y_UNIT_TEST(AnsiIdentifiers) {
        const auto changes = CollectDatabaseConfigAuditSettingsFromSql(R"sql(--!ansi_lexer
ALTER DATABASE "/Root/db" RESET ("ENABLE_CLIENT_MODIFICATIONS", "ENABLE_CLIENT_MODIFICATIONS");
)sql");
        UNIT_ASSERT_VALUES_EQUAL(changes.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(changes[0].Database, "\"/Root/db\"");
        UNIT_ASSERT_VALUES_EQUAL(changes[0].Changes, "\"ENABLE_CLIENT_MODIFICATIONS\"=INHERIT, \"ENABLE_CLIENT_MODIFICATIONS\"=INHERIT");
    }

    Y_UNIT_TEST(ExcludesCommentsStringsLegacyAndMalformedSql) {
        for (const auto* query : {
            "-- ALTER DATABASE db RESET (ENABLE_CLIENT_MODIFICATIONS);\nSELECT 1;",
            "SELECT 'ALTER DATABASE db SET (ENABLE_CLIENT_MODIFICATIONS = false);';",
            "ALTER DATABASE db SET (max_shards = 1, `MAX_PATHS` = 2);",
            "ALTER DATABASE db RESET ('MAX_SHARDS', \"MAX_PATHS\");",
            "ALTER DATABASE db OWNER TO user;",
            "ALTER DATABASE db SET (ENABLE_CLIENT_MODIFICATIONS = );",
            "--!syntax_pg\nALTER DATABASE db RESET (ENABLE_CLIENT_MODIFICATIONS);",
            "--!syntax_v0\nALTER DATABASE db RESET (ENABLE_CLIENT_MODIFICATIONS);",
            "\xff ALTER DATABASE db RESET (ENABLE_CLIENT_MODIFICATIONS);",
        }) {
            UNIT_ASSERT_C(CollectDatabaseConfigAuditSettingsFromSql(query).empty(), query);
        }
    }

    Y_UNIT_TEST(BoundsParserDepthWithoutReplacingQueryFailure) {
        TString query = "--!syntax_v1\n";
        constexpr size_t depth = 2 * NSQLTranslation::SQL_MAX_PARSE_TREE_DEPTH;
        for (size_t i = 0; i < depth; ++i) {
            query += "SELECT 1 FROM (";
        }
        query += "SELECT 1";
        query.append(depth, ')');
        query += "; ALTER DATABASE db RESET (ENABLE_CLIENT_MODIFICATIONS);";
        static_assert(noexcept(CollectDatabaseConfigAuditSettingsFromSql(query)));
        UNIT_ASSERT(CollectDatabaseConfigAuditSettingsFromSql(query).empty());
    }
}

} // namespace NKikimr::NKqp
