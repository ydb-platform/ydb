#include <ydb/public/lib/ydb_cli/dump/util/query_utils.h>

#include <yql/essentials/public/issue/yql_issue.h>

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(TQueryUtils) {
    Y_UNIT_TEST(SplitCreateTableAndAlterIndex) {
        const TString query = R"(
            CREATE TABLE `/MyRoot/Table` (
                key Uint32 NOT NULL,
                a Int32,
                g Int32 GENERATED ALWAYS AS (a + 1) STORED,
                PRIMARY KEY (key),
                INDEX by_a GLOBAL ON (a),
                INDEX by_key LOCAL USING bloom_filter ON (key)
            );
            ALTER TABLE `/MyRoot/Table` ALTER INDEX by_a SET (
                AUTO_PARTITIONING_BY_LOAD = ENABLED,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10
            );
        )";

        TVector<TString> statements;
        NYql::TIssues issues;
        UNIT_ASSERT_C(NYdb::NDump::SplitSqlStatements(query, statements, issues), issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(statements.size(), 2);
        UNIT_ASSERT_STRING_CONTAINS(statements[0], "CREATE TABLE");
        UNIT_ASSERT(!statements[0].Contains("ALTER TABLE"));
        UNIT_ASSERT_STRING_CONTAINS(statements[1], "ALTER TABLE");
        UNIT_ASSERT(!statements[1].Contains("CREATE TABLE"));
        UNIT_ASSERT_STRING_CONTAINS(statements[0], "key Uint32 NOT NULL");
        UNIT_ASSERT_STRING_CONTAINS(statements[1], "AUTO_PARTITIONING_BY_LOAD = ENABLED");
    }
}
