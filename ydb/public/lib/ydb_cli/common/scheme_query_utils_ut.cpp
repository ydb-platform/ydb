#include <ydb/public/lib/ydb_cli/common/scheme_query_utils.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NConsoleClient {

Y_UNIT_TEST_SUITE(SchemeQueryUtils) {

    Y_UNIT_TEST(CreateTable) {
        UNIT_ASSERT(LooksLikeSchemeQuery("CREATE TABLE t (id Uint64, PRIMARY KEY (id));"));
        UNIT_ASSERT(LooksLikeSchemeQuery("create table t (id Uint64);"));
    }

    Y_UNIT_TEST(AlterDropTruncate) {
        UNIT_ASSERT(LooksLikeSchemeQuery("ALTER TABLE `/local/t` ADD COLUMN x Int32;"));
        UNIT_ASSERT(LooksLikeSchemeQuery("DROP TABLE `/local/t`;"));
        UNIT_ASSERT(LooksLikeSchemeQuery("TRUNCATE TABLE `/local/t`;"));
    }

    Y_UNIT_TEST(ExplainSchemeQuery) {
        UNIT_ASSERT(LooksLikeSchemeQuery("EXPLAIN CREATE TABLE t (id Uint64, PRIMARY KEY (id));"));
        UNIT_ASSERT(LooksLikeSchemeQuery("EXPLAIN QUERY PLAN ALTER TABLE t DROP COLUMN x;"));
    }

    Y_UNIT_TEST(DmlNotScheme) {
        UNIT_ASSERT(!LooksLikeSchemeQuery("SELECT 1;"));
        UNIT_ASSERT(!LooksLikeSchemeQuery("UPSERT INTO t (id) VALUES (1);"));
        UNIT_ASSERT(!LooksLikeSchemeQuery("EXPLAIN SELECT 1;"));
    }

    Y_UNIT_TEST(ShowCreateAllowed) {
        UNIT_ASSERT(!LooksLikeSchemeQuery("SHOW CREATE TABLE `/local/t`;"));
        UNIT_ASSERT(!LooksLikeSchemeQuery("SHOW CREATE VIEW `/local/v`;"));
        UNIT_ASSERT(!IsExcludedSchemeQueryCompletionKeyword("SHOW CREATE", ""));
        UNIT_ASSERT(!IsExcludedSchemeQueryCompletionKeyword("CREATE", "SHOW "));
    }

    Y_UNIT_TEST(MultiStatementWithDdl) {
        UNIT_ASSERT(LooksLikeSchemeQuery(
            "SELECT 1; CREATE TABLE t (id Uint64, PRIMARY KEY (id))"));
        UNIT_ASSERT(!LooksLikeSchemeQuery("SELECT 1; SELECT 2"));
    }

    Y_UNIT_TEST(CompletionContextAndKeywords) {
        UNIT_ASSERT_VALUES_EQUAL(GetCurrentStatementPrefix("SELECT 1; CRE"), " CRE");
        UNIT_ASSERT(IsSchemeQueryCompletionContext("ALTER "));
        UNIT_ASSERT(!IsSchemeQueryCompletionContext("SELECT "));
        UNIT_ASSERT(IsExcludedSchemeQueryCompletionKeyword("CREATE", ""));
        UNIT_ASSERT(!IsExcludedSchemeQueryCompletionKeyword("SELECT", ""));
        UNIT_ASSERT(IsExcludedSchemeQueryCompletionKeyword("TABLE", "ALTER "));
    }

} // Y_UNIT_TEST_SUITE

} // namespace NYdb::NConsoleClient
