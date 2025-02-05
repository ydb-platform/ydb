#include "sql_complete.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NSQLComplete;

Y_UNIT_TEST_SUITE(SqlCompleteTests) {
    TCompletionContext Complete(ISqlCompletionEngine::TPtr& engine, TCompletionInput input) {
        auto context = engine->Complete(input);
        Sort(context.Keywords);
        return context;
    }

    Y_UNIT_TEST(Beginning) {
        TVector<std::string> expected = {
            "ALTER",
            "ANALYZE",
            "BACKUP",
            "BATCH",
            "COMMIT",
            "CREATE",
            "DECLARE",
            "DEFINE",
            "DELETE",
            "DISCARD",
            "DO",
            "DROP",
            "EVALUATE",
            "EXPLAIN",
            "EXPORT",
            "FOR",
            "FROM",
            "GRANT",
            "IF",
            "IMPORT",
            "INSERT",
            "PARALLEL",
            "PRAGMA",
            "PROCESS",
            "REDUCE",
            "REPLACE",
            "RESTORE",
            "REVOKE",
            "ROLLBACK",
            "SELECT",
            "UPDATE",
            "UPSERT",
            "USE",
            "VALUES",
        };

        auto engine = MakeSqlCompletionEngine();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {""}).Keywords, expected);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {" "}).Keywords, expected);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"  "}).Keywords, expected);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {";"}).Keywords, expected);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"; "}).Keywords, expected);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {" ; "}).Keywords, expected);
    }

    Y_UNIT_TEST(Alter) {
        TVector<std::string> expected = {
            "ASYNC",
            "BACKUP",
            "EXTERNAL",
            "GROUP",
            "OBJECT",
            "RESOURCE",
            "SEQUENCE",
            "TABLE",
            "TABLESTORE",
            "TOPIC",
            "TRANSFER",
            "USER",
        };

        auto engine = MakeSqlCompletionEngine();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"ALTER "}).Keywords, expected);
    }

    Y_UNIT_TEST(Create) {
        TVector<std::string> expected = {
            "ASYNC",
            "BACKUP",
            "EXTERNAL",
            "GROUP",
            "OBJECT",
            "OR",
            "RESOURCE",
            "TABLE",
            "TABLESTORE",
            "TEMP",
            "TEMPORARY",
            "TOPIC",
            "TRANSFER",
            "USER",
            "VIEW",
        };

        auto engine = MakeSqlCompletionEngine();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"CREATE "}).Keywords, expected);
    }

    Y_UNIT_TEST(Delete) {
        TVector<std::string> expected = {
            "FROM",
        };

        auto engine = MakeSqlCompletionEngine();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"DELETE "}).Keywords, expected);
    }

    Y_UNIT_TEST(Drop) {
        TVector<std::string> expected = {
            "ASYNC",
            "BACKUP",
            "EXTERNAL",
            "GROUP",
            "OBJECT",
            "RESOURCE",
            "TABLE",
            "TABLESTORE",
            "TOPIC",
            "TRANSFER",
            "USER",
            "VIEW",
        };

        auto engine = MakeSqlCompletionEngine();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"DROP "}).Keywords, expected);
    }

    Y_UNIT_TEST(Explain) {
        TVector<std::string> expected = {
            "ALTER",
            "ANALYZE",
            "BACKUP",
            "BATCH",
            "COMMIT",
            "CREATE",
            "DECLARE",
            "DEFINE",
            "DELETE",
            "DISCARD",
            "DO",
            "DROP",
            "EVALUATE",
            "EXPORT",
            "FOR",
            "FROM",
            "GRANT",
            "IF",
            "IMPORT",
            "INSERT",
            "PARALLEL",
            "PRAGMA",
            "PROCESS",
            "QUERY",
            "REDUCE",
            "REPLACE",
            "RESTORE",
            "REVOKE",
            "ROLLBACK",
            "SELECT",
            "UPDATE",
            "UPSERT",
            "USE",
            "VALUES",
        };

        auto engine = MakeSqlCompletionEngine();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"EXPLAIN "}).Keywords, expected);
    }

    Y_UNIT_TEST(Grant) {
        TVector<std::string> expected = {
            "ALL",
            "ALTER",
            "CONNECT",
            "CREATE",
            "DESCRIBE",
            "DROP",
            "ERASE",
            "FULL",
            "GRANT",
            "INSERT",
            "LIST",
            "MANAGE",
            "MODIFY",
            "REMOVE",
            "SELECT",
            "UPDATE",
            "USE",
        };

        auto engine = MakeSqlCompletionEngine();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"GRANT "}).Keywords, expected);
    }

    Y_UNIT_TEST(Insert) {
        TVector<std::string> expected = {
            "INTO",
            "OR",
        };

        auto engine = MakeSqlCompletionEngine();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"INSERT "}).Keywords, expected);
    }

    Y_UNIT_TEST(Pragma) {
        TVector<std::string> expected = {
            "ANSI",
            "CALLABLE",
            "DICT",
            "ENUM",
            "FLOW",
            "LIST",
            "OPTIONAL",
            "RESOURCE",
            "SET",
            "STRUCT",
            "TAGGED",
            "TUPLE",
            "VARIANT",
        };

        auto engine = MakeSqlCompletionEngine();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"PRAGMA "}).Keywords, expected);
    }

    Y_UNIT_TEST(Select) {
        TVector<std::string> expected = {
            "ALL",
            "BITCAST",
            "CALLABLE",
            "CASE",
            "CAST",
            "CURRENT_DATE",
            "CURRENT_TIME",
            "CURRENT_TIMESTAMP",
            "DICT",
            "DISTINCT",
            "EMPTY_ACTION",
            "ENUM",
            "EXISTS",
            "FALSE",
            "FLOW",
            "JSON_EXISTS",
            "JSON_QUERY",
            "JSON_VALUE",
            "LIST",
            "NOT",
            "NULL",
            "OPTIONAL",
            "RESOURCE",
            "SET",
            "STREAM",
            "STRUCT",
            "TAGGED",
            "TRUE",
            "TUPLE",
            "VARIANT",
        };

        auto engine = MakeSqlCompletionEngine();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"SELECT "}).Keywords, expected);
    }

    Y_UNIT_TEST(Upsert) {
        TVector<std::string> expected = {
            "INTO",
            "OBJECT",
        };

        auto engine = MakeSqlCompletionEngine();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"UPSERT "}).Keywords, expected);
    }

    Y_UNIT_TEST(UTF8Wide) {
        auto engine = MakeSqlCompletionEngine();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"\xF0\x9F\x98\x8A"}).Keywords.size(), 34);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"编码"}).Keywords.size(), 34);
    }

    Y_UNIT_TEST(WordBreak) {
        auto engine = MakeSqlCompletionEngine();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"SELECT ("}).Keywords.size(), 28);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"SELECT (1)"}).Keywords.size(), 30);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"SELECT 1;"}).Keywords.size(), 34);
    }

    Y_UNIT_TEST(Typing) {
        const auto queryUtf16 = TUtf16String::FromUtf8(
            "SELECT \n"
            "  123467, \"Hello, {name}! 编码\", \n"
            "  (1 + (5 * 1 / 0)), MIN(identifier), \n"
            "  Bool(field), Math::Sin(var) \n"
            "FROM `local/test/space/table` JOIN test;");

        auto engine = MakeSqlCompletionEngine();

        for (std::size_t size = 0; size <= queryUtf16.size(); ++size) {
            const TWtringBuf prefixUtf16(queryUtf16, 0, size);
            auto completion = engine->Complete({TString::FromUtf16(prefixUtf16)});
            Y_DO_NOT_OPTIMIZE_AWAY(completion);
        }
    }

} // Y_UNIT_TEST_SUITE(SqlCompleteTests)
