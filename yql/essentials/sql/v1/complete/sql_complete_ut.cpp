#include "sql_complete.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NSQLComplete;

Y_UNIT_TEST_SUITE(SqlCompleteTests) {
    using ECandidateKind::Keyword;

    TVector<TCandidate> Complete(ISqlCompletionEngine::TPtr& engine, TStringBuf prefix) {
        return engine->Complete({prefix}).Candidates;
    }

    Y_UNIT_TEST(Beginning) {
        TVector<TCandidate> expected = {
            {Keyword, "ALTER"},
            {Keyword, "ANALYZE"},
            {Keyword, "BACKUP"},
            {Keyword, "BATCH"},
            {Keyword, "COMMIT"},
            {Keyword, "CREATE"},
            {Keyword, "DECLARE"},
            {Keyword, "DEFINE"},
            {Keyword, "DELETE"},
            {Keyword, "DISCARD"},
            {Keyword, "DO"},
            {Keyword, "DROP"},
            {Keyword, "EVALUATE"},
            {Keyword, "EXPLAIN"},
            {Keyword, "EXPORT"},
            {Keyword, "FOR"},
            {Keyword, "FROM"},
            {Keyword, "GRANT"},
            {Keyword, "IF"},
            {Keyword, "IMPORT"},
            {Keyword, "INSERT"},
            {Keyword, "PARALLEL"},
            {Keyword, "PRAGMA"},
            {Keyword, "PROCESS"},
            {Keyword, "REDUCE"},
            {Keyword, "REPLACE"},
            {Keyword, "RESTORE"},
            {Keyword, "REVOKE"},
            {Keyword, "ROLLBACK"},
            {Keyword, "SELECT"},
            {Keyword, "SHOW"},
            {Keyword, "UPDATE"},
            {Keyword, "UPSERT"},
            {Keyword, "USE"},
            {Keyword, "VALUES"},
        };

        auto engine = MakeSqlCompletionEngine();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {""}), expected);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {" "}), expected);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"  "}), expected);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {";"}), expected);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"; "}), expected);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {" ; "}), expected);
    }

    Y_UNIT_TEST(Alter) {
        TVector<TCandidate> expected = {
            {Keyword, "ASYNC"},
            {Keyword, "BACKUP"},
            {Keyword, "DATABASE"},
            {Keyword, "EXTERNAL"},
            {Keyword, "GROUP"},
            {Keyword, "OBJECT"},
            {Keyword, "RESOURCE"},
            {Keyword, "SEQUENCE"},
            {Keyword, "TABLE"},
            {Keyword, "TABLESTORE"},
            {Keyword, "TOPIC"},
            {Keyword, "TRANSFER"},
            {Keyword, "USER"},
        };

        auto engine = MakeSqlCompletionEngine();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"ALTER "}), expected);
    }

    Y_UNIT_TEST(Create) {
        TVector<TCandidate> expected = {
            {Keyword, "ASYNC"},
            {Keyword, "BACKUP"},
            {Keyword, "EXTERNAL"},
            {Keyword, "GROUP"},
            {Keyword, "OBJECT"},
            {Keyword, "OR"},
            {Keyword, "RESOURCE"},
            {Keyword, "TABLE"},
            {Keyword, "TABLESTORE"},
            {Keyword, "TEMP"},
            {Keyword, "TEMPORARY"},
            {Keyword, "TOPIC"},
            {Keyword, "TRANSFER"},
            {Keyword, "USER"},
            {Keyword, "VIEW"},
        };

        auto engine = MakeSqlCompletionEngine();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"CREATE "}), expected);
    }

    Y_UNIT_TEST(Delete) {
        TVector<TCandidate> expected = {
            {Keyword, "FROM"},
        };

        auto engine = MakeSqlCompletionEngine();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"DELETE "}), expected);
    }

    Y_UNIT_TEST(Drop) {
        TVector<TCandidate> expected = {
            {Keyword, "ASYNC"},
            {Keyword, "BACKUP"},
            {Keyword, "EXTERNAL"},
            {Keyword, "GROUP"},
            {Keyword, "OBJECT"},
            {Keyword, "RESOURCE"},
            {Keyword, "TABLE"},
            {Keyword, "TABLESTORE"},
            {Keyword, "TOPIC"},
            {Keyword, "TRANSFER"},
            {Keyword, "USER"},
            {Keyword, "VIEW"},
        };

        auto engine = MakeSqlCompletionEngine();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"DROP "}), expected);
    }

    Y_UNIT_TEST(Explain) {
        TVector<TCandidate> expected = {
            {Keyword, "ALTER"},
            {Keyword, "ANALYZE"},
            {Keyword, "BACKUP"},
            {Keyword, "BATCH"},
            {Keyword, "COMMIT"},
            {Keyword, "CREATE"},
            {Keyword, "DECLARE"},
            {Keyword, "DEFINE"},
            {Keyword, "DELETE"},
            {Keyword, "DISCARD"},
            {Keyword, "DO"},
            {Keyword, "DROP"},
            {Keyword, "EVALUATE"},
            {Keyword, "EXPORT"},
            {Keyword, "FOR"},
            {Keyword, "FROM"},
            {Keyword, "GRANT"},
            {Keyword, "IF"},
            {Keyword, "IMPORT"},
            {Keyword, "INSERT"},
            {Keyword, "PARALLEL"},
            {Keyword, "PRAGMA"},
            {Keyword, "PROCESS"},
            {Keyword, "QUERY"},
            {Keyword, "REDUCE"},
            {Keyword, "REPLACE"},
            {Keyword, "RESTORE"},
            {Keyword, "REVOKE"},
            {Keyword, "ROLLBACK"},
            {Keyword, "SELECT"},
            {Keyword, "SHOW"},
            {Keyword, "UPDATE"},
            {Keyword, "UPSERT"},
            {Keyword, "USE"},
            {Keyword, "VALUES"},
        };

        auto engine = MakeSqlCompletionEngine();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"EXPLAIN "}), expected);
    }

    Y_UNIT_TEST(Grant) {
        TVector<TCandidate> expected = {
            {Keyword, "ALL"},
            {Keyword, "ALTER"},
            {Keyword, "CONNECT"},
            {Keyword, "CREATE"},
            {Keyword, "DESCRIBE"},
            {Keyword, "DROP"},
            {Keyword, "ERASE"},
            {Keyword, "FULL"},
            {Keyword, "GRANT"},
            {Keyword, "INSERT"},
            {Keyword, "LIST"},
            {Keyword, "MANAGE"},
            {Keyword, "MODIFY"},
            {Keyword, "REMOVE"},
            {Keyword, "SELECT"},
            {Keyword, "UPDATE"},
            {Keyword, "USE"},
        };

        auto engine = MakeSqlCompletionEngine();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"GRANT "}), expected);
    }

    Y_UNIT_TEST(Insert) {
        TVector<TCandidate> expected = {
            {Keyword, "INTO"},
            {Keyword, "OR"},
        };

        auto engine = MakeSqlCompletionEngine();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"INSERT "}), expected);
    }

    Y_UNIT_TEST(Pragma) {
        TVector<TCandidate> expected = {
            {Keyword, "ANSI"},
            {Keyword, "CALLABLE"},
            {Keyword, "DICT"},
            {Keyword, "ENUM"},
            {Keyword, "FLOW"},
            {Keyword, "LIST"},
            {Keyword, "OPTIONAL"},
            {Keyword, "RESOURCE"},
            {Keyword, "SET"},
            {Keyword, "STRUCT"},
            {Keyword, "TAGGED"},
            {Keyword, "TUPLE"},
            {Keyword, "VARIANT"},
        };

        auto engine = MakeSqlCompletionEngine();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"PRAGMA "}), expected);
    }

    Y_UNIT_TEST(Select) {
        TVector<TCandidate> expected = {
            {Keyword, "ALL"},
            {Keyword, "BITCAST"},
            {Keyword, "CALLABLE"},
            {Keyword, "CASE"},
            {Keyword, "CAST"},
            {Keyword, "CURRENT_DATE"},
            {Keyword, "CURRENT_TIME"},
            {Keyword, "CURRENT_TIMESTAMP"},
            {Keyword, "DICT"},
            {Keyword, "DISTINCT"},
            {Keyword, "EMPTY_ACTION"},
            {Keyword, "ENUM"},
            {Keyword, "EXISTS"},
            {Keyword, "FALSE"},
            {Keyword, "FLOW"},
            {Keyword, "JSON_EXISTS"},
            {Keyword, "JSON_QUERY"},
            {Keyword, "JSON_VALUE"},
            {Keyword, "LIST"},
            {Keyword, "NOT"},
            {Keyword, "NULL"},
            {Keyword, "OPTIONAL"},
            {Keyword, "RESOURCE"},
            {Keyword, "SET"},
            {Keyword, "STREAM"},
            {Keyword, "STRUCT"},
            {Keyword, "TAGGED"},
            {Keyword, "TRUE"},
            {Keyword, "TUPLE"},
            {Keyword, "VARIANT"},
        };

        auto engine = MakeSqlCompletionEngine();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"SELECT "}), expected);
    }

    Y_UNIT_TEST(Upsert) {
        TVector<TCandidate> expected = {
            {Keyword, "INTO"},
            {Keyword, "OBJECT"},
        };

        auto engine = MakeSqlCompletionEngine();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"UPSERT "}), expected);
    }

    Y_UNIT_TEST(UTF8Wide) {
        auto engine = MakeSqlCompletionEngine();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"\xF0\x9F\x98\x8A"}).size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"编码"}).size(), 0);
    }

    Y_UNIT_TEST(WordBreak) {
        auto engine = MakeSqlCompletionEngine();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"SELECT ("}).size(), 28);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"SELECT (1)"}).size(), 30);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"SELECT 1;"}).size(), 35);
    }

    Y_UNIT_TEST(Typing) {
        const auto queryUtf16 = TUtf16String::FromUtf8(
            "SELECT \n"
            "  123467, \"Hello, {name}! 编码\"}, \n"
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

    Y_UNIT_TEST(CaseInsensitivity) {
        TVector<TCandidate> expected = {
            {Keyword, "SELECT"},
        };

        auto engine = MakeSqlCompletionEngine();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "se"), expected);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "sE"), expected);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "Se"), expected);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SE"), expected);
    }
} // Y_UNIT_TEST_SUITE(SqlCompleteTests)
