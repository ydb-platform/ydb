#include "yql_complete.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb::NConsoleClient;

Y_UNIT_TEST_SUITE(YqlCompleteTests) {
    Y_UNIT_TEST(Beginning) {
        TVector<TCandidate> expected = {
            {ECandidateKind::Keyword, "ALTER"},
            {ECandidateKind::Keyword, "ANALYZE"},
            {ECandidateKind::Keyword, "BACKUP"},
            {ECandidateKind::Keyword, "COMMIT"},
            {ECandidateKind::Keyword, "CREATE"},
            {ECandidateKind::Keyword, "DECLARE"},
            {ECandidateKind::Keyword, "DEFINE"},
            {ECandidateKind::Keyword, "DELETE"},
            {ECandidateKind::Keyword, "DISCARD"},
            {ECandidateKind::Keyword, "DO"},
            {ECandidateKind::Keyword, "DROP"},
            {ECandidateKind::Keyword, "EVALUATE"},
            {ECandidateKind::Keyword, "EXPLAIN"},
            {ECandidateKind::Keyword, "EXPORT"},
            {ECandidateKind::Keyword, "FOR"},
            {ECandidateKind::Keyword, "FROM"},
            {ECandidateKind::Keyword, "GRANT"},
            {ECandidateKind::Keyword, "IF"},
            {ECandidateKind::Keyword, "IMPORT"},
            {ECandidateKind::Keyword, "INSERT"},
            {ECandidateKind::Keyword, "PARALLEL"},
            {ECandidateKind::Keyword, "PRAGMA"},
            {ECandidateKind::Keyword, "PROCESS"},
            {ECandidateKind::Keyword, "REDUCE"},
            {ECandidateKind::Keyword, "REPLACE"},
            {ECandidateKind::Keyword, "RESTORE"},
            {ECandidateKind::Keyword, "REVOKE"},
            {ECandidateKind::Keyword, "ROLLBACK"},
            {ECandidateKind::Keyword, "SELECT"},
            {ECandidateKind::Keyword, "UPDATE"},
            {ECandidateKind::Keyword, "UPSERT"},
            {ECandidateKind::Keyword, "USE"},
            {ECandidateKind::Keyword, "VALUES"},
        };

        TYQLCompletionEngine engine;
        UNIT_ASSERT_VALUES_EQUAL(engine.Complete("").Candidates, expected);
        UNIT_ASSERT_VALUES_EQUAL(engine.Complete(" ").Candidates, expected);
        UNIT_ASSERT_VALUES_EQUAL(engine.Complete("  ").Candidates, expected);
        UNIT_ASSERT_VALUES_EQUAL(engine.Complete(";").Candidates, expected);
        UNIT_ASSERT_VALUES_EQUAL(engine.Complete("; ").Candidates, expected);
        UNIT_ASSERT_VALUES_EQUAL(engine.Complete(" ; ").Candidates, expected);
    }

    Y_UNIT_TEST(Alter) {
        TVector<TCandidate> expected = {
            {ECandidateKind::Keyword, "ASYNC"},
            {ECandidateKind::Keyword, "BACKUP"},
            {ECandidateKind::Keyword, "EXTERNAL"},
            {ECandidateKind::Keyword, "GROUP"},
            {ECandidateKind::Keyword, "OBJECT"},
            {ECandidateKind::Keyword, "RESOURCE"},
            {ECandidateKind::Keyword, "SEQUENCE"},
            {ECandidateKind::Keyword, "TABLE"},
            {ECandidateKind::Keyword, "TABLESTORE"},
            {ECandidateKind::Keyword, "TOPIC"},
            {ECandidateKind::Keyword, "USER"},
        };

        TYQLCompletionEngine engine;
        UNIT_ASSERT_VALUES_EQUAL(engine.Complete("ALTER ").Candidates, expected);
    }

    Y_UNIT_TEST(Create) {
        TVector<TCandidate> expected = {
            {ECandidateKind::Keyword, "ASYNC"},
            {ECandidateKind::Keyword, "BACKUP"},
            {ECandidateKind::Keyword, "EXTERNAL"},
            {ECandidateKind::Keyword, "GROUP"},
            {ECandidateKind::Keyword, "OBJECT"},
            {ECandidateKind::Keyword, "OR"},
            {ECandidateKind::Keyword, "RESOURCE"},
            {ECandidateKind::Keyword, "TABLE"},
            {ECandidateKind::Keyword, "TABLESTORE"},
            {ECandidateKind::Keyword, "TEMP"},
            {ECandidateKind::Keyword, "TEMPORARY"},
            {ECandidateKind::Keyword, "TOPIC"},
            {ECandidateKind::Keyword, "USER"},
            {ECandidateKind::Keyword, "VIEW"},
        };

        TYQLCompletionEngine engine;
        UNIT_ASSERT_VALUES_EQUAL(engine.Complete("CREATE ").Candidates, expected);
    }

    Y_UNIT_TEST(Delete) {
        TVector<TCandidate> expected = {
            {ECandidateKind::Keyword, "FROM"},
        };

        TYQLCompletionEngine engine;
        UNIT_ASSERT_VALUES_EQUAL(engine.Complete("DELETE ").Candidates, expected);
    }

    Y_UNIT_TEST(Drop) {
        TVector<TCandidate> expected = {
            {ECandidateKind::Keyword, "ASYNC"},
            {ECandidateKind::Keyword, "BACKUP"},
            {ECandidateKind::Keyword, "EXTERNAL"},
            {ECandidateKind::Keyword, "GROUP"},
            {ECandidateKind::Keyword, "OBJECT"},
            {ECandidateKind::Keyword, "RESOURCE"},
            {ECandidateKind::Keyword, "TABLE"},
            {ECandidateKind::Keyword, "TABLESTORE"},
            {ECandidateKind::Keyword, "TOPIC"},
            {ECandidateKind::Keyword, "USER"},
            {ECandidateKind::Keyword, "VIEW"},
        };

        TYQLCompletionEngine engine;
        UNIT_ASSERT_VALUES_EQUAL(engine.Complete("DROP ").Candidates, expected);
    }

    Y_UNIT_TEST(Explain) {
        TVector<TCandidate> expected = {
            {ECandidateKind::Keyword, "ALTER"},
            {ECandidateKind::Keyword, "ANALYZE"},
            {ECandidateKind::Keyword, "BACKUP"},
            {ECandidateKind::Keyword, "COMMIT"},
            {ECandidateKind::Keyword, "CREATE"},
            {ECandidateKind::Keyword, "DECLARE"},
            {ECandidateKind::Keyword, "DEFINE"},
            {ECandidateKind::Keyword, "DELETE"},
            {ECandidateKind::Keyword, "DISCARD"},
            {ECandidateKind::Keyword, "DO"},
            {ECandidateKind::Keyword, "DROP"},
            {ECandidateKind::Keyword, "EVALUATE"},
            {ECandidateKind::Keyword, "EXPORT"},
            {ECandidateKind::Keyword, "FOR"},
            {ECandidateKind::Keyword, "FROM"},
            {ECandidateKind::Keyword, "GRANT"},
            {ECandidateKind::Keyword, "IF"},
            {ECandidateKind::Keyword, "IMPORT"},
            {ECandidateKind::Keyword, "INSERT"},
            {ECandidateKind::Keyword, "PARALLEL"},
            {ECandidateKind::Keyword, "PRAGMA"},
            {ECandidateKind::Keyword, "PROCESS"},
            {ECandidateKind::Keyword, "QUERY"},
            {ECandidateKind::Keyword, "REDUCE"},
            {ECandidateKind::Keyword, "REPLACE"},
            {ECandidateKind::Keyword, "RESTORE"},
            {ECandidateKind::Keyword, "REVOKE"},
            {ECandidateKind::Keyword, "ROLLBACK"},
            {ECandidateKind::Keyword, "SELECT"},
            {ECandidateKind::Keyword, "UPDATE"},
            {ECandidateKind::Keyword, "UPSERT"},
            {ECandidateKind::Keyword, "USE"},
            {ECandidateKind::Keyword, "VALUES"},
        };

        TYQLCompletionEngine engine;
        UNIT_ASSERT_VALUES_EQUAL(engine.Complete("EXPLAIN ").Candidates, expected);
    }

    Y_UNIT_TEST(Grant) {
        TVector<TCandidate> expected = {
            {ECandidateKind::Keyword, "ALL"},
            {ECandidateKind::Keyword, "ALTER"},
            {ECandidateKind::Keyword, "CONNECT"},
            {ECandidateKind::Keyword, "CREATE"},
            {ECandidateKind::Keyword, "DESCRIBE"},
            {ECandidateKind::Keyword, "DROP"},
            {ECandidateKind::Keyword, "ERASE"},
            {ECandidateKind::Keyword, "FULL"},
            {ECandidateKind::Keyword, "GRANT"},
            {ECandidateKind::Keyword, "INSERT"},
            {ECandidateKind::Keyword, "LIST"},
            {ECandidateKind::Keyword, "MANAGE"},
            {ECandidateKind::Keyword, "MODIFY"},
            {ECandidateKind::Keyword, "REMOVE"},
            {ECandidateKind::Keyword, "SELECT"},
            {ECandidateKind::Keyword, "UPDATE"},
            {ECandidateKind::Keyword, "USE"},
        };

        TYQLCompletionEngine engine;
        UNIT_ASSERT_VALUES_EQUAL(engine.Complete("GRANT ").Candidates, expected);
    }

    Y_UNIT_TEST(Insert) {
        TVector<TCandidate> expected = {
            {ECandidateKind::Keyword, "INTO"},
            {ECandidateKind::Keyword, "OR"},
        };

        TYQLCompletionEngine engine;
        UNIT_ASSERT_VALUES_EQUAL(engine.Complete("INSERT ").Candidates, expected);
    }

    Y_UNIT_TEST(Pragma) {
        TVector<TCandidate> expected = {
            {ECandidateKind::Keyword, "ANSI"},
            {ECandidateKind::Keyword, "CALLABLE"},
            {ECandidateKind::Keyword, "DICT"},
            {ECandidateKind::Keyword, "ENUM"},
            {ECandidateKind::Keyword, "FLOW"},
            {ECandidateKind::Keyword, "LIST"},
            {ECandidateKind::Keyword, "OPTIONAL"},
            {ECandidateKind::Keyword, "RESOURCE"},
            {ECandidateKind::Keyword, "SET"},
            {ECandidateKind::Keyword, "STRUCT"},
            {ECandidateKind::Keyword, "TAGGED"},
            {ECandidateKind::Keyword, "TUPLE"},
            {ECandidateKind::Keyword, "VARIANT"},
        };

        TYQLCompletionEngine engine;
        UNIT_ASSERT_VALUES_EQUAL(engine.Complete("PRAGMA ").Candidates, expected);
    }

    Y_UNIT_TEST(Select) {
        TVector<TCandidate> expected = {
            {ECandidateKind::Keyword, "ALL"},
            {ECandidateKind::Keyword, "BITCAST"},
            {ECandidateKind::Keyword, "CALLABLE"},
            {ECandidateKind::Keyword, "CASE"},
            {ECandidateKind::Keyword, "CAST"},
            {ECandidateKind::Keyword, "CURRENT_DATE"},
            {ECandidateKind::Keyword, "CURRENT_TIME"},
            {ECandidateKind::Keyword, "CURRENT_TIMESTAMP"},
            {ECandidateKind::Keyword, "DICT"},
            {ECandidateKind::Keyword, "DISTINCT"},
            {ECandidateKind::Keyword, "EMPTY_ACTION"},
            {ECandidateKind::Keyword, "ENUM"},
            {ECandidateKind::Keyword, "EXISTS"},
            {ECandidateKind::Keyword, "FALSE"},
            {ECandidateKind::Keyword, "FLOW"},
            {ECandidateKind::Keyword, "JSON_EXISTS"},
            {ECandidateKind::Keyword, "JSON_QUERY"},
            {ECandidateKind::Keyword, "JSON_VALUE"},
            {ECandidateKind::Keyword, "LIST"},
            {ECandidateKind::Keyword, "NOT"},
            {ECandidateKind::Keyword, "NULL"},
            {ECandidateKind::Keyword, "OPTIONAL"},
            {ECandidateKind::Keyword, "RESOURCE"},
            {ECandidateKind::Keyword, "SET"},
            {ECandidateKind::Keyword, "STREAM"},
            {ECandidateKind::Keyword, "STRUCT"},
            {ECandidateKind::Keyword, "TAGGED"},
            {ECandidateKind::Keyword, "TRUE"},
            {ECandidateKind::Keyword, "TUPLE"},
            {ECandidateKind::Keyword, "VARIANT"},
        };

        TYQLCompletionEngine engine;
        UNIT_ASSERT_VALUES_EQUAL(engine.Complete("SELECT ").Candidates, expected);
    }

    Y_UNIT_TEST(Upsert) {
        TVector<TCandidate> expected = {
            {ECandidateKind::Keyword, "INTO"},
            {ECandidateKind::Keyword, "OBJECT"},
        };

        TYQLCompletionEngine engine;
        UNIT_ASSERT_VALUES_EQUAL(engine.Complete("UPSERT ").Candidates, expected);
    }

    Y_UNIT_TEST(UTF8Wide) {
        TYQLCompletionEngine engine;
        UNIT_ASSERT_VALUES_EQUAL(engine.Complete("\xF0\x9F\x98\x8A").Candidates.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(engine.Complete("编码").Candidates.size(), 0);
    }

    Y_UNIT_TEST(Typing) {
        const auto queryUtf16 = TUtf16String::FromUtf8(
            "SELECT \n"
            "  123467, \"Hello, {name}! 编码\", \n"
            "  (1 + (5 * 1 / 0)), MIN(identifier), \n"
            "  Bool(field), Math::Sin(var) \n"
            "FROM `local/test/space/table` JOIN test;");

        TYQLCompletionEngine engine;

        for (std::size_t size = 0; size <= queryUtf16.size(); ++size) {
            const TWtringBuf prefixUtf16(queryUtf16, 0, size);
            auto completion = engine.Complete(TString::FromUtf16(prefixUtf16));
            Y_DO_NOT_OPTIMIZE_AWAY(completion);
        }
    }
} // Y_UNIT_TEST_SUITE(YqlCompleteTests)
