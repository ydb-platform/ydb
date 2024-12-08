#include "yql_complete.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb::NConsoleClient;

Y_UNIT_TEST_SUITE(YqlCompleteTests) {
    size_t CompletionsCount(TYQLCompletionEngine& engine, TString prefix) {
        return engine.Complete(prefix).Candidates.size();
    }

    Y_UNIT_TEST(Blank) {
        TYQLCompletionEngine engine;
        UNIT_ASSERT_VALUES_EQUAL(CompletionsCount(engine, ""), 33);
        UNIT_ASSERT_VALUES_EQUAL(CompletionsCount(engine, " "), 33);
        UNIT_ASSERT_VALUES_EQUAL(CompletionsCount(engine, "  "), 33);
        UNIT_ASSERT_VALUES_EQUAL(CompletionsCount(engine, "   "), 33);
    }

    Y_UNIT_TEST(Select) {
        TYQLCompletionEngine engine;
        UNIT_ASSERT_VALUES_EQUAL(CompletionsCount(engine, "s"), 1);
        UNIT_ASSERT_VALUES_EQUAL(CompletionsCount(engine, "select"), 1);
        UNIT_ASSERT_VALUES_EQUAL(CompletionsCount(engine, "select "), 30);
        UNIT_ASSERT_VALUES_EQUAL(CompletionsCount(engine, "select ("), 28);
        UNIT_ASSERT_VALUES_EQUAL(CompletionsCount(engine, "select (s"), 3);
        UNIT_ASSERT_VALUES_EQUAL(CompletionsCount(engine, "select 1 "), 31);
        UNIT_ASSERT_VALUES_EQUAL(CompletionsCount(engine, "select 1 + "), 27);
        UNIT_ASSERT_VALUES_EQUAL(CompletionsCount(engine, "select test "), 31);
        UNIT_ASSERT_VALUES_EQUAL(CompletionsCount(engine, "select test from "), 13);
        UNIT_ASSERT_VALUES_EQUAL(CompletionsCount(engine, "select test from (s"), 1);
        UNIT_ASSERT_VALUES_EQUAL(CompletionsCount(engine, "select test from select 1 "), 0);
        UNIT_ASSERT_VALUES_EQUAL(CompletionsCount(engine, "select test from as "), 28);
        UNIT_ASSERT_VALUES_EQUAL(CompletionsCount(engine, "select test from as as "), 0);
        UNIT_ASSERT_VALUES_EQUAL(CompletionsCount(engine, "select test from as as as "), 24);
    }

    Y_UNIT_TEST(UTF8Wide) {
        TYQLCompletionEngine engine;
        UNIT_ASSERT_VALUES_EQUAL(CompletionsCount(engine, "\xF0\x9F\x98\x8A"), 0);
        UNIT_ASSERT_VALUES_EQUAL(CompletionsCount(engine, "编码"), 0);
    }

    Y_UNIT_TEST(Typing) {
        const TString query =
            "SELECT \n"
            "  123467, \"Hello, {name}! 编码\", \n"
            "  (1 + (5 * 1 / 0)), MIN(identifier), \n"
            "  Bool(field), Math::Sin(var) \n"
            "FROM `local/test/space/table` JOIN test;";

        TYQLCompletionEngine engine;

        for (std::size_t size = 0; size <= query.size(); ++size) {
            const TStringBuf prefix(query, 0, size);
            auto completion = engine.Complete(prefix);
            Y_DO_NOT_OPTIMIZE_AWAY(completion);
        }
    }
} // Y_UNIT_TEST_SUITE(YqlCompleteTests)
