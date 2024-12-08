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
} // Y_UNIT_TEST_SUITE(YqlCompleteTests)
