#include "yql_suggest.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb::NConsoleClient;

Y_UNIT_TEST_SUITE(YqlSuggestTests) {
    auto Suggest(TStringBuf queryUtf8) {
        return YQLSuggestionEngine().Suggest(queryUtf8);
    }

    Y_UNIT_TEST(Blank) {
        UNIT_ASSERT_EQUAL(Suggest("").size(), 33);
        UNIT_ASSERT_EQUAL(Suggest(" ").size(), 33);
    }

    Y_UNIT_TEST(Select) {
        UNIT_ASSERT_EQUAL(Suggest("s").size(), 1);
        UNIT_ASSERT_EQUAL(Suggest("select").size(), 1);
        UNIT_ASSERT_EQUAL(Suggest("select ").size(), 281);
        UNIT_ASSERT_EQUAL(Suggest("select (").size(), 0); // FIXME
        UNIT_ASSERT_EQUAL(Suggest("select 1 ").size(), 211);
        UNIT_ASSERT_EQUAL(Suggest("select 1 + ").size(), 281);
        UNIT_ASSERT_EQUAL(Suggest("select test ").size(), 211);
        UNIT_ASSERT_EQUAL(Suggest("select test from ").size(), 279);
        UNIT_ASSERT_EQUAL(Suggest("select test from select 1 ").size(), 0);
        UNIT_ASSERT_EQUAL(Suggest("select test from as ").size(), 216);
        UNIT_ASSERT_EQUAL(Suggest("select test from as as ").size(), 267);
        UNIT_ASSERT_EQUAL(Suggest("select test from as as as ").size(), 24);
    }
} // Y_UNIT_TEST_SUITE(YqlSuggestTests)
