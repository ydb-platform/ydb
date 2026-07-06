#include "regex_predicate.h"

#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr {

using NResourcePool::TRegexPredicate;


Y_UNIT_TEST_SUITE(TRegexPredicateTest) {

    Y_UNIT_TEST(LiteralIsAnchored) {
        auto pred = TRegexPredicate::Compile("ydb-ui");
        UNIT_ASSERT(pred.Match("ydb-ui"));
        UNIT_ASSERT(!pred.Match("ydb-ui-prod"));
        UNIT_ASSERT(!pred.Match("prefix-ydb-ui"));
    }

    Y_UNIT_TEST(AlternationMatchesExactlyEachBranch) {
        auto pred = TRegexPredicate::Compile("ydb-ui|ydb-cli");
        UNIT_ASSERT(pred.Match("ydb-ui"));
        UNIT_ASSERT(pred.Match("ydb-cli"));
        UNIT_ASSERT(!pred.Match("ydb-ui-prod"));
        UNIT_ASSERT(!pred.Match("prefix-ydb-cli"));
    }

    Y_UNIT_TEST(WildcardSuffix) {
        auto pred = TRegexPredicate::Compile("ydb-.*");
        UNIT_ASSERT(pred.Match("ydb-ui"));
        UNIT_ASSERT(pred.Match("ydb-cli-v2"));
        UNIT_ASSERT(!pred.Match("ydb"));
        UNIT_ASSERT(!pred.Match("other-ydb-ui"));
    }

    Y_UNIT_TEST(EmptyInputDoesNotMatchNonEmptyPattern) {
        auto pred = TRegexPredicate::Compile("ydb-ui");
        UNIT_ASSERT(!pred.Match(""));
    }

    Y_UNIT_TEST(EmptyPatternMatchesOnlyEmpty) {
        auto pred = TRegexPredicate::Compile("");
        UNIT_ASSERT(pred.Match(""));
        UNIT_ASSERT(!pred.Match("x"));
    }

    Y_UNIT_TEST(AdminProvidedAnchorsAreHarmless) {
        auto pred = TRegexPredicate::Compile("^ydb-ui$");
        UNIT_ASSERT(pred.Match("ydb-ui"));
        UNIT_ASSERT(!pred.Match("ydb-ui-prod"));
    }

    Y_UNIT_TEST(InvalidPatternThrows) {
        UNIT_ASSERT_EXCEPTION(TRegexPredicate::Compile("[unclosed"), yexception);
        UNIT_ASSERT_EXCEPTION(TRegexPredicate::Compile("(?<bad"), yexception);
    }
}

}  // namespace NKikimr
