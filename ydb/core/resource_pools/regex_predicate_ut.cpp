#include "regex_predicate.h"

#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr {

using NResourcePool::TRegexPredicate;


Y_UNIT_TEST_SUITE(TRegexPredicateFromGlob) {

    Y_UNIT_TEST(LiteralGlob) {
        auto pred = TRegexPredicate::FromGlob("/Root/db/orders");
        UNIT_ASSERT(pred.Match("/Root/db/orders"));
        UNIT_ASSERT(!pred.Match("/Root/db/orders_archive"));
        UNIT_ASSERT(!pred.Match("/Root/db"));
    }

    Y_UNIT_TEST(StarMatchesAnyCount) {
        auto pred = TRegexPredicate::FromGlob("/Root/db/*");
        UNIT_ASSERT(pred.Match("/Root/db/"));
        UNIT_ASSERT(pred.Match("/Root/db/orders"));
        UNIT_ASSERT(pred.Match("/Root/db/archive/orders/2024"));
        UNIT_ASSERT(!pred.Match("/Root/db"));
        UNIT_ASSERT(!pred.Match("/Root/other/x"));
    }

    Y_UNIT_TEST(QuestionMarkMatchesExactlyOne) {
        auto pred = TRegexPredicate::FromGlob("/Root/t?");
        UNIT_ASSERT(pred.Match("/Root/t1"));
        UNIT_ASSERT(pred.Match("/Root/t_"));
        UNIT_ASSERT(!pred.Match("/Root/t"));
        UNIT_ASSERT(!pred.Match("/Root/t12"));
    }

    Y_UNIT_TEST(QuestionMarkMatchesSlash) {
        auto pred = TRegexPredicate::FromGlob("/Root?db");
        UNIT_ASSERT(pred.Match("/Root/db"));
        UNIT_ASSERT(pred.Match("/RootXdb"));
        UNIT_ASSERT(!pred.Match("/Rootdb"));
    }

    Y_UNIT_TEST(StarMatchesSlash) {
        auto pred = TRegexPredicate::FromGlob("/Root/*/orders");
        UNIT_ASSERT(pred.Match("/Root/db1/orders"));
        UNIT_ASSERT(pred.Match("/Root/tenant/sub/orders"));
        UNIT_ASSERT(!pred.Match("/Root/orders"));
    }

    Y_UNIT_TEST(DoubleStarCollapses) {
        auto both = TRegexPredicate::FromGlob("/Root/**");
        auto one = TRegexPredicate::FromGlob("/Root/*");
        UNIT_ASSERT(both.Match("/Root/"));
        UNIT_ASSERT(both.Match("/Root/anything"));
        UNIT_ASSERT(one.Match("/Root/"));
        UNIT_ASSERT(one.Match("/Root/anything"));
    }

    Y_UNIT_TEST(MixedStarAndQuestion) {
        auto pred = TRegexPredicate::FromGlob("/Root/t?/*");
        UNIT_ASSERT(pred.Match("/Root/t1/anything"));
        UNIT_ASSERT(pred.Match("/Root/tX/"));
        UNIT_ASSERT(!pred.Match("/Root/t/anything"));
        UNIT_ASSERT(!pred.Match("/Root/t12/anything"));
    }

    Y_UNIT_TEST(DotIsLiteral) {
        auto pred = TRegexPredicate::FromGlob("/Root/foo.bar");
        UNIT_ASSERT(pred.Match("/Root/foo.bar"));
        UNIT_ASSERT(!pred.Match("/Root/fooxbar"));
    }

    Y_UNIT_TEST(RegexMetacharsAreLiteral) {
        auto pred = TRegexPredicate::FromGlob("/Root/.+()[]{}|^$\\");
        UNIT_ASSERT(pred.Match("/Root/.+()[]{}|^$\\"));
        UNIT_ASSERT(!pred.Match("/Root/a"));
    }

    Y_UNIT_TEST(EmptyThrows) {
        UNIT_ASSERT_EXCEPTION(TRegexPredicate::FromGlob(""), yexception);
    }

    Y_UNIT_TEST(StarOnlyMatchesAnything) {
        auto pred = TRegexPredicate::FromGlob("*");
        UNIT_ASSERT(pred.Match(""));
        UNIT_ASSERT(pred.Match("anything"));
        UNIT_ASSERT(pred.Match("/Root/db/orders"));
    }
}

}  // namespace NKikimr
