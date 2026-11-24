#include <ydb/core/kqp/ut/rbo/effects/kqp_rbo_index_returning_helpers.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpRboIndexReturningFallback) {
    Y_UNIT_TEST(UpdateDeleteReturningOnlyKey) {
        NIndexReturningTests::UpdateDeleteReturningOnlyKey<true, false>();
    }

    Y_UNIT_TEST(ManyEmptyReturningClauses) {
        NIndexReturningTests::ManyEmptyReturningClauses<true, false>();
    }

    Y_UNIT_TEST(UpdateIndexKeyChangeWithReturningVerifyIndex) {
        NIndexReturningTests::UpdateIndexKeyChangeWithReturningVerifyIndex<true, false>();
    }

    Y_UNIT_TEST(DefaultOnIndexedColumnWithReturning) {
        NIndexReturningTests::DefaultOnIndexedColumnWithReturning<true, false>();
    }

    Y_UNIT_TEST(UpdateSetIndexedColumnToDefaultWithReturning) {
        NIndexReturningTests::UpdateSetIndexedColumnToDefaultWithReturning<true, false>();
    }
}

} // namespace NKikimr::NKqp
