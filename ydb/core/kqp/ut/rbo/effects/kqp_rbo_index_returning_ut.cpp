#include "kqp_rbo_index_returning_helpers.h"

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpRboIndexReturning) {
    Y_UNIT_TEST_TWIN(UpdateDeleteReturningOnlyKey, NewRbo) {
        NIndexReturningTests::UpdateDeleteReturningOnlyKey<NewRbo, true>();
    }

    Y_UNIT_TEST(UpdateDeleteReturningOnlyKeyLegacyWithoutStreamIndex) {
        NIndexReturningTests::UpdateDeleteReturningOnlyKey<false, false>();
    }

    Y_UNIT_TEST_TWIN(ManyEmptyReturningClauses, NewRbo) {
        NIndexReturningTests::ManyEmptyReturningClauses<NewRbo, true>();
    }

    Y_UNIT_TEST(ManyEmptyReturningClausesLegacyWithoutStreamIndex) {
        NIndexReturningTests::ManyEmptyReturningClauses<false, false>();
    }

    Y_UNIT_TEST_TWIN(UpdateIndexKeyChangeWithReturningVerifyIndex, NewRbo) {
        NIndexReturningTests::UpdateIndexKeyChangeWithReturningVerifyIndex<NewRbo, true>();
    }

    Y_UNIT_TEST(UpdateIndexKeyChangeWithReturningVerifyIndexLegacyWithoutStreamIndex) {
        NIndexReturningTests::UpdateIndexKeyChangeWithReturningVerifyIndex<false, false>();
    }

    Y_UNIT_TEST_TWIN(DefaultOnIndexedColumnWithReturning, NewRbo) {
        NIndexReturningTests::DefaultOnIndexedColumnWithReturning<NewRbo, true>();
    }

    Y_UNIT_TEST(DefaultOnIndexedColumnWithReturningLegacyWithoutStreamIndex) {
        NIndexReturningTests::DefaultOnIndexedColumnWithReturning<false, false>();
    }

    Y_UNIT_TEST_TWIN(UpdateSetIndexedColumnToDefaultWithReturning, NewRbo) {
        NIndexReturningTests::UpdateSetIndexedColumnToDefaultWithReturning<NewRbo, true>();
    }

    Y_UNIT_TEST(UpdateSetIndexedColumnToDefaultWithReturningLegacyWithoutStreamIndex) {
        NIndexReturningTests::UpdateSetIndexedColumnToDefaultWithReturning<false, false>();
    }
}

} // namespace NKikimr::NKqp
