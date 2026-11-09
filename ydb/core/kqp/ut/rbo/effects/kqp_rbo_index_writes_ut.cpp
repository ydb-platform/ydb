#include "kqp_rbo_index_writes_helpers.h"

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpRboIndexWrites) {
    Y_UNIT_TEST_TWIN(UpsertUniqIndexNullKeys, NewRbo) {
        NIndexWritesTests::UpsertUniqIndexNullKeys<NewRbo, true>();
    }

    Y_UNIT_TEST(UpsertUniqIndexNullKeysLegacyWithoutStreamIndex) {
        NIndexWritesTests::UpsertUniqIndexNullKeys<false, false>();
    }

    Y_UNIT_TEST_TWIN(UpsertUniqIndexCompositePartialNull, NewRbo) {
        NIndexWritesTests::UpsertUniqIndexCompositePartialNull<NewRbo, true>();
    }

    Y_UNIT_TEST(UpsertUniqIndexCompositePartialNullLegacyWithoutStreamIndex) {
        NIndexWritesTests::UpsertUniqIndexCompositePartialNull<false, false>();
    }

    Y_UNIT_TEST_TWIN(ReplaceMultipleIndexesEquivalence, NewRbo) {
        NIndexWritesTests::ReplaceMultipleIndexesEquivalence<NewRbo, true>();
    }

    Y_UNIT_TEST(ReplaceMultipleIndexesEquivalenceLegacyWithoutStreamIndex) {
        NIndexWritesTests::ReplaceMultipleIndexesEquivalence<false, false>();
    }

    Y_UNIT_TEST_TWIN(UpsertPartialColumnsWithCoveringIndex, NewRbo) {
        NIndexWritesTests::UpsertPartialColumnsWithCoveringIndex<NewRbo, true>();
    }

    Y_UNIT_TEST(UpsertPartialColumnsWithCoveringIndexLegacyWithoutStreamIndex) {
        NIndexWritesTests::UpsertPartialColumnsWithCoveringIndex<false, false>();
    }

    Y_UNIT_TEST_TWIN(UpsertDefaultColumnWithCoveringIndex, NewRbo) {
        NIndexWritesTests::UpsertDefaultColumnWithCoveringIndex<NewRbo, true>();
    }

    Y_UNIT_TEST(UpsertDefaultColumnWithCoveringIndexLegacyWithoutStreamIndex) {
        NIndexWritesTests::UpsertDefaultColumnWithCoveringIndex<false, false>();
    }

    Y_UNIT_TEST_TWIN(ConditionalUpdateIndexKeyChangeAndNoChange, NewRbo) {
        NIndexWritesTests::ConditionalUpdateIndexKeyChangeAndNoChange<NewRbo, true>();
    }

    Y_UNIT_TEST(ConditionalUpdateIndexKeyChangeAndNoChangeLegacyWithoutStreamIndex) {
        NIndexWritesTests::ConditionalUpdateIndexKeyChangeAndNoChange<false, false>();
    }

    Y_UNIT_TEST_TWIN(UpdateIndexedColumnToNullAndBack, NewRbo) {
        NIndexWritesTests::UpdateIndexedColumnToNullAndBack<NewRbo, true>();
    }

    Y_UNIT_TEST(UpdateIndexedColumnToNullAndBackLegacyWithoutStreamIndex) {
        NIndexWritesTests::UpdateIndexedColumnToNullAndBack<false, false>();
    }
}

} // namespace NKikimr::NKqp
