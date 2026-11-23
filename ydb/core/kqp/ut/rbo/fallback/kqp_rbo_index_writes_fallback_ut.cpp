#include <ydb/core/kqp/ut/rbo/effects/kqp_rbo_index_writes_helpers.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpRboIndexWritesFallback) {
    Y_UNIT_TEST(UpsertUniqIndexNullKeys) {
        NIndexWritesTests::UpsertUniqIndexNullKeys<true, false>();
    }

    Y_UNIT_TEST(UpsertUniqIndexCompositePartialNull) {
        NIndexWritesTests::UpsertUniqIndexCompositePartialNull<true, false>();
    }

    Y_UNIT_TEST(ReplaceMultipleIndexesEquivalence) {
        NIndexWritesTests::ReplaceMultipleIndexesEquivalence<true, false>();
    }

    Y_UNIT_TEST(UpsertPartialColumnsWithCoveringIndex) {
        NIndexWritesTests::UpsertPartialColumnsWithCoveringIndex<true, false>();
    }

    Y_UNIT_TEST(UpsertDefaultColumnWithCoveringIndex) {
        NIndexWritesTests::UpsertDefaultColumnWithCoveringIndex<true, false>();
    }

    Y_UNIT_TEST(ConditionalUpdateIndexKeyChangeAndNoChange) {
        NIndexWritesTests::ConditionalUpdateIndexKeyChangeAndNoChange<true, false>();
    }

    Y_UNIT_TEST(UpdateIndexedColumnToNullAndBack) {
        NIndexWritesTests::UpdateIndexedColumnToNullAndBack<true, false>();
    }
}

} // namespace NKikimr::NKqp
