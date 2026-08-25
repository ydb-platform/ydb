#include "simple_leaky_bucket.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TSimpleLeakyBucketTest)
{
    Y_UNIT_TEST(ShouldNotAccumulateBudgetAboveInitialBudget)
    {
        const TInstant start = TInstant::Seconds(1);
        TSimpleLeakyBucket bucket(start, 10, 100);

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            bucket.Register(start, 100));
        UNIT_ASSERT_DOUBLES_EQUAL(0, bucket.GetBudget(), 1e-10);

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            bucket.Register(start + TDuration::Seconds(100), 10));
        UNIT_ASSERT_DOUBLES_EQUAL(90, bucket.GetBudget(), 1e-10);
    }

    Y_UNIT_TEST(ShouldAccumulateBudgetBelowLimit)
    {
        const TInstant start = TInstant::Seconds(1);
        TSimpleLeakyBucket bucket(start, 10, 100);

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            bucket.Register(start, 100));

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            bucket.Register(start + TDuration::Seconds(3), 10));
        UNIT_ASSERT_DOUBLES_EQUAL(20, bucket.GetBudget(), 1e-10);
    }
}

}   // namespace NYdb::NBS::NBlockStore
