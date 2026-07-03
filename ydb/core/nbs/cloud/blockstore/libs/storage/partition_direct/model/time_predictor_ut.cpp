#include "time_predictor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TTimePredictorTest)
{
    constexpr ui32 TimePredictionHistorySize = 10;
    constexpr ui32 TimePredictionNthFromEnd = 1;

    Y_UNIT_TEST(PredictReturnsZeroForEmptyHistory)
    {
        TTimePredictor predictor(
            TimePredictionHistorySize,
            TimePredictionNthFromEnd);

        UNIT_ASSERT_VALUES_EQUAL(TDuration(), predictor.Predict(0));
        UNIT_ASSERT_VALUES_EQUAL(TDuration(), predictor.Predict(1));
        UNIT_ASSERT_VALUES_EQUAL(TDuration(), predictor.Predict(4));
    }

    Y_UNIT_TEST(PredictReturnsZeroForOutOfRangeHost)
    {
        TTimePredictor predictor(
            TimePredictionHistorySize,
            TimePredictionNthFromEnd);

        // Host index beyond initial DirectBlockGroupHostCount (5) and never
        // added — should return zero without crashing.
        UNIT_ASSERT_VALUES_EQUAL(TDuration(), predictor.Predict(100));
    }

    Y_UNIT_TEST(AddSingleValue)
    {
        TTimePredictor predictor(
            TimePredictionHistorySize,
            TimePredictionNthFromEnd);

        predictor.Add(THostIndex(0), TDuration::MilliSeconds(100));

        // With nthFromEnd == 1 and only 1 element, we skip 1 from the end
        // and hit rend → returns zero.
        UNIT_ASSERT_VALUES_EQUAL(TDuration(), predictor.Predict(0));
    }

    Y_UNIT_TEST(AddTwoValues)
    {
        TTimePredictor predictor(
            TimePredictionHistorySize,
            TimePredictionNthFromEnd);

        predictor.Add(THostIndex(0), TDuration::MilliSeconds(100));
        predictor.Add(THostIndex(0), TDuration::MilliSeconds(200));

        // Sorted: [100, 200]. NthFromEnd=1 means skip last (200), return 100.
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(100),
            predictor.Predict(0));
    }

    Y_UNIT_TEST(PredictReturnsSecondLargest)
    {
        TTimePredictor predictor(
            TimePredictionHistorySize,
            TimePredictionNthFromEnd);

        predictor.Add(THostIndex(0), TDuration::MilliSeconds(50));
        predictor.Add(THostIndex(0), TDuration::MilliSeconds(300));
        predictor.Add(THostIndex(0), TDuration::MilliSeconds(100));

        // Sorted: [50, 100, 300]. NthFromEnd=1 → skip 300, return 100.
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(100),
            predictor.Predict(0));
    }

    Y_UNIT_TEST(PredictWithDuplicateValues)
    {
        TTimePredictor predictor(
            TimePredictionHistorySize,
            TimePredictionNthFromEnd);

        predictor.Add(THostIndex(0), TDuration::MilliSeconds(100));
        predictor.Add(THostIndex(0), TDuration::MilliSeconds(100));
        predictor.Add(THostIndex(0), TDuration::MilliSeconds(100));

        // Sorted (multiset): [100, 100, 100]. NthFromEnd=1 → skip last 100,
        // return 100.
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(100),
            predictor.Predict(0));
    }

    Y_UNIT_TEST(HostsAreIndependent)
    {
        TTimePredictor predictor(
            TimePredictionHistorySize,
            TimePredictionNthFromEnd);

        predictor.Add(THostIndex(0), TDuration::MilliSeconds(100));
        predictor.Add(THostIndex(0), TDuration::MilliSeconds(200));

        predictor.Add(THostIndex(1), TDuration::MilliSeconds(500));
        predictor.Add(THostIndex(1), TDuration::MilliSeconds(600));

        // Host 0: sorted [100, 200] → predict 100.
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(100),
            predictor.Predict(0));

        // Host 1: sorted [500, 600] → predict 500.
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(500),
            predictor.Predict(1));
    }

    Y_UNIT_TEST(RingBufferEvictsOldValues)
    {
        TTimePredictor predictor(
            TimePredictionHistorySize,
            TimePredictionNthFromEnd);

        // Fill the ring buffer (capacity == 10) with small values.
        for (size_t i = 0; i < TimePredictionHistorySize; ++i) {
            predictor.Add(THostIndex(0), TDuration::MilliSeconds(10));
        }

        // Sorted: 10 x [10ms]. Predict → 10ms (second from end).
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(10),
            predictor.Predict(0));

        // Now push one large value — it evicts one old 10ms.
        predictor.Add(THostIndex(0), TDuration::MilliSeconds(1000));

        // Sorted: 9 x [10ms], 1 x [1000ms]. NthFromEnd=1 → skip 1000,
        // return 10.
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(10),
            predictor.Predict(0));

        // Now push one large value — it evicts one old 10ms.
        predictor.Add(THostIndex(0), TDuration::MilliSeconds(500));

        // Sorted: 5 x [10ms], 1 x [500ms], 1 x [1000ms]. NthFromEnd=1 → skip
        // 1000, return 500.
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(500),
            predictor.Predict(0));
    }

    Y_UNIT_TEST(AddWithHostMask)
    {
        TTimePredictor predictor(
            TimePredictionHistorySize,
            TimePredictionNthFromEnd);

        auto mask = THostMask::MakeOne(0).Include(THostMask::MakeOne(2));

        predictor.Add(mask, TDuration::MilliSeconds(100));
        predictor.Add(mask, TDuration::MilliSeconds(200));

        // Both host 0 and host 2 should have [100, 200].
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(100),
            predictor.Predict(0));
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(100),
            predictor.Predict(2));

        // Host 1 was not in the mask — should be zero.
        UNIT_ASSERT_VALUES_EQUAL(TDuration(), predictor.Predict(1));
    }

    Y_UNIT_TEST(PredictWithHostMaskReturnsMax)
    {
        TTimePredictor predictor(
            TimePredictionHistorySize,
            TimePredictionNthFromEnd);

        // Host 0: sorted [100, 200] → predict 100.
        predictor.Add(THostIndex(0), TDuration::MilliSeconds(100));
        predictor.Add(THostIndex(0), TDuration::MilliSeconds(200));

        // Host 1: sorted [300, 400] → predict 300.
        predictor.Add(THostIndex(1), TDuration::MilliSeconds(300));
        predictor.Add(THostIndex(1), TDuration::MilliSeconds(400));

        auto mask = THostMask::MakeOne(0).Include(THostMask::MakeOne(1));

        // Predict(mask) = max(100, 300) = 300.
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(300),
            predictor.Predict(mask));
    }

    Y_UNIT_TEST(PredictWithEmptyMaskReturnsZero)
    {
        TTimePredictor predictor(
            TimePredictionHistorySize,
            TimePredictionNthFromEnd);

        predictor.Add(THostIndex(0), TDuration::MilliSeconds(100));
        predictor.Add(THostIndex(0), TDuration::MilliSeconds(200));

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration(),
            predictor.Predict(THostMask::MakeEmpty()));
    }

    Y_UNIT_TEST(AddResizesHistoryForLargeHostIndex)
    {
        TTimePredictor predictor(
            TimePredictionHistorySize,
            TimePredictionNthFromEnd);

        // Host index beyond initial DirectBlockGroupHostCount (5).
        THostIndex largeHost = 10;

        predictor.Add(largeHost, TDuration::MilliSeconds(100));
        predictor.Add(largeHost, TDuration::MilliSeconds(200));

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(100),
            predictor.Predict(largeHost));
    }

    Y_UNIT_TEST(EvictionRemovesCorrectValueFromMultiset)
    {
        TTimePredictor predictor(
            TimePredictionHistorySize,
            TimePredictionNthFromEnd);

        // Fill with ascending values: 10, 20, ..., 100.
        for (size_t i = 0; i < TimePredictionHistorySize; ++i) {
            predictor.Add(THostIndex(0), TDuration::MilliSeconds(10 * (i + 1)));
        }

        // Predict: second-from-end of [10..100] → 90ms.
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(90),
            predictor.Predict(0));

        // Push 5ms — evicts the oldest, which is 10ms (first pushed).
        // New sorted set: [5, 20, 30, 40, 50, 60, 70, 80, 90, 100].
        // NthFromEnd=1 → skip 100, return 90.
        predictor.Add(THostIndex(0), TDuration::MilliSeconds(5));
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(90),
            predictor.Predict(0));

        // Push 95ms — evicts 20ms.
        // New sorted set: [5, 30, 40, 50, 60, 70, 80, 90, 95, 100].
        // NthFromEnd=1 → skip 100, return 95.
        predictor.Add(THostIndex(0), TDuration::MilliSeconds(95));
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(95),
            predictor.Predict(0));
    }

    Y_UNIT_TEST(PredictWithMaskIncludingUnseenHost)
    {
        TTimePredictor predictor(
            TimePredictionHistorySize,
            TimePredictionNthFromEnd);

        predictor.Add(THostIndex(0), TDuration::MilliSeconds(100));
        predictor.Add(THostIndex(0), TDuration::MilliSeconds(200));

        // Host 1 has no data. Mask includes both.
        auto mask = THostMask::MakeOne(0).Include(THostMask::MakeOne(1));

        // max(Predict(0), Predict(1)) = max(100, 0) = 100.
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(100),
            predictor.Predict(mask));
    }

    Y_UNIT_TEST(PredictWithMaskIncludingOutOfRangeHost)
    {
        TTimePredictor predictor(
            TimePredictionHistorySize,
            TimePredictionNthFromEnd);

        predictor.Add(THostIndex(0), TDuration::MilliSeconds(100));
        predictor.Add(THostIndex(0), TDuration::MilliSeconds(200));

        // Host 20 is beyond History.size(), never added.
        auto mask = THostMask::MakeOne(0).Include(THostMask::MakeOne(20));

        // max(100, 0) = 100.
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(100),
            predictor.Predict(mask));
    }

    Y_UNIT_TEST(ZeroDurationsAreHandledCorrectly)
    {
        TTimePredictor predictor(
            TimePredictionHistorySize,
            TimePredictionNthFromEnd);

        predictor.Add(THostIndex(0), TDuration());
        predictor.Add(THostIndex(0), TDuration());

        // Sorted: [0, 0]. NthFromEnd=1 → skip last 0, return 0.
        UNIT_ASSERT_VALUES_EQUAL(TDuration(), predictor.Predict(0));
    }

    Y_UNIT_TEST(MixedZeroAndNonZeroDurations)
    {
        TTimePredictor predictor(
            TimePredictionHistorySize,
            TimePredictionNthFromEnd);

        predictor.Add(THostIndex(0), TDuration());
        predictor.Add(THostIndex(0), TDuration::MilliSeconds(50));
        predictor.Add(THostIndex(0), TDuration::MilliSeconds(100));

        // Sorted: [0, 50, 100]. NthFromEnd=1 → skip 100, return 50.
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(50),
            predictor.Predict(0));
    }

    Y_UNIT_TEST(NthFromEndZeroReturnsMax)
    {
        // nthFromEnd=0 means return the absolute maximum (100th percentile).
        TTimePredictor predictor(TimePredictionHistorySize, 0);

        predictor.Add(THostIndex(0), TDuration::MilliSeconds(50));
        predictor.Add(THostIndex(0), TDuration::MilliSeconds(300));
        predictor.Add(THostIndex(0), TDuration::MilliSeconds(100));

        // Sorted: [50, 100, 300]. NthFromEnd=0 → return 300.
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(300),
            predictor.Predict(0));
    }

    Y_UNIT_TEST(NthFromEndTwo)
    {
        // nthFromEnd=2 skips the two largest.
        TTimePredictor predictor(TimePredictionHistorySize, 2);

        predictor.Add(THostIndex(0), TDuration::MilliSeconds(10));
        predictor.Add(THostIndex(0), TDuration::MilliSeconds(20));
        predictor.Add(THostIndex(0), TDuration::MilliSeconds(30));
        predictor.Add(THostIndex(0), TDuration::MilliSeconds(40));

        // Sorted: [10, 20, 30, 40]. Skip 40, 30 → return 20.
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(20),
            predictor.Predict(0));
    }

    Y_UNIT_TEST(NthFromEndExceedsSizeReturnsZero)
    {
        // nthFromEnd=5, but only 3 elements → returns zero.
        TTimePredictor predictor(TimePredictionHistorySize, 5);

        predictor.Add(THostIndex(0), TDuration::MilliSeconds(10));
        predictor.Add(THostIndex(0), TDuration::MilliSeconds(20));
        predictor.Add(THostIndex(0), TDuration::MilliSeconds(30));

        UNIT_ASSERT_VALUES_EQUAL(TDuration(), predictor.Predict(0));
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
