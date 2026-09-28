#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/vchunk_counters.h>
#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/vchunk_stats.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

namespace {

NMonitoring::TDynamicCounterPtr MakeRoot()
{
    return MakeIntrusive<NMonitoring::TDynamicCounters>();
}

void AddReplies(TVChunkStats& stats, EVChunkOperation operation, ui64 ok)
{
    for (ui64 i = 0; i < ok; ++i) {
        stats.RequestFinished(operation, true);
    }
}

void AddErrors(TVChunkStats& stats, EVChunkOperation operation, ui64 err)
{
    for (ui64 i = 0; i < err; ++i) {
        stats.RequestFinished(operation, false);
    }
}

ui64 GetOpCounter(
    NMonitoring::TDynamicCounterPtr root,
    const char* operation,
    const char* name,
    bool derivative)
{
    return root->GetSubgroup("operation", operation)
        ->GetCounter(name, derivative)
        ->Val();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVChunkStatsTest)
{
    Y_UNIT_TEST(ShouldAccumulateSumsAndMinNonZeroLsn)
    {
        TVChunkStats first;
        AddReplies(first, EVChunkOperation::Write, 3);
        AddErrors(first, EVChunkOperation::Write, 1);
        first.UpdatePending(EVChunkOperation::Flush, 4);
        first.UpdateMinLsn(EVChunkOperation::Flush, 10);
        first.UpdateMinLsn(EVChunkOperation::Erase, 0);

        TVChunkStats second;
        AddReplies(second, EVChunkOperation::Write, 2);
        second.UpdatePending(EVChunkOperation::Flush, 1);
        second.UpdateMinLsn(EVChunkOperation::Flush, 7);
        second.UpdateMinLsn(EVChunkOperation::Erase, 20);

        first.Accumulate(second);

        const auto& write = first.Get(EVChunkOperation::Write);
        UNIT_ASSERT_VALUES_EQUAL(5, write.ReplyOk);
        UNIT_ASSERT_VALUES_EQUAL(1, write.ReplyErr);

        const auto& flush = first.Get(EVChunkOperation::Flush);
        UNIT_ASSERT_VALUES_EQUAL(5, flush.Pending);
        UNIT_ASSERT_VALUES_EQUAL(7, flush.MinLsn);

        const auto& erase = first.Get(EVChunkOperation::Erase);
        UNIT_ASSERT_VALUES_EQUAL(20, erase.MinLsn);
    }

    Y_UNIT_TEST(ShouldTreatZeroMinLsnAsAbsent)
    {
        TVChunkStats first;
        first.UpdateMinLsn(EVChunkOperation::Flush, 0);

        TVChunkStats second;
        second.UpdateMinLsn(EVChunkOperation::Flush, 42);

        first.Accumulate(second);
        UNIT_ASSERT_VALUES_EQUAL(42, first.Get(EVChunkOperation::Flush).MinLsn);

        TVChunkStats third;
        third.UpdateMinLsn(EVChunkOperation::Flush, 0);
        first.Accumulate(third);
        UNIT_ASSERT_VALUES_EQUAL(42, first.Get(EVChunkOperation::Flush).MinLsn);
    }

    Y_UNIT_TEST(ShouldReportZeroWhenNothingHappened)
    {
        TVChunkStats stats;
        UNIT_ASSERT(stats.IsZero());
        stats.RequestFinished(EVChunkOperation::Read, true);
        UNIT_ASSERT(!stats.IsZero());
        UNIT_ASSERT(!stats.Get(EVChunkOperation::Read).IsZero());
        UNIT_ASSERT(stats.Get(EVChunkOperation::Write).IsZero());
    }
}

Y_UNIT_TEST_SUITE(TVChunkCountersTest)
{
    Y_UNIT_TEST(ShouldNotCrashWithNullParent)
    {
        TVChunkCounters counters(nullptr);
        TVChunkStats stats;
        AddReplies(stats, EVChunkOperation::Write, 2);
        counters.Publish(stats);
        counters.Publish(stats);
    }

    Y_UNIT_TEST(ShouldPublishDeltasAndAbsoluteGauges)
    {
        auto root = MakeRoot();
        TVChunkCounters counters(root);

        TVChunkStats first;
        AddReplies(first, EVChunkOperation::Write, 10);
        AddErrors(first, EVChunkOperation::Write, 2);
        first.UpdatePending(EVChunkOperation::Flush, 5);
        first.UpdateMinLsn(EVChunkOperation::Flush, 100);
        counters.Publish(first);

        UNIT_ASSERT_VALUES_EQUAL(
            10,
            GetOpCounter(root, "Write", "ReplyOk", true));
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            GetOpCounter(root, "Write", "ReplyErr", true));
        UNIT_ASSERT_VALUES_EQUAL(
            5,
            GetOpCounter(root, "Flush", "Pending", false));
        UNIT_ASSERT_VALUES_EQUAL(
            100,
            GetOpCounter(root, "Flush", "MinLsn", false));

        TVChunkStats second;
        AddReplies(second, EVChunkOperation::Write, 15);
        AddErrors(second, EVChunkOperation::Write, 2);
        second.UpdatePending(EVChunkOperation::Flush, 1);
        second.UpdateMinLsn(EVChunkOperation::Flush, 80);
        counters.Publish(second);

        UNIT_ASSERT_VALUES_EQUAL(
            15,
            GetOpCounter(root, "Write", "ReplyOk", true));
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            GetOpCounter(root, "Write", "ReplyErr", true));
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            GetOpCounter(root, "Flush", "Pending", false));
        UNIT_ASSERT_VALUES_EQUAL(
            80,
            GetOpCounter(root, "Flush", "MinLsn", false));
    }

    Y_UNIT_TEST(ShouldSkipDerivativeDecrease)
    {
        auto root = MakeRoot();
        TVChunkCounters counters(root);

        TVChunkStats high;
        AddReplies(high, EVChunkOperation::Read, 20);
        counters.Publish(high);
        UNIT_ASSERT_VALUES_EQUAL(
            20,
            GetOpCounter(root, "Read", "ReplyOk", true));

        TVChunkStats dropped;
        AddReplies(dropped, EVChunkOperation::Read, 12);
        counters.Publish(dropped);
        UNIT_ASSERT_VALUES_EQUAL(
            20,
            GetOpCounter(root, "Read", "ReplyOk", true));

        TVChunkStats recovered;
        AddReplies(recovered, EVChunkOperation::Read, 14);
        counters.Publish(recovered);
        UNIT_ASSERT_VALUES_EQUAL(
            22,
            GetOpCounter(root, "Read", "ReplyOk", true));
    }
}

}   // namespace NYdb::NBS::NBlockStore
