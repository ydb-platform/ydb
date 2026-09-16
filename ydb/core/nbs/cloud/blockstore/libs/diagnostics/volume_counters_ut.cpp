#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/volume_counters.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/metrics/histogram_snapshot.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

ui64 GetTotalCount(const NMonitoring::IHistogramSnapshot& snapshot)
{
    ui64 total = 0;
    for (ui32 i = 0; i < snapshot.Count(); ++i) {
        total += snapshot.Value(i);
    }
    return total;
}

// Explicit histogram buckets are (prev, bound], so a value belongs to the
// first bucket whose upper bound is not less than the value.
ui64 GetCountInBucket(
    const NMonitoring::IHistogramSnapshot& snapshot,
    double valueMs)
{
    for (ui32 i = 0; i < snapshot.Count(); ++i) {
        if (valueMs <= snapshot.UpperBound(i)) {
            return snapshot.Value(i);
        }
    }
    return snapshot.Value(snapshot.Count() - 1);
}

NMonitoring::TDynamicCounterPtr MakeRoot()
{
    return MakeIntrusive<NMonitoring::TDynamicCounters>();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(VolumeCountersTest)
{
    Y_UNIT_TEST(ShouldNotCrashWithNullParent)
    {
        TVolumeRequestCounters counters(nullptr);
        counters.RequestStarted(1024);
        counters.RequestFinished(true, TDuration::MicroSeconds(1500));
        counters.RequestFinished(false, TDuration::Zero());
        UNIT_ASSERT(!counters.GetRequestTime());
    }

    Y_UNIT_TEST(ShouldCountRequestsAndBytes)
    {
        auto root = MakeRoot();
        TVolumeRequestCounters counters(root);

        counters.RequestStarted(1024);
        counters.RequestStarted(2048);

        UNIT_ASSERT_VALUES_EQUAL(2, root->GetCounter("Requests", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(3072, root->GetCounter("Bytes", true)->Val());
    }

    Y_UNIT_TEST(ShouldCountInflightRequests)
    {
        auto root = MakeRoot();
        TVolumeRequestCounters counters(root);

        // Inflight is a gauge (non-cumulative) counter.
        UNIT_ASSERT_VALUES_EQUAL(0, root->GetCounter("Inflight", false)->Val());

        counters.RequestStarted(1024);
        counters.RequestStarted(2048);

        UNIT_ASSERT_VALUES_EQUAL(2, root->GetCounter("Inflight", false)->Val());

        counters.RequestFinished(true, TDuration::MilliSeconds(1));

        UNIT_ASSERT_VALUES_EQUAL(1, root->GetCounter("Inflight", false)->Val());

        counters.RequestFinished(false, TDuration::MilliSeconds(1));

        UNIT_ASSERT_VALUES_EQUAL(0, root->GetCounter("Inflight", false)->Val());
    }

    Y_UNIT_TEST(ShouldCountOkAndErrReplies)
    {
        auto root = MakeRoot();
        TVolumeRequestCounters counters(root);

        counters.RequestFinished(true, TDuration::MilliSeconds(1));
        counters.RequestFinished(true, TDuration::MilliSeconds(1));
        counters.RequestFinished(false, TDuration::MilliSeconds(1));

        UNIT_ASSERT_VALUES_EQUAL(2, root->GetCounter("ReplyOk", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, root->GetCounter("ReplyErr", true)->Val());
    }

    Y_UNIT_TEST(ShouldNotRecordHistogramWhenDurationIsZero)
    {
        auto root = MakeRoot();
        TVolumeRequestCounters counters(root);

        counters.RequestFinished(true, TDuration::Zero());
        counters.RequestFinished(false, TDuration::Zero());

        auto histogram = counters.GetRequestTime();
        UNIT_ASSERT(histogram);
        UNIT_ASSERT_VALUES_EQUAL(0, GetTotalCount(*histogram->Snapshot()));

        // Replies are still counted.
        UNIT_ASSERT_VALUES_EQUAL(1, root->GetCounter("ReplyOk", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, root->GetCounter("ReplyErr", true)->Val());
    }

    Y_UNIT_TEST(ShouldRecordLatencyHistogram)
    {
        auto root = MakeRoot();
        TVolumeRequestCounters counters(root);

        counters.RequestFinished(true, TDuration::MicroSeconds(500));
        counters.RequestFinished(true, TDuration::MicroSeconds(1500));
        counters.RequestFinished(false, TDuration::MilliSeconds(50));

        auto histogram = counters.GetRequestTime();
        UNIT_ASSERT(histogram);

        auto snapshot = histogram->Snapshot();
        UNIT_ASSERT_VALUES_EQUAL(3, GetTotalCount(*snapshot));

        UNIT_ASSERT_VALUES_EQUAL(1, GetCountInBucket(*snapshot, 0.5));
        UNIT_ASSERT_VALUES_EQUAL(1, GetCountInBucket(*snapshot, 1.5));
        UNIT_ASSERT_VALUES_EQUAL(1, GetCountInBucket(*snapshot, 50.0));
    }

    Y_UNIT_TEST(ShouldRouteCountersPerOperation)
    {
        auto root = MakeRoot();
        TVolumeCounters counters(root);

        counters.RequestStarted(EBlockStoreRequest::ReadBlocks, 128);
        counters.RequestStarted(EBlockStoreRequest::WriteBlocks, 256);
        counters.RequestStarted(EBlockStoreRequest::WriteBlocks, 512);
        counters.RequestStarted(EBlockStoreRequest::ZeroBlocks, 64);

        counters.RequestFinished(
            EBlockStoreRequest::ReadBlocks,
            true,
            TDuration::MilliSeconds(2));
        counters.RequestFinished(
            EBlockStoreRequest::WriteBlocks,
            false,
            TDuration::MilliSeconds(3));
        counters.RequestFinished(
            EBlockStoreRequest::WriteBlocks,
            true,
            TDuration::MilliSeconds(100));
        counters.RequestFinished(
            EBlockStoreRequest::ZeroBlocks,
            true,
            TDuration::MilliSeconds(5));

        auto readGroup = root->GetSubgroup("operation", "ReadBlocks");
        auto writeGroup = root->GetSubgroup("operation", "WriteBlocks");
        auto zeroGroup = root->GetSubgroup("operation", "ZeroBlocks");

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            readGroup->GetCounter("Requests", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            writeGroup->GetCounter("Requests", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            zeroGroup->GetCounter("Requests", true)->Val());

        UNIT_ASSERT_VALUES_EQUAL(
            128,
            readGroup->GetCounter("Bytes", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            768,
            writeGroup->GetCounter("Bytes", true)->Val());

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            readGroup->GetCounter("ReplyOk", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            writeGroup->GetCounter("ReplyOk", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            writeGroup->GetCounter("ReplyErr", true)->Val());

        auto readHistogram = readGroup->FindHistogram("RequestTimeMs");
        auto writeHistogram = writeGroup->FindHistogram("RequestTimeMs");
        UNIT_ASSERT(readHistogram);
        UNIT_ASSERT(writeHistogram);

        auto readSnapshot = readHistogram->Snapshot();
        auto writeSnapshot = writeHistogram->Snapshot();

        UNIT_ASSERT_VALUES_EQUAL(1, GetTotalCount(*readSnapshot));
        UNIT_ASSERT_VALUES_EQUAL(2, GetTotalCount(*writeSnapshot));

        UNIT_ASSERT_VALUES_EQUAL(1, GetCountInBucket(*readSnapshot, 2.0));
        UNIT_ASSERT_VALUES_EQUAL(1, GetCountInBucket(*writeSnapshot, 3.0));
        UNIT_ASSERT_VALUES_EQUAL(1, GetCountInBucket(*writeSnapshot, 100.0));
    }

    Y_UNIT_TEST(ShouldCountInflightPerOperation)
    {
        auto root = MakeRoot();
        TVolumeCounters counters(root);

        auto readGroup = root->GetSubgroup("operation", "ReadBlocks");
        auto writeGroup = root->GetSubgroup("operation", "WriteBlocks");

        // Start two reads and one write.
        counters.RequestStarted(EBlockStoreRequest::ReadBlocks, 128);
        counters.RequestStarted(EBlockStoreRequest::ReadBlocks, 256);
        counters.RequestStarted(EBlockStoreRequest::WriteBlocks, 512);

        UNIT_ASSERT_VALUES_EQUAL(
            2,
            readGroup->GetCounter("Inflight", false)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            writeGroup->GetCounter("Inflight", false)->Val());

        // Finish one read.
        counters.RequestFinished(
            EBlockStoreRequest::ReadBlocks,
            true,
            TDuration::MilliSeconds(1));

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            readGroup->GetCounter("Inflight", false)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            writeGroup->GetCounter("Inflight", false)->Val());

        // Finish the write.
        counters.RequestFinished(
            EBlockStoreRequest::WriteBlocks,
            true,
            TDuration::MilliSeconds(1));

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            readGroup->GetCounter("Inflight", false)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            writeGroup->GetCounter("Inflight", false)->Val());

        // Finish the remaining read.
        counters.RequestFinished(
            EBlockStoreRequest::ReadBlocks,
            false,
            TDuration::MilliSeconds(1));

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            readGroup->GetCounter("Inflight", false)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            writeGroup->GetCounter("Inflight", false)->Val());
    }
}

}   // namespace NYdb::NBS::NBlockStore
