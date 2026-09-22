#include "ddisk_state.h"

#include "block_field_serializer.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/dirty_map.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr ui16 TestBlockCount = 32768;

struct TTestBlockFieldMonitor: public IBehindMonitor
{
    void OnBehindChanged() override
    {
        ++StateGeneration;
    }

    ui64 StateGeneration = 0;
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////
Y_UNIT_TEST_SUITE(TDDiskStateTest)
{
    Y_UNIT_TEST(ShouldRemoveRangeFromBehindOnLateFlush)
    {
        TTestBlockFieldMonitor testBlockFieldMonitor;
        TDDiskState ddisk(CreateArenaAllocator(), TestBlockCount);
        // Fresh DDisk (operational 5 < total 100) => tracking enabled.
        ddisk.Init(
            &testBlockFieldMonitor,
            /*totalBlockCount=*/100,
            /*operationalBlockCount=*/40);
        UNIT_ASSERT_VALUES_EQUAL(true, ddisk.IsTrackingEnabled());
        UNIT_ASSERT_VALUES_EQUAL("[40..99]", ddisk.DebugPrintBehind());

        const auto range = TBlockRange16::WithLength(50, 10);
        ddisk.RangeSynced(range);
        UNIT_ASSERT_VALUES_EQUAL("[40..49][60..99]", ddisk.DebugPrintBehind());

        // While lagging, a missed flush marks the range as outdated (Behind).
        ddisk.StartLagging();
        ddisk.OnRangeFlushed(range, TDDiskState::EFlushCompletion::Missed);
        UNIT_ASSERT_VALUES_EQUAL("[40..99]", ddisk.DebugPrintBehind());

        // The DDisk catches up and the same range is flushed successfully.
        // The range must leave Behind.
        ddisk.StopLagging();
        ddisk.OnRangeFlushed(range, TDDiskState::EFlushCompletion::Completed);
        UNIT_ASSERT_VALUES_EQUAL("[40..49][60..99]", ddisk.DebugPrintBehind());
    }

    Y_UNIT_TEST(ShouldAddPreviouslyFlushedRangeToBehindOnMissedFlush)
    {
        TTestBlockFieldMonitor monitor;
        TDDiskState ddisk(CreateArenaAllocator(), TestBlockCount);
        ddisk.Init(
            &monitor,
            /*totalBlockCount=*/100,
            /*operationalBlockCount=*/40);

        const auto range = TBlockRange16::WithLength(50, 10);
        ddisk.RangeSynced(range);
        ddisk.OnRangeFlushed(range, TDDiskState::EFlushCompletion::Completed);
        UNIT_ASSERT_VALUES_EQUAL("[40..49][60..99]", ddisk.DebugPrintBehind());

        ddisk.StartLagging();
        ddisk.OnRangeFlushed(range, TDDiskState::EFlushCompletion::Missed);
        UNIT_ASSERT_VALUES_EQUAL("[40..99]", ddisk.DebugPrintBehind());
    }

    // A sync that completes while a DDisk is lagging is stale and must not
    // clear a range dirtied after lagging started. Once lagging ends, the
    // range is reported again and can be synchronized successfully.
    Y_UNIT_TEST(ShouldIgnoreStaleSyncWhileLagging)
    {
        TTestBlockFieldMonitor monitor;
        TDDiskState ddisk(CreateArenaAllocator(), TestBlockCount);
        ddisk.Init(
            &monitor,
            /*totalBlockCount=*/100,
            /*operationalBlockCount=*/40);

        const auto dirtyRange = TBlockRange16::WithLength(50, 10);

        // The DDisk starts lagging and gets dirty. The lagging state ends and
        // the dirty range is synchronized successfully. The readable prefix
        // remains at 40 because only a part of the fresh tail was synchronized.
        ddisk.StartLagging();
        ddisk.OnRangeFlushed(dirtyRange, TDDiskState::EFlushCompletion::Missed);
        ddisk.StopLagging();
        ddisk.RangeSynced(dirtyRange);
        UNIT_ASSERT_VALUES_EQUAL("[40..49][60..99]", ddisk.DebugPrintBehind());

        // The DDisk starts lagging again. The same range is dirtied again
        // while the readable prefix is still below it.
        ddisk.StartLagging();
        ddisk.OnRangeFlushed(dirtyRange, TDDiskState::EFlushCompletion::Missed);

        // The sync callback is stale while the DDisk is lagging.
        ddisk.RangeSynced(dirtyRange);
        UNIT_ASSERT_VALUES_EQUAL("[40..99]", ddisk.DebugPrintBehind());

        // After lagging ends, the dirty ranges can be synchronized
        // successfully. Adjacent dirty blocks are merged into one range by
        // BehindField.
        ddisk.StopLagging();
        const auto freshRange = ddisk.GetFreshRange();
        UNIT_ASSERT(freshRange.has_value());
        UNIT_ASSERT_VALUES_EQUAL("[40..99]", freshRange->Print());
        ddisk.RangeSynced(*freshRange);

        UNIT_ASSERT_VALUES_EQUAL("", ddisk.DebugPrintBehind());
        UNIT_ASSERT(!ddisk.GetFreshRange().has_value());
    }

    // Save() chooses a compact encoding for Behind; Load() must restore
    // exactly the same ranges. An empty DDisk produces an empty proto and loads
    // back to empty.
    Y_UNIT_TEST(ShouldSaveAndLoadBehind)
    {
        TTestBlockFieldMonitor monitor;
        TDDiskState source(CreateArenaAllocator(), TestBlockCount);
        source.Init(
            &monitor,
            /*totalBlockCount=*/100,
            /*operationalBlockCount=*/5);

        // Populate Behind via a missed flush while lagging.
        source.StartLagging();
        source.OnRangeFlushed(
            TBlockRange16::WithLength(10, 10),
            TDDiskState::EFlushCompletion::Missed);   // Behind = [10..19]

        // Remove a range via a successful flush after stopping lagging.
        source.StopLagging();
        source.OnRangeFlushed(
            TBlockRange16::WithLength(30, 5),
            TDDiskState::EFlushCompletion::Completed);

        // --- Save ---
        TDDiskStateProto proto;
        source.Save(&proto);

        // --- Load into a fresh DDisk ---
        TTestBlockFieldMonitor monitor2;
        TDDiskState target(CreateArenaAllocator(), TestBlockCount);
        target.Init(
            &monitor2,
            /*totalBlockCount=*/100,
            /*operationalBlockCount=*/5);
        target.Load(proto);

        UNIT_ASSERT_VALUES_EQUAL(
            source.DebugPrintBehind(),
            target.DebugPrintBehind());
        // --- Empty DDisk round-trip ---
        TTestBlockFieldMonitor monitor3;
        TDDiskState empty(CreateArenaAllocator(), TestBlockCount);
        empty.Init(
            &monitor3,
            /*totalBlockCount=*/100,
            /*operationalBlockCount=*/100);

        TDDiskStateProto emptyProto;
        empty.Save(&emptyProto);
        UNIT_ASSERT(
            emptyProto.GetBehind().GetEncodingCase() ==
            TBlockFieldProto::ENCODING_NOT_SET);

        TTestBlockFieldMonitor monitor4;
        TDDiskState loaded(CreateArenaAllocator(), TestBlockCount);
        loaded.Init(
            &monitor4,
            /*totalBlockCount=*/100,
            /*operationalBlockCount=*/100);
        loaded.Load(emptyProto);
        UNIT_ASSERT_VALUES_EQUAL("", loaded.DebugPrintBehind());
    }

    Y_UNIT_TEST(ShouldPreferLoadedBehindState)
    {
        TBlockRangeField behind(CreateArenaAllocator(), TestBlockCount);
        behind.Add(TBlockRange16::WithLength(10, 10));

        TDDiskStateProto proto;
        SaveBlockField(behind, proto.MutableBehind());

        TTestBlockFieldMonitor monitor;
        TDDiskState ddisk(CreateArenaAllocator(), TestBlockCount);
        ddisk.Init(
            &monitor,
            /*totalBlockCount=*/100,
            /*operationalBlockCount=*/40);
        ddisk.Load(proto);

        UNIT_ASSERT_VALUES_EQUAL("[10..19]", ddisk.DebugPrintBehind());
    }

    Y_UNIT_TEST(ShouldTreatLoadedEmptyBehindAsAuthoritative)
    {
        TTestBlockFieldMonitor monitor;
        TDDiskState ddisk(CreateArenaAllocator(), TestBlockCount);
        ddisk.Init(
            &monitor,
            /*totalBlockCount=*/100,
            /*operationalBlockCount=*/40);
        ddisk.Load({});

        UNIT_ASSERT_VALUES_EQUAL("", ddisk.DebugPrintBehind());
    }

    Y_UNIT_TEST(ShouldAdvanceReadablePrefixOnCompletedFlush)
    {
        TTestBlockFieldMonitor monitor;
        TDDiskState ddisk(CreateArenaAllocator(), TestBlockCount);
        ddisk.Init(
            &monitor,
            /*totalBlockCount=*/100,
            /*operationalBlockCount=*/40);

        ddisk.OnRangeFlushed(
            TBlockRange16::WithLength(0, 39),
            TDDiskState::EFlushCompletion::Completed);
        UNIT_ASSERT(ddisk.CanReadFromDDisk(TBlockRange16::WithLength(0, 40)));

        ddisk.OnRangeFlushed(
            TBlockRange16::WithLength(35, 10),
            TDDiskState::EFlushCompletion::Completed);
        UNIT_ASSERT(ddisk.CanReadFromDDisk(TBlockRange16::WithLength(0, 45)));
        UNIT_ASSERT(!ddisk.CanReadFromDDisk(TBlockRange16::WithLength(0, 46)));
    }

    Y_UNIT_TEST(ShouldReadOnlyFirstIsland)
    {
        TTestBlockFieldMonitor monitor;
        TDDiskState ddisk(CreateArenaAllocator(), TestBlockCount);
        ddisk.Init(
            &monitor,
            /*totalBlockCount=*/100,
            /*operationalBlockCount=*/20);
        ddisk.RangeSynced(TBlockRange16::WithLength(30, 30));

        UNIT_ASSERT_VALUES_EQUAL("[20..29][60..99]", ddisk.DebugPrintBehind());
        UNIT_ASSERT(ddisk.CanReadFromDDisk(TBlockRange16::WithLength(0, 20)));
        UNIT_ASSERT(!ddisk.CanReadFromDDisk(TBlockRange16::WithLength(30, 30)));
    }

    Y_UNIT_TEST(ShouldClearBehindWhenSwitchedOffline)
    {
        TTestBlockFieldMonitor monitor;
        TDDiskState ddisk(CreateArenaAllocator(), TestBlockCount);
        ddisk.Init(
            &monitor,
            /*totalBlockCount=*/100,
            /*operationalBlockCount=*/5);

        ddisk.StartLagging();
        ddisk.OnRangeFlushed(
            TBlockRange16::WithLength(10, 10),
            TDDiskState::EFlushCompletion::Missed);
        ddisk.StopLagging();
        ddisk.OnRangeFlushed(
            TBlockRange16::WithLength(30, 5),
            TDDiskState::EFlushCompletion::Completed);

        UNIT_ASSERT_VALUES_EQUAL("[5..29][35..99]", ddisk.DebugPrintBehind());

        ddisk.SwitchOffline();

        UNIT_ASSERT_VALUES_EQUAL(
            TDDiskState::EState::Disabled,
            ddisk.GetState());
        UNIT_ASSERT_VALUES_EQUAL(false, ddisk.IsTrackingEnabled());
        UNIT_ASSERT_VALUES_EQUAL("", ddisk.DebugPrintBehind());
        UNIT_ASSERT_VALUES_EQUAL(0, ddisk.GetFreshBlockCount());
        UNIT_ASSERT_VALUES_EQUAL(0, ddisk.GetRottenBlockCount());
    }

    Y_UNIT_TEST(ShouldReportFreshAndRottenBlocksByLaggingState)
    {
        TTestBlockFieldMonitor monitor;
        TDDiskState ddisk(CreateArenaAllocator(), TestBlockCount);
        ddisk.Init(
            &monitor,
            /*totalBlockCount=*/100,
            /*operationalBlockCount=*/40);

        UNIT_ASSERT_VALUES_EQUAL(60, ddisk.GetFreshBlockCount());
        UNIT_ASSERT_VALUES_EQUAL(0, ddisk.GetRottenBlockCount());

        ddisk.OnRangeFlushed(
            TBlockRange16::WithLength(50, 10),
            TDDiskState::EFlushCompletion::Completed);
        UNIT_ASSERT_VALUES_EQUAL(50, ddisk.GetFreshBlockCount());

        ddisk.StartLagging();
        UNIT_ASSERT_VALUES_EQUAL(0, ddisk.GetFreshBlockCount());
        UNIT_ASSERT_VALUES_EQUAL(50, ddisk.GetRottenBlockCount());

        ddisk.OnRangeFlushed(
            TBlockRange16::WithLength(50, 10),
            TDDiskState::EFlushCompletion::Missed);
        UNIT_ASSERT_VALUES_EQUAL(60, ddisk.GetRottenBlockCount());
    }

    // HasBehindOverlapping: false when empty, true when the query overlaps
    // Behind, false when the query is disjoint from Behind.
    Y_UNIT_TEST(HasBehindOverlapping)
    {
        TTestBlockFieldMonitor monitor;
        TDDiskState ddisk(CreateArenaAllocator(), TestBlockCount);
        ddisk.Init(
            &monitor,
            /*totalBlockCount=*/100,
            /*operationalBlockCount=*/100);

        // Empty Behind – always false.
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            ddisk.HasBehindOverlapping(TBlockRange16::WithLength(0, 20)));

        // Populate Behind = [10..19].
        ddisk.StartLagging();
        ddisk.OnRangeFlushed(
            TBlockRange16::WithLength(10, 10),
            TDDiskState::EFlushCompletion::Missed);

        // Ranges that DO overlap.
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            ddisk.HasBehindOverlapping(
                TBlockRange16::WithLength(12, 5)));   // fully inside
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            ddisk.HasBehindOverlapping(
                TBlockRange16::WithLength(5, 10)));   // overlaps left edge
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            ddisk.HasBehindOverlapping(
                TBlockRange16::WithLength(15, 10)));   // overlaps right edge

        // Ranges that do NOT overlap.
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            ddisk.HasBehindOverlapping(
                TBlockRange16::WithLength(0, 10)));   // before Behind
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            ddisk.HasBehindOverlapping(
                TBlockRange16::WithLength(20, 5)));   // after Behind
    }

    // IBehindMonitor is notified on Behind changes and NOT notified
    // when the field does not actually change (already covered or empty sync).
    Y_UNIT_TEST(MonitorNotifications)
    {
        TTestBlockFieldMonitor monitor;
        TDDiskState ddisk(CreateArenaAllocator(), TestBlockCount);
        ddisk.Init(
            &monitor,
            /*totalBlockCount=*/100,
            /*operationalBlockCount=*/100);

        UNIT_ASSERT_VALUES_EQUAL(0u, monitor.StateGeneration);

        // First missed flush → Behind changes → monitor called.
        ddisk.StartLagging();
        ddisk.OnRangeFlushed(
            TBlockRange16::WithLength(10, 10),
            TDDiskState::EFlushCompletion::Missed);
        UNIT_ASSERT_VALUES_EQUAL(1u, monitor.StateGeneration);

        // Identical range already covered → no change → monitor NOT called.
        ddisk.OnRangeFlushed(
            TBlockRange16::WithLength(10, 10),
            TDDiskState::EFlushCompletion::Missed);
        UNIT_ASSERT_VALUES_EQUAL(1u, monitor.StateGeneration);

        // Leave lagging and synchronize the range successfully.
        ddisk.StopLagging();
        ddisk.RangeSynced(TBlockRange16::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(2u, monitor.StateGeneration);
        UNIT_ASSERT_VALUES_EQUAL("", ddisk.DebugPrintBehind());

        // The DDisk starts lagging again and the range becomes dirty again.
        ddisk.StartLagging();
        ddisk.OnRangeFlushed(
            TBlockRange16::WithLength(10, 10),
            TDDiskState::EFlushCompletion::Missed);
        UNIT_ASSERT_VALUES_EQUAL(3u, monitor.StateGeneration);
        UNIT_ASSERT_VALUES_EQUAL("[10..19]", ddisk.DebugPrintBehind());

        // A sync completed while the DDisk was lagging is stale and must be
        // ignored.
        ddisk.RangeSynced(TBlockRange16::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(3u, monitor.StateGeneration);
        UNIT_ASSERT_VALUES_EQUAL("[10..19]", ddisk.DebugPrintBehind());

        // After lagging ends, the same sync can be applied successfully.
        ddisk.StopLagging();
        ddisk.RangeSynced(TBlockRange16::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(4u, monitor.StateGeneration);
        UNIT_ASSERT_VALUES_EQUAL("", ddisk.DebugPrintBehind());

        // Syncing an empty field → no change → monitor NOT called.
        ddisk.RangeSynced(TBlockRange16::WithLength(0, 10));
        UNIT_ASSERT_VALUES_EQUAL(4u, monitor.StateGeneration);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
