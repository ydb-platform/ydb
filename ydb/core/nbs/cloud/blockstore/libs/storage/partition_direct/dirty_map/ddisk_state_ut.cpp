#include "ddisk_state.h"

#include "block_field_serializer.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/dirty_map.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

namespace {
struct TTestBlockFieldMonitor: public IBehindAheadMonitor
{
    void OnBehindAheadChanged() override
    {
        ++BehindAheadGeneration;
    }

    ui64 BehindAheadGeneration = 0;
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////
Y_UNIT_TEST_SUITE(TDDiskStateTest)
{
    // A range missed while the DDisk was lagging is recorded in the Behind
    // field. Once the DDisk stops lagging and the same range is flushed
    // successfully, AddAhead must move it out of Behind and into Ahead (the
    // BehindField.Remove step): the range is now up-to-date, not outdated.
    Y_UNIT_TEST(ShouldMoveRangeFromBehindToAheadOnLateFlush)
    {
        TTestBlockFieldMonitor testBlockFieldMonitor;
        TDDiskState ddisk;
        // Fresh DDisk (operational 5 < total 100) => tracking enabled.
        ddisk.Init(
            &testBlockFieldMonitor,
            /*totalBlockCount=*/100,
            /*operationalBlockCount=*/40);
        UNIT_ASSERT_VALUES_EQUAL(true, ddisk.IsTrackingEnabled());
        UNIT_ASSERT_VALUES_EQUAL("[40..99]", ddisk.DebugPrintBehind());

        const auto range = TBlockRange64::WithLength(50, 10);
        ddisk.RangeSynced(range);
        UNIT_ASSERT_VALUES_EQUAL("[40..49][60..99]", ddisk.DebugPrintBehind());

        // While lagging, a missed flush marks the range as outdated (Behind).
        ddisk.StartLagging();
        ddisk.OnRangeFlushed(range, TDDiskState::EFlushCompletion::Missed);
        UNIT_ASSERT_VALUES_EQUAL("[40..99]", ddisk.DebugPrintBehind());
        UNIT_ASSERT_VALUES_EQUAL("", ddisk.DebugPrintAhead());

        // The DDisk catches up and the same range is flushed successfully.
        // The range must leave Behind and appear in Ahead.
        ddisk.StopLagging();
        ddisk.OnRangeFlushed(range, TDDiskState::EFlushCompletion::Completed);
        UNIT_ASSERT_VALUES_EQUAL("[40..49][60..99]", ddisk.DebugPrintBehind());
        UNIT_ASSERT_VALUES_EQUAL("[50..59]", ddisk.DebugPrintAhead());
    }

    Y_UNIT_TEST(ShouldMoveRangeFromAheadToBehindOnMissedFlush)
    {
        TTestBlockFieldMonitor monitor;
        TDDiskState ddisk;
        ddisk.Init(
            &monitor,
            /*totalBlockCount=*/100,
            /*operationalBlockCount=*/40);

        const auto range = TBlockRange64::WithLength(50, 10);
        ddisk.RangeSynced(range);
        ddisk.OnRangeFlushed(range, TDDiskState::EFlushCompletion::Completed);
        UNIT_ASSERT_VALUES_EQUAL("[50..59]", ddisk.DebugPrintAhead());
        UNIT_ASSERT_VALUES_EQUAL("[40..49][60..99]", ddisk.DebugPrintBehind());

        ddisk.StartLagging();
        ddisk.OnRangeFlushed(range, TDDiskState::EFlushCompletion::Missed);
        UNIT_ASSERT_VALUES_EQUAL("", ddisk.DebugPrintAhead());
        UNIT_ASSERT_VALUES_EQUAL("[40..99]", ddisk.DebugPrintBehind());
    }

    // Save() chooses a compact encoding for Ahead/Behind; Load() must restore
    // exactly the same ranges. An empty DDisk produces an empty proto and loads
    // back to empty.
    Y_UNIT_TEST(ShouldSaveAndLoadAheadAndBehind)
    {
        TTestBlockFieldMonitor monitor;
        TDDiskState source;
        source.Init(
            &monitor,
            /*totalBlockCount=*/100,
            /*operationalBlockCount=*/5);

        // Populate Behind via a missed flush while lagging.
        source.StartLagging();
        source.OnRangeFlushed(
            TBlockRange64::WithLength(10, 10),
            TDDiskState::EFlushCompletion::Missed);   // Behind = [10..19]

        // Populate Ahead via a successful flush after stopping lagging.
        source.StopLagging();
        source.OnRangeFlushed(
            TBlockRange64::WithLength(30, 5),
            TDDiskState::EFlushCompletion::Completed);   // Ahead = [30..34]

        // --- Save ---
        TDDiskStateProto proto;
        source.Save(&proto);

        // --- Load into a fresh DDisk ---
        TTestBlockFieldMonitor monitor2;
        TDDiskState target;
        target.Init(
            &monitor2,
            /*totalBlockCount=*/100,
            /*operationalBlockCount=*/5);
        target.Load(proto);

        UNIT_ASSERT_VALUES_EQUAL(
            source.DebugPrintBehind(),
            target.DebugPrintBehind());
        UNIT_ASSERT_VALUES_EQUAL(
            source.DebugPrintAhead(),
            target.DebugPrintAhead());

        // --- Empty DDisk round-trip ---
        TTestBlockFieldMonitor monitor3;
        TDDiskState empty;
        empty.Init(
            &monitor3,
            /*totalBlockCount=*/100,
            /*operationalBlockCount=*/100);

        TDDiskStateProto emptyProto;
        empty.Save(&emptyProto);
        UNIT_ASSERT(
            emptyProto.GetAhead().GetEncodingCase() ==
            TBlockFieldProto::ENCODING_NOT_SET);
        UNIT_ASSERT(
            emptyProto.GetBehind().GetEncodingCase() ==
            TBlockFieldProto::ENCODING_NOT_SET);

        TTestBlockFieldMonitor monitor4;
        TDDiskState loaded;
        loaded.Init(
            &monitor4,
            /*totalBlockCount=*/100,
            /*operationalBlockCount=*/100);
        loaded.Load(emptyProto);
        UNIT_ASSERT_VALUES_EQUAL("", loaded.DebugPrintBehind());
        UNIT_ASSERT_VALUES_EQUAL("", loaded.DebugPrintAhead());
    }

    Y_UNIT_TEST(ShouldPreferLoadedBehindState)
    {
        TBlockRangeField ahead;
        ahead.Add(TBlockRange64::WithLength(50, 10));

        TBlockRangeField behind;
        behind.Add(TBlockRange64::WithLength(10, 10));

        TDDiskStateProto proto;
        SaveBlockField(ahead, 100, proto.MutableAhead());
        SaveBlockField(behind, 100, proto.MutableBehind());

        TTestBlockFieldMonitor monitor;
        TDDiskState ddisk;
        ddisk.Init(
            &monitor,
            /*totalBlockCount=*/100,
            /*operationalBlockCount=*/40);
        ddisk.Load(proto);

        UNIT_ASSERT_VALUES_EQUAL("[50..59]", ddisk.DebugPrintAhead());
        UNIT_ASSERT_VALUES_EQUAL("[10..19]", ddisk.DebugPrintBehind());
    }

    Y_UNIT_TEST(ShouldKeepFreshTailWhenLoadedBehindIsEmpty)
    {
        TBlockRangeField ahead;
        ahead.Add(TBlockRange64::WithLength(50, 10));

        TDDiskStateProto proto;
        SaveBlockField(ahead, 100, proto.MutableAhead());

        TTestBlockFieldMonitor monitor;
        TDDiskState ddisk;
        ddisk.Init(
            &monitor,
            /*totalBlockCount=*/100,
            /*operationalBlockCount=*/40);
        ddisk.Load(proto);

        UNIT_ASSERT_VALUES_EQUAL("[50..59]", ddisk.DebugPrintAhead());
        UNIT_ASSERT_VALUES_EQUAL("[40..49][60..99]", ddisk.DebugPrintBehind());
    }

    Y_UNIT_TEST(ShouldClearAheadAndBehindWhenSwitchedOffline)
    {
        TTestBlockFieldMonitor monitor;
        TDDiskState ddisk;
        ddisk.Init(
            &monitor,
            /*totalBlockCount=*/100,
            /*operationalBlockCount=*/5);

        ddisk.StartLagging();
        ddisk.OnRangeFlushed(
            TBlockRange64::WithLength(10, 10),
            TDDiskState::EFlushCompletion::Missed);
        ddisk.StopLagging();
        ddisk.OnRangeFlushed(
            TBlockRange64::WithLength(30, 5),
            TDDiskState::EFlushCompletion::Completed);

        UNIT_ASSERT_VALUES_EQUAL("[5..29][35..99]", ddisk.DebugPrintBehind());
        UNIT_ASSERT_VALUES_EQUAL("[30..34]", ddisk.DebugPrintAhead());

        ddisk.SwitchOffline();

        UNIT_ASSERT_VALUES_EQUAL(
            TDDiskState::EState::Disabled,
            ddisk.GetState());
        UNIT_ASSERT_VALUES_EQUAL(false, ddisk.IsTrackingEnabled());
        UNIT_ASSERT_VALUES_EQUAL("", ddisk.DebugPrintBehind());
        UNIT_ASSERT_VALUES_EQUAL("", ddisk.DebugPrintAhead());
        UNIT_ASSERT_VALUES_EQUAL(0, ddisk.GetBehindSegmentsStat().Count);
        UNIT_ASSERT_VALUES_EQUAL(0, ddisk.GetAheadSegmentsStat().Count);
    }

    // HasBehindOverlapping: false when empty, true when the query overlaps
    // Behind, false when the query is disjoint from Behind.
    Y_UNIT_TEST(HasBehindOverlapping)
    {
        TTestBlockFieldMonitor monitor;
        TDDiskState ddisk;
        ddisk.Init(
            &monitor,
            /*totalBlockCount=*/100,
            /*operationalBlockCount=*/100);

        // Empty Behind – always false.
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            ddisk.HasBehindOverlapping(TBlockRange64::WithLength(0, 20)));

        // Populate Behind = [10..19].
        ddisk.StartLagging();
        ddisk.OnRangeFlushed(
            TBlockRange64::WithLength(10, 10),
            TDDiskState::EFlushCompletion::Missed);

        // Ranges that DO overlap.
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            ddisk.HasBehindOverlapping(
                TBlockRange64::WithLength(12, 5)));   // fully inside
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            ddisk.HasBehindOverlapping(
                TBlockRange64::WithLength(5, 10)));   // overlaps left edge
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            ddisk.HasBehindOverlapping(
                TBlockRange64::WithLength(15, 10)));   // overlaps right edge

        // Ranges that do NOT overlap.
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            ddisk.HasBehindOverlapping(
                TBlockRange64::WithLength(0, 10)));   // before Behind
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            ddisk.HasBehindOverlapping(
                TBlockRange64::WithLength(20, 5)));   // after Behind
    }

    // IBehindAheadMonitor is notified on Behind/Ahead changes and NOT notified
    // when the field does not actually change (already covered or empty sync).
    Y_UNIT_TEST(MonitorNotifications)
    {
        TTestBlockFieldMonitor monitor;
        TDDiskState ddisk;
        ddisk.Init(
            &monitor,
            /*totalBlockCount=*/100,
            /*operationalBlockCount=*/100);

        UNIT_ASSERT_VALUES_EQUAL(0u, monitor.BehindAheadGeneration);

        // First missed flush → Behind changes → monitor called.
        ddisk.StartLagging();
        ddisk.OnRangeFlushed(
            TBlockRange64::WithLength(10, 10),
            TDDiskState::EFlushCompletion::Missed);
        UNIT_ASSERT_VALUES_EQUAL(1u, monitor.BehindAheadGeneration);

        // Identical range already covered → no change → monitor NOT called.
        ddisk.OnRangeFlushed(
            TBlockRange64::WithLength(10, 10),
            TDDiskState::EFlushCompletion::Missed);
        UNIT_ASSERT_VALUES_EQUAL(1u, monitor.BehindAheadGeneration);

        // RangeSynced removes the range from Behind → monitor called.
        ddisk.RangeSynced(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(2u, monitor.BehindAheadGeneration);
        UNIT_ASSERT_VALUES_EQUAL("", ddisk.DebugPrintBehind());

        // Syncing an empty field → no change → monitor NOT called.
        ddisk.RangeSynced(TBlockRange64::WithLength(0, 10));
        UNIT_ASSERT_VALUES_EQUAL(2u, monitor.BehindAheadGeneration);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
