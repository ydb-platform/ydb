#include "ddisk_state.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDDiskStateTest)
{
    // A range missed while the DDisk was lagging is recorded in the Behind
    // field. Once the DDisk stops lagging and the same range is flushed
    // successfully, AddAhead must move it out of Behind and into Ahead (the
    // BehindField.Remove step): the range is now up-to-date, not outdated.
    Y_UNIT_TEST(ShouldMoveRangeFromBehindToAheadOnLateFlush)
    {
        TDDiskState ddisk;
        // Fresh DDisk (operational 5 < total 100) => tracking enabled.
        ddisk.Init(/*totalBlockCount=*/100, /*operationalBlockCount=*/5);
        UNIT_ASSERT_VALUES_EQUAL(true, ddisk.IsTrackingEnabled());

        // While lagging, a missed flush marks the range as outdated (Behind).
        ddisk.StartLagging();
        ddisk.OnRangeFlushed(
            TBlockRange64::WithLength(10, 10),
            TDDiskState::EFlushCompletion::Missed);
        UNIT_ASSERT_VALUES_EQUAL("[10..19]", ddisk.DebugPrintBehind());
        UNIT_ASSERT_VALUES_EQUAL("", ddisk.DebugPrintAhead());

        // The DDisk catches up and the same range is flushed successfully.
        // The range must leave Behind and appear in Ahead.
        ddisk.StopLagging();
        ddisk.OnRangeFlushed(
            TBlockRange64::WithLength(10, 10),
            TDDiskState::EFlushCompletion::Completed);
        UNIT_ASSERT_VALUES_EQUAL("", ddisk.DebugPrintBehind());
        UNIT_ASSERT_VALUES_EQUAL("[10..19]", ddisk.DebugPrintAhead());
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
