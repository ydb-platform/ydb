#include "watermark_tracker.h"

#include <util/system/types.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TWatermarkTrackerTest) {
    Y_UNIT_TEST(WhenNotGranularShouldNotMoveWatermark) {
        TWatermarkTracker tracker(15, 10);
        UNIT_ASSERT_EQUAL(tracker.HandleNextEventTime(25), 10);
        UNIT_ASSERT_EQUAL(tracker.HandleNextEventTime(26), std::nullopt);
        UNIT_ASSERT_EQUAL(tracker.HandleNextEventTime(30), std::nullopt);
        UNIT_ASSERT_EQUAL(tracker.HandleNextEventTime(34), std::nullopt);
    }

    Y_UNIT_TEST(WhenGranularShouldMoveWatermark) {
        TWatermarkTracker tracker(15, 10);
        UNIT_ASSERT_EQUAL(tracker.HandleNextEventTime(35), 20);
        UNIT_ASSERT_EQUAL(tracker.HandleNextEventTime(45), 30);
    }
}

} // NMiniKQL
} // NKikimr