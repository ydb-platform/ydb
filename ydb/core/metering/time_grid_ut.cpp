#include <library/cpp/testing/unittest/registar.h>
#include "time_grid.h"

namespace NKikimr {

Y_UNIT_TEST_SUITE(TTimeGridTest) {
    Y_UNIT_TEST(TimeGrid) {
        {
            TTimeGrid grid(TDuration::Seconds(5));

            for (ui32 shift = 0; shift < 5; ++shift) {
                TInstant curTime = TInstant::Hours(100) + TDuration::Seconds(shift);
                auto slot = grid.Get(curTime);

                UNIT_ASSERT_VALUES_EQUAL(slot.Start, TInstant::Hours(100));
                UNIT_ASSERT_VALUES_EQUAL(slot.End, TInstant::Hours(100)  + TDuration::Seconds(4));

                UNIT_ASSERT_VALUES_EQUAL(grid.GetNext(slot).Start, TInstant::Hours(100) + TDuration::Seconds(5));
                UNIT_ASSERT_VALUES_EQUAL(grid.GetNext(slot).End, TInstant::Hours(100) + TDuration::Seconds(9));

                UNIT_ASSERT_VALUES_EQUAL(grid.GetPrev(slot).Start, TInstant::Hours(100) - TDuration::Seconds(5));
                UNIT_ASSERT_VALUES_EQUAL(grid.GetPrev(slot).End, TInstant::Hours(100) - TDuration::Seconds(1));
            }

            for (ui32 shift = 5; shift < 10; ++shift) {
                TInstant curTime = TInstant::Hours(100) + TDuration::Seconds(shift);
                auto slot = grid.Get(curTime);

                UNIT_ASSERT_VALUES_EQUAL(slot.Start, TInstant::Hours(100) + TDuration::Seconds(5));
                UNIT_ASSERT_VALUES_EQUAL(slot.End, TInstant::Hours(100) + TDuration::Seconds(9));

                UNIT_ASSERT_VALUES_EQUAL(grid.GetNext(slot).Start, TInstant::Hours(100) + TDuration::Seconds(10));
                UNIT_ASSERT_VALUES_EQUAL(grid.GetNext(slot).End, TInstant::Hours(100) + TDuration::Seconds(14));

                UNIT_ASSERT_VALUES_EQUAL(grid.GetPrev(slot).Start, TInstant::Hours(100));
                UNIT_ASSERT_VALUES_EQUAL(grid.GetPrev(slot).End, TInstant::Hours(100) + TDuration::Seconds(4));
            }

            for (ui32 shift = 55; shift < 60; ++shift) {
                TInstant curTime = TInstant::Hours(100) + TDuration::Seconds(shift);
                auto slot = grid.Get(curTime);

                UNIT_ASSERT_VALUES_EQUAL(slot.Start, TInstant::Hours(100) + TDuration::Seconds(55));
                UNIT_ASSERT_VALUES_EQUAL(slot.End, TInstant::Hours(100) + TDuration::Seconds(59));

                UNIT_ASSERT_VALUES_EQUAL(grid.GetNext(slot).Start, TInstant::Hours(100) + TDuration::Seconds(60));
                UNIT_ASSERT_VALUES_EQUAL(grid.GetNext(slot).End, TInstant::Hours(100) + TDuration::Seconds(64));

                UNIT_ASSERT_VALUES_EQUAL(grid.GetPrev(slot).Start, TInstant::Hours(100) + TDuration::Seconds(50));
                UNIT_ASSERT_VALUES_EQUAL(grid.GetPrev(slot).End, TInstant::Hours(100) + TDuration::Seconds(54));
            }
        }
    }

}

}   // namespace NKikimr
