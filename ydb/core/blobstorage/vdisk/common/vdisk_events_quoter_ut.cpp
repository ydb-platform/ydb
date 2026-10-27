#include "vdisk_events_quoter.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

    Y_UNIT_TEST_SUITE(TEventsQuoterTest) {
        Y_UNIT_TEST(ReadsLiveRateFromSharedControl) {
            TControlWrapper rate(1'000, 0, 1'000'000);
            TEventsQuoter quoter(rate);

            UNIT_ASSERT_VALUES_EQUAL(quoter.GetMaxPacketSize(), 100);

            rate = 2'000;
            UNIT_ASSERT_VALUES_EQUAL(quoter.GetMaxPacketSize(), 200);

            rate = 0;
            UNIT_ASSERT_VALUES_EQUAL(quoter.GetMaxPacketSize(), Max<ui64>());
        }

        Y_UNIT_TEST(ExplicitRateTakesPrecedenceOverControl) {
            TControlWrapper rate(1'000, 0, 1'000'000);
            TEventsQuoter quoter(rate);

            UNIT_ASSERT_VALUES_EQUAL(quoter.GetMaxPacketSize(3'000), 300);
        }
    }

} // NKikimr
