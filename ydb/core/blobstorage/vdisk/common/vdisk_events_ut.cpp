#include "vdisk_events.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

    Y_UNIT_TEST_SUITE(TVDiskEventsToString) {

        Y_UNIT_TEST(VPutWithoutPayload) {
            TEvBlobStorage::TEvVPut event;

            UNIT_ASSERT_STRING_CONTAINS(event.ToString(), "invalid payload count# 0");
        }

        Y_UNIT_TEST(VMultiPutWithoutPayload) {
            TEvBlobStorage::TEvVMultiPut event;
            event.Record.AddItems();

            UNIT_ASSERT_STRING_CONTAINS(event.ToString(), "missing payload");
        }

        Y_UNIT_TEST(VGetWithoutQuery) {
            TEvBlobStorage::TEvVGet event;

            UNIT_ASSERT(!event.ToString().empty());
        }

        Y_UNIT_TEST(VGetWithMalformedQueries) {
            TEvBlobStorage::TEvVGet event;
            event.Record.MutableRangeQuery();
            event.Record.AddExtremeQueries();

            UNIT_ASSERT_STRING_CONTAINS(event.ToString(), "<missing>");
        }
    }

} // NKikimr
