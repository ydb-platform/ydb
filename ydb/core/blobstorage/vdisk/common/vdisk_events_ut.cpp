#include "vdisk_events.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

    Y_UNIT_TEST_SUITE(TVDiskEventsToString) {

        Y_UNIT_TEST(SingleDcSettingsInGenerationChange) {
            for (ui32 realm = 0; realm < 3; ++realm) {
                auto info = MakeIntrusive<TBlobStorageGroupInfo>(TBlobStorageGroupType::ErasureMirror3dc, 1u, 3u, 3u);
                info->EnableSingleDcMode = true;
                info->SurvivingDc = realm;
                TEvVGenerationChange event(info->GetVDiskId(0), info);
                THolder<TEvVGenerationChange> clone(event.Clone());
                UNIT_ASSERT(clone->NewInfo->EnableSingleDcMode);
                UNIT_ASSERT(clone->NewInfo->SurvivingDc.has_value());
                UNIT_ASSERT_VALUES_EQUAL(*clone->NewInfo->SurvivingDc, realm);
                UNIT_ASSERT_VALUES_EQUAL(clone->NewVDiskId, event.NewVDiskId);
            }
            auto info = MakeIntrusive<TBlobStorageGroupInfo>(TBlobStorageGroupType::ErasureMirror3dc, 1u, 3u, 3u);
            TEvVGenerationChange event(info->GetVDiskId(0), info);
            THolder<TEvVGenerationChange> clone(event.Clone());
            UNIT_ASSERT(!clone->NewInfo->EnableSingleDcMode);
            UNIT_ASSERT(!clone->NewInfo->SurvivingDc);
        }

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
