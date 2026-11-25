#include "config.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NBlobDepot {

    Y_UNIT_TEST_SUITE(BlobDepotS3Config) {
        Y_UNIT_TEST(CoolingUploadBufferLimits) {
            NKikimrBlobDepot::TS3BackendSettings settings;
            settings.MutableSettings()->SetBucket("bucket");
            auto* async = settings.MutableAsyncMode();
            UNIT_ASSERT(!ValidateDataCooling(settings));
            async->SetUploadPutsInFlight(32);
            UNIT_ASSERT(!ValidateDataCooling(settings)); // 32 default-size packs use 512 MiB.
            async->SetTargetObjectBytes((16 << 20) + 1);
            UNIT_ASSERT(ValidateDataCooling(settings));
            async->SetUploadPutsInFlight(8);
            async->SetTargetObjectBytes(64 << 20);
            UNIT_ASSERT(!ValidateDataCooling(settings));
            async->SetTargetObjectBytes((64 << 20) + 1);
            async->SetUploadPutsInFlight(1);
            UNIT_ASSERT(ValidateDataCooling(settings));
            async->SetTargetObjectBytes(1);
            async->SetUploadPutsInFlight(33);
            UNIT_ASSERT(ValidateDataCooling(settings));
        }

        Y_UNIT_TEST(PhysicalKeyPrefix) {
            NKikimrSchemeOp::TS3Settings settings;
            UNIT_ASSERT_VALUES_EQUAL(MakeS3BasePath(settings, "vg"), "vg");
            settings.SetObjectKeyPattern("archive/team/");
            UNIT_ASSERT_VALUES_EQUAL(MakeS3BasePath(settings, "vg"), "archive/team/vg");
            settings.SetObjectKeyPattern("archive");
            UNIT_ASSERT_VALUES_EQUAL(MakeS3BasePath(settings, "team/vg"), "archive/team/vg");
        }
    }

} // namespace NKikimr::NBlobDepot
