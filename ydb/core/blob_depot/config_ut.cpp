#include "config.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NBlobDepot {

    Y_UNIT_TEST_SUITE(BlobDepotS3Config) {
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
