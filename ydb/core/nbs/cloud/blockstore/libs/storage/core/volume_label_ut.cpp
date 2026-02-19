#include "volume_label.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/digest/murmur.h>
#include <util/string/cast.h>

namespace NYdb::NBS::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskIdTest)
{
    static void VerifyDiskIdToPathDeprecated(const TString& diskId,
                                             const TString& path)
    {
        UNIT_ASSERT_VALUES_EQUAL_C(DiskIdToPathDeprecated(diskId), path,
                                   "where diskId=" << diskId.Quote());
    }

    Y_UNIT_TEST(TestDiskIdToPathDeprecated)
    {
        VerifyDiskIdToPathDeprecated("volume", "volume");
        VerifyDiskIdToPathDeprecated("test volume", "test+volume");
        VerifyDiskIdToPathDeprecated("test/volume", "test/volume");
        VerifyDiskIdToPathDeprecated("тест", "%D1%82%D0%B5%D1%81%D1%82");
    }

    static void VerifyDiskIdToPath(const TString& diskId, const TString& path)
    {
        UNIT_ASSERT_VALUES_EQUAL_C(DiskIdToPath(diskId), path,
                                   "where diskId=" << diskId.Quote());
    }

    Y_UNIT_TEST(TestDiskIdToPath)
    {
        VerifyDiskIdToPath("volume", "_355/volume");
        VerifyDiskIdToPath("test volume", "_249/test+volume");
        VerifyDiskIdToPath("test/volume", "_EB/test%2Fvolume");
        VerifyDiskIdToPath("тест", "_189/%D1%82%D0%B5%D1%81%D1%82");
    }

    static void VerifyPathNameToDiskId(const TString& pathName,
                                       const TString& diskId)
    {
        UNIT_ASSERT_VALUES_EQUAL_C(PathNameToDiskId(pathName), diskId,
                                   "where pathName=" << pathName.Quote());
    }

    Y_UNIT_TEST(TestPathNameToDiskId)
    {
        VerifyPathNameToDiskId("volume", "volume");
        VerifyPathNameToDiskId("test+volume", "test volume");
        VerifyPathNameToDiskId("test%2Fvolume", "test/volume");
        VerifyPathNameToDiskId("%D1%82%D0%B5%D1%81%D1%82", "тест");
    }

    static void VerifyDiskIdToVolumeDirAndNameDeprecated(
        const TString& rootDir, const TString& diskId,
        const TString& expectedDir, const TString& expectedName)
    {
        TString dir, name;
        std::tie(dir, name) =
            DiskIdToVolumeDirAndNameDeprecated(rootDir, diskId);
        UNIT_ASSERT_VALUES_EQUAL(dir, expectedDir);
        UNIT_ASSERT_VALUES_EQUAL(name, expectedName);
    }

    Y_UNIT_TEST(TestDiskIdToVolumeDirAndNameDeprecated)
    {
        VerifyDiskIdToVolumeDirAndNameDeprecated("/local/nbs", "volume",
                                                 "/local/nbs", "volume");
        VerifyDiskIdToVolumeDirAndNameDeprecated("/local/nbs", "test volume",
                                                 "/local/nbs", "test+volume");
        VerifyDiskIdToVolumeDirAndNameDeprecated("/local/nbs", "///volume",
                                                 "/local/nbs", "volume");
        VerifyDiskIdToVolumeDirAndNameDeprecated("/local/nbs", "/test/volume",
                                                 "/local/nbs/test", "volume");
        VerifyDiskIdToVolumeDirAndNameDeprecated(
            "/local/nbs", "///test///volume", "/local/nbs/test", "volume");
        VerifyDiskIdToVolumeDirAndNameDeprecated(
            "/local/nbs", "///test//dir///volume", "/local/nbs/test/dir",
            "volume");
    }

    static void VerifyDiskIdToVolumeDirAndName(
        const TString& rootDir, const TString& diskId,
        const TString& expectedDir, const TString& expectedName)
    {
        TString dir, name;
        std::tie(dir, name) = DiskIdToVolumeDirAndName(rootDir, diskId);
        UNIT_ASSERT_VALUES_EQUAL(dir, expectedDir);
        UNIT_ASSERT_VALUES_EQUAL(name, expectedName);
    }

    Y_UNIT_TEST(TestDiskIdToVolumeDirAndName)
    {
        VerifyDiskIdToVolumeDirAndName("/local/nbs", "volume",
                                       "/local/nbs/_355", "volume");
        VerifyDiskIdToVolumeDirAndName("/local/nbs", "test volume",
                                       "/local/nbs/_249", "test+volume");
        VerifyDiskIdToVolumeDirAndName("/local/nbs", "///volume",
                                       "/local/nbs/_3E", "%2F%2F%2Fvolume");
        VerifyDiskIdToVolumeDirAndName("/local/nbs", "/test/volume",
                                       "/local/nbs/_366", "%2Ftest%2Fvolume");
        VerifyDiskIdToVolumeDirAndName("/local/nbs", "///test///volume",
                                       "/local/nbs/_1D2",
                                       "%2F%2F%2Ftest%2F%2F%2Fvolume");
    }
}

}   // namespace NYdb::NBS::NStorage
