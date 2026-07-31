#include <ydb/core/blob_depot/types.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NBlobDepot {

Y_UNIT_TEST_SUITE(BlobDepotS3Locator) {
    // Object names of already stored blobs must never change: a blob is found in S3 by the name built from its
    // locator, so a different name means the data is lost. This pins the naming down, including the fan-out shared
    // with ColumnShard tiering (MakeS3KeyFanout).
    Y_UNIT_TEST(ObjectNameIsStable) {
        const TS3Locator locator{ .Len = 1024, .Generation = 3, .KeyId = 42 };
        UNIT_ASSERT_VALUES_EQUAL(locator.MakeObjectName("blob_depot/cloud"), "blob_depot/cloud/3/a/n/42");
    }

    Y_UNIT_TEST(ObjectNameRoundTrip) {
        for (ui32 generation = 1; generation < 5; ++generation) {
            for (ui64 keyId = 0; keyId < 100; ++keyId) {
                const TS3Locator locator{ .Len = 100, .Generation = generation, .KeyId = keyId };
                const TString name = locator.MakeObjectName(TString());
                TString error;
                const auto parsed = TS3Locator::FromObjectName(name.substr(1), locator.Len, &error);
                UNIT_ASSERT_C(parsed, error);
                UNIT_ASSERT_VALUES_EQUAL(parsed->Generation, locator.Generation);
                UNIT_ASSERT_VALUES_EQUAL(parsed->KeyId, locator.KeyId);
                UNIT_ASSERT_VALUES_EQUAL(parsed->Len, locator.Len);
            }
        }
    }
}

}   // namespace NKikimr::NBlobDepot
