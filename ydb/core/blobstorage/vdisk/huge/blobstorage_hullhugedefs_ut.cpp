#include "blobstorage_hullhugedefs.h"
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/null.h>

#define STR Cnull


namespace NKikimr {

    using namespace NHuge;

    Y_UNIT_TEST_SUITE(TBlobStorageHullHugeBlobMerger) {

        Y_UNIT_TEST(Basic) {
            TBlobMerger merger;

            const ui32 blobSize = 128u << 10u;
            NMatrix::TVectorType parts1(0, 6);
            parts1.Set(1);
            parts1.Set(3);
            TDiskPart diskParts1[2] = {{1, blobSize * 2u, blobSize}, {2, blobSize * 4u, blobSize}};

            NMatrix::TVectorType parts2(0, 6);
            parts2.Set(2);
            TDiskPart diskParts2[1] = {{3, blobSize * 6u, blobSize}};

            NMatrix::TVectorType parts3(0, 6);
            parts3.Set(2);
            parts3.Set(3);
            TDiskPart diskParts3[2] = {{4, blobSize * 8u, blobSize}, {5, blobSize * 10u, blobSize}};


            merger.Add(diskParts1, diskParts1 + 2, parts1, 4); // circaLsn = 4
            STR << merger.ToString() << "\n";
            TVector<TDiskPart> res1 = {{1, blobSize * 2u, blobSize}, {2, blobSize * 4u, blobSize}};
            TVector<ui64> sstIds1{4, 4};
            UNIT_ASSERT(merger.GetCircaLsns() == sstIds1);
            UNIT_ASSERT(merger.SavedData() == res1);
            UNIT_ASSERT(merger.DeletedData().empty());

            merger.Add(diskParts2, diskParts2 + 1, parts2, 6); // circaLsn = 6
            STR << merger.ToString() << "\n";
            TVector<TDiskPart> res2 = {{1, blobSize * 2u, blobSize}, {3, blobSize * 6u, blobSize},
                {2, blobSize * 4u, blobSize}};
            TVector<ui64> sstIds2{4, 6, 4};
            UNIT_ASSERT(merger.GetCircaLsns() == sstIds2);
            UNIT_ASSERT(merger.SavedData() == res2);
            UNIT_ASSERT(merger.DeletedData().empty());

            merger.Add(diskParts3, diskParts3 + 2, parts3, 5); // circaLsn = 5
            STR << merger.ToString() << "\n";
            TVector<TDiskPart> res3 = {{1, blobSize * 2u, blobSize}, {3, blobSize * 6u, blobSize},
                {5, blobSize * 10u, blobSize}};
            TVector<TDiskPart> deleted3 = {{4, blobSize * 8u, blobSize}, {2, blobSize * 4u, blobSize}};
            TVector<ui64> sstIds3{4, 6, 5};
            UNIT_ASSERT(merger.GetCircaLsns() == sstIds3);
            UNIT_ASSERT(merger.SavedData() == res3);
            UNIT_ASSERT(merger.DeletedData() == deleted3);
        }
    }

} // NKikimr
