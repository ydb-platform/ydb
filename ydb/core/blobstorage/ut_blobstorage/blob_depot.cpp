#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blob_depot/events.h>

#include "blob_depot_event_managers.h"
#include "blob_depot_auxiliary_structures.h"
#include "blob_depot_test_functions.h"

#include <util/random/entropy.h>

using namespace NKikimr::NBlobDepot;

Y_UNIT_TEST_SUITE(BlobDepot) {
    Y_UNIT_TEST(BasicPutAndGet) {
        ui32 seed;
        Seed().LoadOrFail(&seed, sizeof(seed));
        TBlobDepotTestEnvironment tenv(seed);
        
        TestBasicPutAndGet(tenv, 1, tenv.RegularGroups[0]);
        TestBasicPutAndGet(tenv, 11, tenv.BlobDepot);
    }

    Y_UNIT_TEST(BasicRange) {
        ui32 seed;
        Seed().LoadOrFail(&seed, sizeof(seed));
        TBlobDepotTestEnvironment tenv(seed);
        
        TestBasicRange(tenv, 1, tenv.RegularGroups[0]);
        TestBasicRange(tenv, 100, tenv.BlobDepot);
    }

    Y_UNIT_TEST(BasicDiscover) {
        ui32 seed;
        Seed().LoadOrFail(&seed, sizeof(seed));
        TBlobDepotTestEnvironment tenv(seed);
        
        TestBasicDiscover(tenv, 1000, tenv.RegularGroups[0]);
        TestBasicDiscover(tenv, 100, tenv.BlobDepot);
    }

    Y_UNIT_TEST(BasicBlock) {
        ui32 seed;
        Seed().LoadOrFail(&seed, sizeof(seed));
        TBlobDepotTestEnvironment tenv(seed);
        
        TestBasicBlock(tenv, 15, tenv.RegularGroups[0]);
        TestBasicBlock(tenv, 100, tenv.BlobDepot);
    }

    Y_UNIT_TEST(BasicCollectGarbage) {
        ui32 seed;
        Seed().LoadOrFail(&seed, sizeof(seed));
        TBlobDepotTestEnvironment tenv(seed);
        
        TestBasicCollectGarbage(tenv, 15, tenv.RegularGroups[0]);
        TestBasicCollectGarbage(tenv, 100, tenv.BlobDepot);
    }

    Y_UNIT_TEST(VerifiedRandom) {
        ui32 seed;
        Seed().LoadOrFail(&seed, sizeof(seed));
        TBlobDepotTestEnvironment tenv(seed);
        
        // TestVerifiedRandom(tenv, 8, 15, tenv.RegularGroups[0], 1000);
        TestVerifiedRandom(tenv, 8, 100, tenv.BlobDepot, 1000);
    }

    Y_UNIT_TEST(LoadPutAndRead) {
        ui32 seed;
        Seed().LoadOrFail(&seed, sizeof(seed));
        TBlobDepotTestEnvironment tenv(seed);
        
        // TestLoadPutAndGet(tenv, 100, tenv.BlobDepot, 1 << 10, 1 << 15, 500);
        TestLoadPutAndGet(tenv, 100, tenv.BlobDepot, 100, 1 << 10, 500);
    }

    Y_UNIT_TEST(DecommitPutAndRead) {
        ui32 seed;
        Seed().LoadOrFail(&seed, sizeof(seed));
        TBlobDepotTestEnvironment tenv(seed);
        
        TestLoadPutAndGet(tenv, 15, tenv.RegularGroups[0], 100, 1 << 10, 500, true, { 5, 1, 5, 1, 1, 0 });
    }

    Y_UNIT_TEST(DecommitVerifiedRandom) {
        ui32 seed;
        Seed().LoadOrFail(&seed, sizeof(seed));
        TBlobDepotTestEnvironment tenv(seed);
        
        TestVerifiedRandom(tenv, 8, 15, tenv.RegularGroups[0], 1000, 499, { 10, 10, 3, 3, 2, 1, 1, 3, 3, 0 });
    }
}
