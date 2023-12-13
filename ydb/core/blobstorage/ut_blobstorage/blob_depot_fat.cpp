#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blob_depot/events.h>

#include "blob_depot_event_managers.h"
#include "blob_depot_test_env.h"
#include "blob_depot_test_functions.h"

using namespace NKikimr::NBlobDepot;

Y_UNIT_TEST_SUITE(BlobDepotFat) {
    void LoadSeed(ui32& seed) {
        ui32 constantSeed = 0;
        if (TryIntFromString<10, ui32>(GetEnv("MERSENNE_SEED"), constantSeed)) {
            seed = constantSeed;
        } else {
            Seed().LoadOrFail(&seed, sizeof(seed));
        }
    }

    Y_UNIT_TEST(FatVerifiedRandom) {
        ui32 seed;
        LoadSeed(seed);
        TBlobDepotTestEnvironment tenv(seed, 1, 8, TBlobStorageGroupType::ErasureMirror3of4);

        TestVerifiedRandom(tenv, 8, 100, tenv.BlobDepot, 1e9, 1e9, 1500);
    }

    Y_UNIT_TEST(FatDecommitVerifiedRandom) {
        ui32 seed;
        LoadSeed(seed);
        TBlobDepotTestEnvironment tenv(seed, 1, 8, TBlobStorageGroupType::ErasureMirror3of4);

        TestVerifiedRandom(tenv, 8, 100, tenv.RegularGroups[0], 1e9, 1000, 1500);
    }

/* ----- Restore is not implemented in blob depot ------
    Y_UNIT_TEST(RestoreGet) {
        ui32 seed;
        Seed().LoadOrFail(&seed, sizeof(seed));
        TBlobDepotTestEnvironment tenv(seed);
        auto vdisksRegular = tenv.Env->GetGroupInfo(tenv.RegularGroups[0])->GetDynamicInfo().ServiceIdForOrderNumber;

        TestRestoreGet(tenv, 15, tenv.RegularGroups[0], 10, &vdisksRegular);
        TestRestoreGet(tenv, 100, tenv.BlobDepot, 10, &vdisksBlobDepot);
    }

    Y_UNIT_TEST(RestoreDiscover) {
        ui32 seed;
        Seed().LoadOrFail(&seed, sizeof(seed));
        TBlobDepotTestEnvironment tenv(seed);
        auto vdisksRegular = tenv.Env->GetGroupInfo(tenv.RegularGroups[0])->GetDynamicInfo().ServiceIdForOrderNumber;

        TestRestoreDiscover(tenv, 15, tenv.RegularGroups[0], 10, &vdisksRegular);
        TestRestoreDiscover(tenv, 100, tenv.BlobDepot, 10, &vdisksBlobDepot);
    }

    Y_UNIT_TEST(RestoreRange) {
        ui32 seed;
        Seed().LoadOrFail(&seed, sizeof(seed));
        TBlobDepotTestEnvironment tenv(seed);
        auto vdisksRegular = tenv.Env->GetGroupInfo(tenv.RegularGroups[0])->GetDynamicInfo().ServiceIdForOrderNumber;

        TestRestoreRange(tenv, 15, tenv.RegularGroups[0], 10, &vdisksRegular);
        TestRestoreRange(tenv, 100, tenv.BlobDepot, 10, &vdisksBlobDepot);
    }
*/
}
