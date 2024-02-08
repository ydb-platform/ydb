#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blob_depot/events.h>

#include "blob_depot_test_functions.h"

using namespace NKikimr::NBlobDepot;

Y_UNIT_TEST_SUITE(BlobDepot) {
    void LoadSeed(ui32& seed) {
        ui32 constantSeed = 0;
        if (TryIntFromString<10, ui32>(GetEnv("MERSENNE_SEED"), constantSeed)) {
            seed = constantSeed;
        } else {
            Seed().LoadOrFail(&seed, sizeof(seed));
        }
    }

    Y_UNIT_TEST(BasicPutAndGet) {
        ui32 seed;
        LoadSeed(seed);
        TBlobDepotTestEnvironment tenv(seed);

        TestBasicPutAndGet(tenv, 1, tenv.RegularGroups[0]);
        TestBasicPutAndGet(tenv, 11, tenv.BlobDepot);
    }

    Y_UNIT_TEST(TestBlockedEvGetRequest) {
        ui32 seed;
        LoadSeed(seed);
        TBlobDepotTestEnvironment tenv(seed);

        constexpr ui32 tabletId = 10;
        constexpr ui32 cookie = 1;
        constexpr ui32 tabletGeneration = 3;

        TBlobInfo blob(tenv.DataGen(100), tabletId, cookie, tabletGeneration);

        auto& env = *tenv.Env;
        constexpr ui32 nodeId = 1;
        auto groupId = tenv.BlobDepot;

        TBSState state;
        state[tabletId];

        // write blob to blob depot
        VerifiedPut(env, nodeId, groupId, blob, state);

        // block all tablet generations <= tabletGeneration
        VerifiedBlock(env, nodeId, groupId, tabletId, tabletGeneration, state);

        // do TEvGet with Reader* params
        auto mustRestoreFirst = false;
        auto isIndexOnly = false;

        auto ev = std::make_unique<TEvBlobStorage::TEvGet>(
            blob.Id,
            0,
            blob.Id.BlobSize(),
            TInstant::Max(),
            NKikimrBlobStorage::EGetHandleClass::FastRead,
            mustRestoreFirst,
            isIndexOnly);
        ev->ReaderTabletData = {tabletId, tabletGeneration};

        auto sender = tenv.Env->Runtime->AllocateEdgeActor(nodeId);
        env.Runtime->WrapInActorContext(sender, [&] {
            SendToBSProxy(sender, groupId, ev.release(), cookie);
        });

        auto res = CaptureTEvGetResult(env, sender, true, true);

        // check that TEvGet returns BLOCKED
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::BLOCKED);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Responses[0].Status, NKikimrProto::BLOCKED);
    }

    Y_UNIT_TEST(BasicRange) {
        ui32 seed;
        LoadSeed(seed);
        TBlobDepotTestEnvironment tenv(seed);

        TestBasicRange(tenv, 1, tenv.RegularGroups[0]);
        TestBasicRange(tenv, 100, tenv.BlobDepot);
    }

    Y_UNIT_TEST(BasicDiscover) {
        ui32 seed;
        LoadSeed(seed);
        TBlobDepotTestEnvironment tenv(seed);

        TestBasicDiscover(tenv, 1000, tenv.RegularGroups[0]);
        TestBasicDiscover(tenv, 100, tenv.BlobDepot);
    }

    Y_UNIT_TEST(BasicBlock) {
        ui32 seed;
        LoadSeed(seed);
        TBlobDepotTestEnvironment tenv(seed);

        TestBasicBlock(tenv, 15, tenv.RegularGroups[0]);
        TestBasicBlock(tenv, 100, tenv.BlobDepot);
    }

    Y_UNIT_TEST(BasicCollectGarbage) {
        ui32 seed;
        LoadSeed(seed);
        TBlobDepotTestEnvironment tenv(seed);

        TestBasicCollectGarbage(tenv, 15, tenv.RegularGroups[0]);
        TestBasicCollectGarbage(tenv, 100, tenv.BlobDepot);
    }

    Y_UNIT_TEST(VerifiedRandom) {
        ui32 seed;
        LoadSeed(seed);
        TBlobDepotTestEnvironment tenv(seed);

        // TestVerifiedRandom(tenv, 8, 15, tenv.RegularGroups[0], 1000);
        TestVerifiedRandom(tenv, 8, 100, tenv.BlobDepot, 1000);
    }

    Y_UNIT_TEST(LoadPutAndRead) {
        ui32 seed;
        LoadSeed(seed);
        TBlobDepotTestEnvironment tenv(seed);

        // TestLoadPutAndGet(tenv, 100, tenv.BlobDepot, 1 << 10, 1 << 15, 500);
        TestLoadPutAndGet(tenv, 100, tenv.BlobDepot, 100, 1 << 10, 500);
    }

    Y_UNIT_TEST(DecommitPutAndRead) {
        ui32 seed;
        LoadSeed(seed);
        TBlobDepotTestEnvironment tenv(seed);

        TestLoadPutAndGet(tenv, 15, tenv.RegularGroups[0], 100, 1 << 10, 500, true, 10, { 5, 1, 5, 1, 1, 0 });
        // no blob depot restarts performed
    }

    Y_UNIT_TEST(DecommitVerifiedRandom) {
        ui32 seed;
        LoadSeed(seed);
        TBlobDepotTestEnvironment tenv(seed);

        TestVerifiedRandom(tenv, 8, 15, tenv.RegularGroups[0], 1000, 500, 10, { 10, 10, 3, 3, 2, 1, 1, 3, 3, 0 });
        // no blob depot restarts performed
    }
}
