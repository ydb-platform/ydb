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

    Y_UNIT_TEST(GetBlockedTabletGeneration) {
        ui32 seed;
        Seed().LoadOrFail(&seed, sizeof(seed));
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
        ui32 forceBlockedGeneration = 0;

        auto ev = std::make_unique<TEvBlobStorage::TEvGet>(
            blob.Id,
            0,
            blob.Id.BlobSize(),
            TInstant::Max(),
            NKikimrBlobStorage::EGetHandleClass::FastRead, 
            mustRestoreFirst,
            isIndexOnly,
            forceBlockedGeneration);
        ev->ReaderTabletId = tabletId;
        ev->ReaderTabletGeneration = tabletGeneration;

        auto sender = tenv.Env->Runtime->AllocateEdgeActor(nodeId);
        env.Runtime->WrapInActorContext(sender, [&] {
            SendToBSProxy(sender, groupId, ev.release(), cookie);
        });

        auto res = CaptureTEvGetResult(env, sender, true, true);

        // check that TEvGet returns BLOCKED
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Responses[0].Status, NKikimrProto::BLOCKED);
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
