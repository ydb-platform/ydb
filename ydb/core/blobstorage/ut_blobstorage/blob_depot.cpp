#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>
#include <ydb/core/blob_depot/events.h>
#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/control/lib/immediate_control_board_impl.h>
#include <ydb/library/actors/core/events.h>

#include "blob_depot_test_functions.h"

using namespace NKikimr::NBlobDepot;

namespace {

std::optional<ui64> TryGetBlobDepotTabletId(TEnvironmentSetup& env, ui32 groupId) {
    const NKikimrBlobStorage::TBaseConfig baseConfig = env.FetchBaseConfig();
    for (const auto& group : baseConfig.GetGroup()) {
        if (group.GetGroupId() != groupId) {
            continue;
        }
        const ui64 depotTabletId = group.GetVirtualGroupInfo().GetBlobDepotId();
        if (depotTabletId) {
            return depotTabletId;
        }
    }
    return std::nullopt;
}

void SetAllNodesBlobDepotMaxLoadedTrashRecords(TEnvironmentSetup& env, ui64 limit) {
    for (const ui32 node : env.Runtime->GetNodes()) {
        TControlBoard::SetValue(limit,
            env.Runtime->GetNode(node)->AppData->Icb->BlobDepotControls.MaxLoadedTrashRecords);
    }
}

void RebootBlobDepotTablet(TEnvironmentSetup& env, ui64 blobDepotTabletId) {
    auto& runtime = *env.Runtime;
    const TActorId sender = runtime.AllocateEdgeActor(1);

    auto* poison = new NActors::TEvents::TEvPoison();
    auto* nested = new IEventHandle(TActorId(), sender, poison);
    runtime.Send(new IEventHandle(MakeTabletResolverID(), sender, new TEvTabletResolver::TEvForward(blobDepotTabletId, nested, {},
                 TEvTabletResolver::TEvForward::EActor::Tablet)),
        sender.NodeId());

    {
        auto forwardResult = env.WaitForEdgeActorEvent<TEvTabletResolver::TEvForwardResult>(sender, false);
        UNIT_ASSERT(forwardResult);
        UNIT_ASSERT_VALUES_EQUAL_C(forwardResult->Get()->Status, NKikimrProto::OK, forwardResult->Get()->ToString());
    }

    env.Sim(TDuration::Seconds(5));
    runtime.Send(new IEventHandle(MakeTabletResolverID(), sender, new TEvTabletResolver::TEvTabletProblem(blobDepotTabletId, TActorId())), sender.NodeId());
    env.Sim(TDuration::MilliSeconds(100));
    runtime.DestroyActor(sender);
}

} // namespace

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

    Y_UNIT_TEST(CollectGarbageAfterMaxGenerationBlock) {
        ui32 seed;
        LoadSeed(seed);
        TBlobDepotTestEnvironment tenv(seed);

        auto& env = *tenv.Env;
        const ui32 nodeId = 1;
        const ui32 groupId = tenv.BlobDepot;
        const ui64 tabletId = 100;
        const ui32 channel = 0;
        auto sender = env.Runtime->AllocateEdgeActor(nodeId);

        SendTEvBlock(env, sender, groupId, tabletId, Max<ui32>());
        auto blockResult = CaptureTEvBlockResult(env, sender, false);
        UNIT_ASSERT_VALUES_EQUAL(blockResult->Get()->Status, NKikimrProto::OK);

        SendTEvCollectGarbage(env, sender, groupId, tabletId, Max<ui32>(), Max<ui32>(), channel,
            true, Max<ui32>(), Max<ui32>(), nullptr, nullptr, false, true);
        auto collectResult = CaptureTEvCollectGarbageResult(env, sender);
        UNIT_ASSERT_VALUES_EQUAL_C(collectResult->Get()->Status, NKikimrProto::OK, collectResult->Get()->ToString());
    }

    Y_UNIT_TEST(TrashBatchReloadAfterRestartWithTinyLimit) {
        ui32 seed;
        LoadSeed(seed);
        TBlobDepotTestEnvironment tenv(seed);

        auto& env = *tenv.Env;
        const ui32 nodeId = 1;
        const ui32 groupId = tenv.BlobDepot;

        std::vector<TBlobInfo> blobs;
        constexpr ui64 tabletId = 100;
        const ui64 tablet2 = tabletId + 1;
        TBSState state;
        state[tabletId];
        state[tablet2];

        for (ui32 i = 0; i < 10; ++i) {
            blobs.push_back(TBlobInfo(tenv.DataGen(100), tabletId, 1, 1, i + 1, 0));
        }
        for (ui32 i = 10; i < 20; ++i) {
            blobs.push_back(TBlobInfo(tenv.DataGen(100), tabletId, 1, 1, i + 1, (i % 2)));
        }
        for (ui32 i = 0; i < 10; ++i) {
            blobs.push_back(TBlobInfo(tenv.DataGen(100), tabletId, 1, 2, i + 1, 0));
        }
        for (ui32 i = 0; i < 10; ++i) {
            blobs.push_back(TBlobInfo(tenv.DataGen(100), tabletId, 1, 3 + i, 1, 0));
        }
        for (ui32 i = 0; i < 5; ++i) {
            blobs.push_back(TBlobInfo(tenv.DataGen(100), tablet2, 1, 1, 1 + i, 0));
        }
        for (ui32 i = 0; i < 5; ++i) {
            blobs.push_back(TBlobInfo(tenv.DataGen(100), tablet2, 1, 2 + i, 1, 0));
        }

        for (auto& blob : blobs) {
            VerifiedPut(env, nodeId, groupId, blob, state);
        }

        ui32 gen = 2;
        ui32 perGenCtr = 1;

        VerifiedCollectGarbage(env, nodeId, groupId, tabletId, gen, perGenCtr++, 0, true, 1, 2, nullptr, nullptr, false,
            false, blobs, state);

        SetAllNodesBlobDepotMaxLoadedTrashRecords(env, 1);

        const auto blobDepotTabletId = TryGetBlobDepotTabletId(env, groupId);
        UNIT_ASSERT_C(blobDepotTabletId.has_value(), "blob depot virtual group must report BlobDepotId in base config");

        RebootBlobDepotTablet(env, *blobDepotTabletId);

        VerifiedGet(env, nodeId, groupId, blobs[1], false, false, std::nullopt, state);
        VerifiedGet(env, nodeId, groupId, blobs[2], false, false, std::nullopt, state);

        VerifiedGet(env, nodeId, groupId, blobs[20], false, false, std::nullopt, state);
        VerifiedGet(env, nodeId, groupId, blobs[30], false, false, std::nullopt, state);
        VerifiedGet(env, nodeId, groupId, blobs[31], false, false, std::nullopt, state);
        VerifiedGet(env, nodeId, groupId, blobs[40], false, false, std::nullopt, state);

        VerifiedCollectGarbage(env, nodeId, groupId, tabletId, gen, perGenCtr++, 0, true, 1, 1, nullptr, nullptr, false, false, blobs, state);

        {
            TBlobInfo blob(tenv.DataGen(100), tabletId, 99, 1, 1, 0);
            VerifiedPut(env, nodeId, groupId, blob, state);
            blobs.push_back(blob);
        }

        VerifiedCollectGarbage(env, nodeId, groupId, tabletId, gen, perGenCtr++, 0, true, 1, 3, nullptr, nullptr, false, true, blobs, state);

        {
            TBlobInfo blob(tenv.DataGen(100), tabletId, 99, 1, 3, 0);
            VerifiedPut(env, nodeId, groupId, blob, state);
            blobs.push_back(blob);
        }
        VerifiedRange(env, nodeId, groupId, tabletId, blobs[1].Id, blobs[1].Id, false, false, blobs, state);

        VerifiedGet(env, nodeId, groupId, blobs[1], false, false, std::nullopt, state);
        VerifiedGet(env, nodeId, groupId, blobs[2], false, false, std::nullopt, state);
        VerifiedGet(env, nodeId, groupId, blobs[3], false, false, std::nullopt, state);

        VerifiedGet(env, nodeId, groupId, blobs[20], false, false, std::nullopt, state);
        VerifiedGet(env, nodeId, groupId, blobs[30], false, false, std::nullopt, state);
        VerifiedGet(env, nodeId, groupId, blobs[31], false, false, std::nullopt, state);
        VerifiedGet(env, nodeId, groupId, blobs[40], false, false, std::nullopt, state);

        VerifiedCollectGarbage(env, nodeId, groupId, tabletId, gen, perGenCtr++, 0, true, 1, 1, nullptr, nullptr, false, true, blobs, state);
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

    Y_UNIT_TEST(CheckIntegrity) {
        ui32 seed;
        LoadSeed(seed);
        TBlobDepotTestEnvironment tenv(seed, 1, 8, TBlobStorageGroupType::Erasure4Plus2Block);

        TestBasicCheckIntegrity(tenv, 1, tenv.RegularGroups[0]);
        TestBasicCheckIntegrity(tenv, 1, tenv.BlobDepot);
    }
}
