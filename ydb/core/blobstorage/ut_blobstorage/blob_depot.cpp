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

    Y_UNIT_TEST(StorageInfoVersion) {
        ui32 seed;
        LoadSeed(seed);
        TBlobDepotTestEnvironment tenv(seed);

        auto& env = *tenv.Env;
        const TActorId sender = env.Runtime->AllocateEdgeActor(1);
        const ui64 tabletId = 100;
        const ui64 issuerGuid = 1;

        auto block = [&](ui32 generation, ui32 version) {
            env.Runtime->WrapInActorContext(sender, [&] {
                SendToBSProxy(sender, tenv.BlobDepot, new TEvBlobStorage::TEvBlock(tabletId, generation,
                    TInstant::Max(), issuerGuid, TWriteSource::Unknown, version));
            });
            return CaptureTEvBlockResult(env, sender, false);
        };

        auto result = block(10, 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);

        result = block(20, 0);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::ERROR);
        UNIT_ASSERT(result->Get()->IsTabletStorageInfoVersionObsolete);

        result = block(11, 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);

        result = block(10, 2);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::ERROR);
        UNIT_ASSERT(!result->Get()->IsTabletStorageInfoVersionObsolete);

        // The rejected version bump must not mutate either value.
        result = block(12, 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);

        result = block(13, 2);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);

        env.Runtime->WrapInActorContext(sender, [&] {
            SendToBSProxy(sender, tenv.BlobDepot, new TEvBlobStorage::TEvBlock(~tabletId, 4, TInstant::Max(),
                TWriteSource::SyncerMergeBlock));
        });
        result = CaptureTEvBlockResult(env, sender, false);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);

        const std::optional<ui64> blobDepotTabletId = TryGetBlobDepotTabletId(env, tenv.BlobDepot);
        UNIT_ASSERT(blobDepotTabletId);
        RebootBlobDepotTablet(env, *blobDepotTabletId);

        result = block(14, 3);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::ERROR);
        UNIT_ASSERT(result->Get()->IsTabletStorageInfoVersionObsolete);
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

    // The agent does not forward the write, it re-originates it under a blob id of its own, so
    // every field the underlying group needs has to be copied across by hand. Without that a system
    // tablet writing through a virtual group reaches the disks as user data.
    Y_UNIT_TEST(DataKindSurvivesTheAgent) {
        ui32 seed;
        LoadSeed(seed);
        TBlobDepotTestEnvironment tenv(seed);
        auto& env = *tenv.Env;

        ui64 tabletId = 100500;
        for (const auto dataKind : {NKikimrBlobStorage::TDataKind::USER, NKikimrBlobStorage::TDataKind::SYSTEM}) {
            const TString data = TStringBuilder() << "data_" << static_cast<int>(dataKind);

            // Both the original write into the virtual group and the one the agent relays into a
            // real group carry this exact buffer, so matching on it picks up both hops.
            std::vector<NKikimrBlobStorage::TDataKind::E> seen;
            env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == TEvBlobStorage::EvPut) {
                    auto *put = ev->Get<TEvBlobStorage::TEvPut>();
                    if (put->Buffer.ConvertToString() == data) {
                        seen.push_back(put->DataKind);
                    }
                }
                return true;
            };

            const TLogoBlobID id(tabletId++, 1, 1, 0, data.size(), 0);
            const TActorId sender = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            env.Runtime->WrapInActorContext(sender, [&] {
                SendToBSProxy(sender, tenv.BlobDepot, new TEvBlobStorage::TEvPut(TEvBlobStorage::TEvPut::TParameters{
                    .BlobId = id,
                    .Buffer = TRope(data),
                    .Deadline = TInstant::Max(),
                    .DataKind = dataKind,
                }));
            });
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(sender, false);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);

            env.Runtime->FilterFunction = nullptr;

            UNIT_ASSERT_C(seen.size() >= 2, "the agent did not relay the write, nothing was proven");
            for (const auto kind : seen) {
                UNIT_ASSERT_EQUAL(kind, dataKind);
            }
        }
    }

    // A read that asks for MustRestoreFirst turns into a resolve, and in decommission mode the
    // tablet answers it by copying the blob into its own storage. The kind of the read is the only
    // thing that can tell that copy whether it is allowed to happen in a group short of space, so
    // it has to travel with the resolve.
    Y_UNIT_TEST(DataKindReachesTheResolve) {
        ui32 seed;
        LoadSeed(seed);
        TBlobDepotTestEnvironment tenv(seed);
        auto& env = *tenv.Env;

        ui64 tabletId = 100600;
        for (const auto dataKind : {NKikimrBlobStorage::TDataKind::USER, NKikimrBlobStorage::TDataKind::SYSTEM}) {
            const TString data = "hello";
            const TLogoBlobID id(tabletId, 1, 1, 0, data.size(), 0);
            const TActorId sender = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            env.Runtime->WrapInActorContext(sender, [&] {
                SendToBSProxy(sender, tenv.BlobDepot, new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
            });
            auto putRes = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(sender, false);
            UNIT_ASSERT_VALUES_EQUAL(putRes->Get()->Status, NKikimrProto::OK);

            // A range resolve names the tablet it scans, which keeps unrelated resolves -- the agent
            // issues them for its own housekeeping too -- out of the measurement.
            std::vector<NKikimrBlobStorage::TDataKind::E> seen;
            env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == TEvBlobDepot::EvResolve) {
                    const auto& record = ev->Get<TEvBlobDepot::TEvResolve>()->Record;
                    for (const auto& item : record.GetItems()) {
                        if (item.GetTabletId() == tabletId) {
                            seen.push_back(record.GetDataKind());
                        }
                    }
                }
                return true;
            };

            const TLogoBlobID from(tabletId, 0, 0, 0, 0, 0);
            const TLogoBlobID to(tabletId, Max<ui32>(), Max<ui32>(), TLogoBlobID::MaxChannel,
                TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie);
            env.Runtime->WrapInActorContext(sender, [&] {
                auto range = std::make_unique<TEvBlobStorage::TEvRange>(tabletId, from, to,
                    true /*mustRestoreFirst*/, TInstant::Max(), false /*isIndexOnly*/);
                range->DataKind = dataKind;
                SendToBSProxy(sender, tenv.BlobDepot, range.release());
            });
            auto rangeRes = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvRangeResult>(sender, false);
            UNIT_ASSERT_VALUES_EQUAL(rangeRes->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(rangeRes->Get()->Responses.size(), 1);

            env.Runtime->FilterFunction = nullptr;

            UNIT_ASSERT_C(!seen.empty(), "the read did not resolve anything, so it proves nothing");
            for (const auto kind : seen) {
                UNIT_ASSERT_EQUAL(kind, dataKind);
            }

            ++tabletId;
        }
    }
}
