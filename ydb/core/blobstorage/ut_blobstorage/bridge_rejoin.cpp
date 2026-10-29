#include "bridge_common.h"
#include "bridge_pile_ops.h"

#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/nodewarden/distconf.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_events.h>

#include <util/system/type_name.h>

using NBridgeTest::GetBridgedLocation;
using NBridgeTest::QueryStorageConfig;
using NBridgeTest::SwitchBridgePileState;

Y_UNIT_TEST_SUITE(BridgeRejoin) {
    Y_UNIT_TEST(TwoDisconnectedPilesRecoverData) {
        TEnvironmentSetup env{{
            .NodeCount = 24,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .SelfManagementConfig = true,
            .NumPiles = 3,
            .AutomaticBootstrap = true,
        }};
        env.CreatePool();
        const ui32 groupId = env.GetGroups().front();
        auto& runtime = env.Runtime;
        ui32 configErrors = 0;
        bool holdSyncers = false;
        std::vector<std::pair<ui32, std::unique_ptr<IEventHandle>>> heldSyncers;
        THashSet<TGroupId> heldTargets;
        std::optional<TGroupId> injectErrorTarget;
        runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == NStorage::TEvNodeConfigInvokeOnRootResult::EventType) {
                const auto& record = event->Get<NStorage::TEvNodeConfigInvokeOnRootResult>()->Record;
                if (record.GetStatus() != NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK) {
                    // A tight retry loop can prevent simulated timeouts from firing.
                    UNIT_ASSERT_C(++configErrors < 1000, record.DebugString());
                }
            }
            if (holdSyncers && event->GetTypeRewrite() == NStorage::TEvNodeWardenNotifySyncerFinished::EventType &&
                    event->Get<NStorage::TEvNodeWardenNotifySyncerFinished>()->BridgeProxyGroupId.GetRawId() == groupId) {
                // Keep sync stages from advancing, so the counters can be checked mid-sync.
                heldTargets.insert(event->Get<NStorage::TEvNodeWardenNotifySyncerFinished>()->TargetGroupId);
                heldSyncers.emplace_back(nodeId, std::move(event));
                return false;
            }
            if (injectErrorTarget && event->GetTypeRewrite() == TEvBlobStorage::TEvControllerUpdateSyncerState::EventType) {
                // NodeWarden reports only its current syncer, so BSC accepts this error
                for (auto& syncer : *event->Get<TEvBlobStorage::TEvControllerUpdateSyncerState>()->Record.MutableSyncers()) {
                    if (syncer.GetTargetGroupId() == injectErrorTarget->GetRawId() && syncer.GetFinished() &&
                            !syncer.HasErrorReason()) {
                        syncer.SetErrorReason("injected by test");
                        injectErrorTarget.reset();
                    }
                }
            }
            return true;
        };
        const auto sender = runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        auto groupRequest = std::make_unique<TEvBlobStorage::TEvControllerGetGroup>();
        groupRequest->Record.AddGroupIDs(groupId);
        runtime->SendToPipe(env.TabletId, sender, groupRequest.release(), Max<ui64>(),
            TTestActorSystem::GetPipeConfigWithRetries());
        auto groupResponse = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>(sender, false,
            runtime->GetClock() + TDuration::Seconds(30));
        UNIT_ASSERT_C(groupResponse, "Timed out waiting for bridge group configuration");
        std::vector<TGroupId> pileGroups;
        for (const auto& group : groupResponse->Get()->Record.GetServiceSet().GetGroups()) {
            if (group.GetGroupID() == groupId) {
                for (const auto& pile : group.GetBridgeGroupState().GetPile()) {
                    pileGroups.push_back(TGroupId::FromProto(&pile, &NKikimrBridge::TGroupState::TPile::GetGroupId));
                }
            }
        }
        UNIT_ASSERT_VALUES_EQUAL_C(pileGroups.size(), 3, groupResponse->Get()->Record.DebugString());

        std::vector<std::pair<TLogoBlobID, TString>> blobs;
        auto put = [&](TString data) {
            const TLogoBlobID id(100500, 1, blobs.size() + 1, 0, data.size(), 0);
            runtime->WrapInActorContext(sender, [&] {
                SendToBSProxy(sender, groupId, new TEvBlobStorage::TEvPut(id, data,
                    runtime->GetClock() + TDuration::Seconds(30)));
            });
            auto response = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(sender, false,
                runtime->GetClock() + TDuration::Seconds(30));
            UNIT_ASSERT_C(response, "Timed out waiting for Put");
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->Status, NKikimrProto::OK);
            blobs.emplace_back(id, std::move(data));
        };
        auto checkData = [&](ui32 readGroupId, ui32 nodeId, bool rejoined = true) {
            runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(nodeId), sender,
                new TEvNodeWardenQueryGroupInfo(readGroupId)), 1);
            auto info = env.WaitForEdgeActorEvent<TEvNodeWardenGroupInfo>(sender, false,
                runtime->GetClock() + TDuration::Seconds(30));
            UNIT_ASSERT_C(info, "Timed out waiting for the pile group configuration");
            UNIT_ASSERT_C(info->Get()->Record.HasGroup(), info->Get()->Record.DebugString());
            const auto generation = info->Get()->Record.GetGroup().GetGroupGeneration();
            for (const auto& [id, data] : blobs) {
                runtime->WrapInActorContext(sender, [&] {
                    auto *request = new TEvBlobStorage::TEvGet(id, 0, 0,
                        runtime->GetClock() + TDuration::Seconds(30), NKikimrBlobStorage::FastRead);
                    request->ForceGroupGeneration = generation;
                    SendToBSProxy(sender, readGroupId, request);
                });
                auto response = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(sender, false,
                    runtime->GetClock() + TDuration::Seconds(30));
                UNIT_ASSERT_C(response, "Timed out waiting for Get");
                UNIT_ASSERT_VALUES_EQUAL(response->Get()->Status, NKikimrProto::OK);
                UNIT_ASSERT_VALUES_EQUAL(response->Get()->ResponseSz, 1);
                if (rejoined || id.Step() <= 2) {
                    UNIT_ASSERT_VALUES_EQUAL(response->Get()->Responses[0].Status, NKikimrProto::OK);
                    UNIT_ASSERT_VALUES_EQUAL(response->Get()->Responses[0].Buffer.ConvertToString(), data);
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(response->Get()->Responses[0].Status, NKikimrProto::NODATA);
                }
            }
        };
        auto pileCounters = [&](const TString& pile) {
            const auto counters = GetServiceCounters(runtime->GetNode(env.Settings.ControllerNodeId)->AppData->Counters,
                "bridge")->GetSubgroup("subsystem", "bsc")->FindSubgroup("pile", pile);
            UNIT_ASSERT_C(counters, pile);
            return counters;
        };
        auto poolCounters = [&](const TString& pile, const TString& pool) {
            auto counters = pileCounters(pile)->FindSubgroup("database", pool);
            counters = counters ? counters->FindSubgroup("storagePool", pool) : nullptr;
            UNIT_ASSERT_C(counters, pile << ' ' << pool);
            return counters;
        };
        auto groupsInStage = [](const auto& counters, const TString& stage) {
            return counters->GetSubgroup("stage", stage)->GetCounter("GroupsInStage")->Val();
        };
        auto pileInState = [&](const TString& pile, const TString& state) {
            return pileCounters(pile)->GetSubgroup("state", state)->GetCounter("PileInState")->Val();
        };
        const std::array<TString, 2> rejoiningPiles{"pile_2", "pile_3"};

        put("before disconnect");
        put(TString(128_KB, 'a'));
        SwitchBridgePileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        SwitchBridgePileState(env, 2, NKikimrBridge::TClusterState::DISCONNECTED);
        // The root's acknowledgement can precede propagation to the local bridge proxy.
        runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(1), sender,
            new TEvNodeWardenQueryStorageConfig(true)), 1);
        const auto disconnectDeadline = runtime->GetClock() + TDuration::Seconds(30);
        for (;;) {
            auto response = env.WaitForEdgeActorEvent<TEvNodeWardenStorageConfig>(sender, false, disconnectDeadline);
            UNIT_ASSERT_C(response, "Timed out waiting for the local disconnected state");
            const auto& state = response->Get()->Config->GetClusterState();
            if (state.GetPerPileState(1) == NKikimrBridge::TClusterState::DISCONNECTED &&
                    state.GetPerPileState(2) == NKikimrBridge::TClusterState::DISCONNECTED) {
                break;
            }
        }
        runtime->Send(new IEventHandle(TEvents::TSystem::Unsubscribe, 0,
            MakeBlobStorageNodeWardenID(1), sender, nullptr, 0), 1);
        put("while two piles are disconnected");
        put(TString(128_KB, 'b'));
        checkData(pileGroups[0].GetRawId(), 1);
        for (size_t i = 1; i < pileGroups.size(); ++i) {
            checkData(pileGroups[i].GetRawId(), 1 + 8 * i, false);
        }

        holdSyncers = true;
        SwitchBridgePileState(env, 1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        SwitchBridgePileState(env, 2, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        const auto holdDeadline = runtime->GetClock() + TDuration::Minutes(1);
        while (!heldTargets.contains(pileGroups[1]) || !heldTargets.contains(pileGroups[2])) {
            UNIT_ASSERT_C(runtime->GetClock() < holdDeadline, "Syncers of the rejoining piles did not finish a stage");
            env.Sim(TDuration::Seconds(1));
        }
        env.Sim(TDuration::Seconds(6)); // BSC refreshes the counters every 5 s
        const auto numGroups = groupsInStage(poolCounters("pile_1", env.StoragePoolName), "SYNCED");
        UNIT_ASSERT_GT(numGroups, 0);
        for (const auto& pile : rejoiningPiles) {
            const auto counters = poolCounters(pile, env.StoragePoolName);
            UNIT_ASSERT_GT(groupsInStage(counters, "BLOCKS"), 0);
            UNIT_ASSERT_GT(counters->GetCounter("SyncersRunning")->Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL(pileCounters(pile)->GetCounter("UnsyncedBSC")->Val(), 1);
            UNIT_ASSERT_VALUES_EQUAL(pileInState(pile, "NOT_SYNCHRONIZED_2"), 1);
        }
        holdSyncers = false;
        // Fail the next stage of one target: SyncErrors must count it, and BSC must restart the stage.
        injectErrorTarget = pileGroups[1];
        for (auto& [nodeId, event] : heldSyncers) {
            runtime->Send(event.release(), nodeId);
        }
        heldSyncers.clear();

        const auto deadline = runtime->GetClock() + TDuration::Minutes(5);
        for (;;) {
            auto config = QueryStorageConfig(env, 1, runtime->GetClock() + TDuration::Seconds(30));
            const auto& state = config.GetClusterState();
            if (state.GetPerPileState(1) == NKikimrBridge::TClusterState::SYNCHRONIZED &&
                    state.GetPerPileState(2) == NKikimrBridge::TClusterState::SYNCHRONIZED) {
                UNIT_ASSERT_VALUES_EQUAL(config.GetClusterStateDetails().PileSyncStateSize(), 0);
                break;
            }
            UNIT_ASSERT_C(runtime->GetClock() < deadline, config.GetClusterStateDetails().DebugString()
                + state.DebugString());
            env.Sim(TDuration::Seconds(1));
        }
        env.Sim(TDuration::Seconds(6));
        for (const auto& pile : rejoiningPiles) {
            const auto counters = poolCounters(pile, env.StoragePoolName);
            UNIT_ASSERT_VALUES_EQUAL(groupsInStage(counters, "SYNCED"), numGroups);
            UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter("SyncersRunning")->Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter("SyncBytesDone")->Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter("SyncBytesTotal")->Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL(pileCounters(pile)->GetCounter("UnsyncedBSC")->Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL(pileInState(pile, "SYNCHRONIZED"), 1);
            UNIT_ASSERT_GT(groupsInStage(poolCounters(pile, "static"), "SYNCED"), 0);
        }
        // Other errors come from generation races of the two rejoining piles, so only the injected one is certain.
        UNIT_ASSERT_C(!injectErrorTarget, "No stage finished after the error injection was armed");
        UNIT_ASSERT_GE(poolCounters("pile_2", env.StoragePoolName)->GetCounter("SyncErrors", true)->Val(), 1);
        // Read the underlying groups directly: the bridge proxy must not mask missing copies.
        for (size_t i = 0; i < pileGroups.size(); ++i) {
            checkData(pileGroups[i].GetRawId(), 1 + 8 * i);
        }
    }


    // the syncer failing at once is started again after a while only, rather than over and over in a loop of BSC and
    // NodeWarden; once the reason is gone, the pile gets synced
    Y_UNIT_TEST(FailingSyncerRestartedWithBackoff) {
        TEnvironmentSetup env{{
            .NodeCount = 16,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .SelfManagementConfig = true,
            .NumPiles = 2,
            .AutomaticBootstrap = true,
        }};
        const ui32 groupId = NBridgeTest::CreatePoolAndGetGroup(env);
        NBridgeTest::TDataChecker checker(env);
        checker.Write(groupId);
        NBridgeTest::SwitchPileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        env.Sim(TDuration::Seconds(10));
        checker.Write(groupId);

        // every assimilation the syncers make fails, and so do the syncers
        bool failing = true;
        THashMap<TGroupId, ui32> failures; // by target group
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (failing && ev->GetTypeRewrite() == TEvBlobStorage::EvAssimilateResult && ev->HasEvent()) {
                if (IActor *actor = env.Runtime->GetActor(ev->GetRecipientRewrite()); actor &&
                        TypeName(*actor).Contains("TSyncerActor")) {
                    ev->Get<TEvBlobStorage::TEvAssimilateResult>()->Status = NKikimrProto::ERROR;
                }
            } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvNodeWardenNotifySyncerFinished && ev->HasEvent()) {
                if (const auto *msg = ev->Get<NStorage::TEvNodeWardenNotifySyncerFinished>(); msg->ErrorReason) {
                    ++failures[msg->TargetGroupId];
                }
            }
            return true;
        };
        NBridgeTest::SwitchPileState(env, 1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        env.Sim(TDuration::Minutes(1));
        UNIT_ASSERT(!failures.empty());
        for (const auto& [targetGroupId, num] : failures) {
            // the backoff from 100 ms to 30 s allows about ten attempts a minute
            UNIT_ASSERT_C(num <= 15, "TargetGroupId# " << targetGroupId << " failures# " << num);
        }

        failing = false;
        NBridgeTest::WaitForPileState(env, 1, NKikimrBridge::TClusterState::SYNCHRONIZED);
        env.Runtime->FilterFunction = {};
        checker.CheckAll();
        NBridgeTest::CheckPileData(env, checker, 1, 9);
    }

}
