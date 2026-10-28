#include "bridge_common.h"
#include "bridge_pile_ops.h"

#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/nodewarden/distconf.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_events.h>

#include <util/system/type_name.h>

using NBridgeTest::GetBridgedLocation;

namespace {

enum class ECompletionRace {
    NewGeneration,
    ReplacedActor,
    ReregisterAfterCompletion,
};

void TestLateSyncerCompletion(ECompletionRace race) {
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
    const auto sender = runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
    auto request = std::make_unique<TEvBlobStorage::TEvControllerGetGroup>();
    request->Record.AddGroupIDs(groupId);
    runtime->SendToPipe(env.TabletId, sender, request.release(), Max<ui64>(),
        TTestActorSystem::GetPipeConfigWithRetries());
    auto response = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>(sender, false,
        runtime->GetClock() + TDuration::Seconds(30));
    UNIT_ASSERT(response);
    auto serviceSet = response->Get()->Record.GetServiceSet();
    NKikimrBlobStorage::TGroupInfo *bridgeGroup = nullptr;
    for (auto& group : *serviceSet.MutableGroups()) {
        if (group.GetGroupID() == groupId) {
            bridgeGroup = &group;
        }
    }
    UNIT_ASSERT(bridgeGroup);
    const ui32 generation = bridgeGroup->GetGroupGeneration();
    const auto& piles = bridgeGroup->GetBridgeGroupState().GetPile();
    UNIT_ASSERT_VALUES_EQUAL(piles.size(), 3);
    const ui32 sourceGroupId = piles[0].GetGroupId();
    const ui32 targetGroupId = piles[1].GetGroupId();

    // The chosen NodeWarden need not host a source VDisk. Supply its group type explicitly.
    auto sourceRequest = std::make_unique<TEvBlobStorage::TEvControllerGetGroup>();
    sourceRequest->Record.AddGroupIDs(sourceGroupId);
    runtime->SendToPipe(env.TabletId, sender, sourceRequest.release(), Max<ui64>(),
        TTestActorSystem::GetPipeConfigWithRetries());
    auto sourceResponse = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>(sender, false,
        runtime->GetClock() + TDuration::Seconds(30));
    UNIT_ASSERT(sourceResponse);
    bool sourceFound = false;
    for (const auto& group : sourceResponse->Get()->Record.GetServiceSet().GetGroups()) {
        if (group.GetGroupID() == sourceGroupId) {
            serviceSet.AddGroups()->CopyFrom(group);
            sourceFound = true;
        }
    }
    UNIT_ASSERT(sourceFound);

    std::vector<std::unique_ptr<IEventHandle>> completions;
    std::vector<NKikimrBlobStorage::TEvControllerUpdateSyncerState> reports;
    std::vector<NKikimrBlobStorage::TEvControllerUpdateSyncerState> registrations;
    const IEventHandle *replayEvent = nullptr;
    runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& event) {
        // This test supplies assignments and group configurations directly to
        // NodeWarden without changing BSC state. Background BSC updates must not
        // replace these assignments or restart an already completed syncer.
        if (nodeId == 1 && event->GetTypeRewrite() ==
                TEvBlobStorage::TEvControllerNodeServiceSetUpdate::EventType && event->Sender != sender) {
            return false;
        }
        if (event->GetTypeRewrite() == NStorage::TEvNodeWardenNotifySyncerFinished::EventType &&
                event->Get<NStorage::TEvNodeWardenNotifySyncerFinished>()->BridgeProxyGroupId.GetRawId() == groupId) {
            if (event.get() == replayEvent) {
                replayEvent = nullptr;
            } else {
                completions.push_back(std::move(event));
                return false;
            }
        }
        if (event->GetTypeRewrite() == TEvBlobStorage::TEvControllerUpdateSyncerState::EventType) {
            const auto& record = event->Get<TEvBlobStorage::TEvControllerUpdateSyncerState>()->Record;
            for (const auto& syncer : record.GetSyncers()) {
                if (syncer.GetBridgeProxyGroupId() == groupId) {
                    reports.push_back(record);
                    // These assignments exercise NodeWarden directly, without advancing BSC state.
                    return false;
                }
            }
        }
        if (event->GetTypeRewrite() == TEvBlobStorage::TEvControllerRegisterNode::EventType) {
            const auto& record = event->Get<TEvBlobStorage::TEvControllerRegisterNode>()->Record;
            if (record.GetNodeID() == 1) {
                registrations.push_back(record.GetSyncerState());
            }
        }
        return true;
    };
    auto assign = [&](ui32 nextGeneration, bool withConfig, bool enabled = true) {
        auto update = std::make_unique<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>();
        auto& record = update->Record;
        if (withConfig) {
            record.MutableServiceSet()->CopyFrom(serviceSet);
        }
        record.SetUpdateSyncers(true);
        if (enabled) {
            auto *syncer = record.AddSyncers();
            syncer->SetBridgeProxyGroupId(groupId);
            syncer->SetBridgeProxyGroupGeneration(nextGeneration);
            syncer->SetSourceGroupId(sourceGroupId);
            syncer->SetTargetGroupId(targetGroupId);
        }
        runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(1), sender, update.release()), 1);
        // A reply from this mailbox fences processing of the assignment above.
        runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(1), sender,
            new TEvNodeWardenQueryGroupInfo(groupId)), 1);
        UNIT_ASSERT(env.WaitForEdgeActorEvent<TEvNodeWardenGroupInfo>(sender, false,
            runtime->GetClock() + TDuration::Seconds(5)));
    };
    auto waitCompletion = [&] {
        const auto deadline = runtime->GetClock() + TDuration::Seconds(5);
        while (completions.empty() && runtime->GetClock() < deadline) {
            env.Sim(TDuration::MilliSeconds(1));
        }
        UNIT_ASSERT_C(!completions.empty(), "Syncer did not complete");
        auto event = std::move(completions.front());
        completions.erase(completions.begin());
        return event;
    };
    auto deliver = [&](std::unique_ptr<IEventHandle> event) {
        replayEvent = event.get();
        runtime->Send(event.release(), 1);
        env.Sim(TDuration::MilliSeconds(100));
        UNIT_ASSERT(!replayEvent);
    };

    // A SYNCED target completes immediately; delay its notification to NodeWarden.
    assign(generation, true);
    auto oldCompletion = waitCompletion();
    if (race == ECompletionRace::ReregisterAfterCompletion) {
        deliver(std::move(oldCompletion));
        UNIT_ASSERT_VALUES_EQUAL(reports.size(), 1);
        UNIT_ASSERT(reports.front().GetSyncers(0).GetFinished());
        UNIT_ASSERT_VALUES_EQUAL(reports.front().GetSyncers(0).GetBridgeProxyGroupGeneration(), generation);
        assign(generation + 1, false);
        // Ignore background registrations captured before the reconnect being tested.
        registrations.clear();
        // Reconnect before the new group configuration arrives. Registration includes syncer state.
        runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(1), sender,
            new TEvTabletPipe::TEvClientDestroyed(env.TabletId, {}, {})), 1);
        const auto deadline = runtime->GetClock() + TDuration::Seconds(5);
        while (registrations.empty() && runtime->GetClock() < deadline) {
            env.Sim(TDuration::MilliSeconds(1));
        }
        UNIT_ASSERT_C(!registrations.empty(), "NodeWarden did not register after reconnect");
        bool found = false;
        for (const auto& syncer : registrations.front().GetSyncers()) {
            if (syncer.GetBridgeProxyGroupId() == groupId) {
                found = true;
                UNIT_ASSERT(syncer.GetFinished());
                UNIT_ASSERT_VALUES_EQUAL_C(syncer.GetBridgeProxyGroupGeneration(), generation,
                    "An old completion was reported for the pending generation");
            }
        }
        UNIT_ASSERT(found);
        runtime->FilterFunction = nullptr;
        return;
    }
    const bool changeGeneration = race == ECompletionRace::NewGeneration;
    const ui32 nextGeneration = generation + changeGeneration;
    if (changeGeneration) {
        // The assignment can arrive before the corresponding group configuration.
        assign(nextGeneration, false);
    } else {
        // Reassignment at the same generation still creates a different actor incarnation.
        assign(generation, false, false);
        assign(generation, false);
    }
    deliver(std::move(oldCompletion));
    for (const auto& report : reports) {
        for (const auto& syncer : report.GetSyncers()) {
            UNIT_ASSERT_C(!syncer.GetFinished(), "Obsolete actor completed a new assignment: " << report.DebugString());
        }
    }
    reports.clear();
    if (changeGeneration) {
        bridgeGroup->SetGroupGeneration(nextGeneration);
        assign(nextGeneration, true);
    }
    deliver(waitCompletion());
    UNIT_ASSERT_VALUES_EQUAL(reports.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(reports.front().SyncersSize(), 1);
    const auto& finished = reports.front().GetSyncers(0);
    UNIT_ASSERT(finished.GetFinished());
    UNIT_ASSERT_VALUES_EQUAL(finished.GetBridgeProxyGroupGeneration(), nextGeneration);
    runtime->FilterFunction = nullptr;
}

} // namespace

Y_UNIT_TEST_SUITE(BridgeSyncerGeneration) {
    Y_UNIT_TEST(LateCompletionWhileWaitingForNewConfiguration) {
        TestLateSyncerCompletion(ECompletionRace::NewGeneration);
    }

    Y_UNIT_TEST(LateCompletionFromReplacedActor) {
        TestLateSyncerCompletion(ECompletionRace::ReplacedActor);
    }

    Y_UNIT_TEST(RegistrationPreservesCompletedGeneration) {
        TestLateSyncerCompletion(ECompletionRace::ReregisterAfterCompletion);
    }

    // the reply to the commit of the static part as SYNCED comes by another path than configs do, so the config with the
    // part SYNCED and even the one resetting it again (the pile goes out) may come first; the reply then belongs to the
    // syncer entry that is gone and must not touch the new one
    Y_UNIT_TEST(StaticCommitReplyAfterReset) {
        using namespace NBridgeTest;

        TEnvironmentSetup env{{
            .NodeCount = 16,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .SelfManagementConfig = true,
            .NumPiles = 2,
            .AutomaticBootstrap = true,
        }};
        const ui32 groupId = CreatePoolAndGetGroup(env);
        TDataChecker checker(env);
        checker.Write(groupId);

        auto isBsc = [&](const TActorId& actorId) {
            IActor *actor = env.Runtime->GetActor(actorId);
            return actor && TypeName(*actor).Contains("TBlobStorageController");
        };
        bool holding = true;
        THashSet<ui64> syncedCommitCookies;
        std::vector<std::tuple<ui32, std::unique_ptr<IEventHandle>>> held;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NStorage::TEvNodeConfigInvokeOnRoot::EventType && isBsc(ev->Sender)) {
                const auto& record = ev->Get<NStorage::TEvNodeConfigInvokeOnRoot>()->Record;
                if (record.HasUpdateBridgeGroupInfo()) {
                    const auto& state = record.GetUpdateBridgeGroupInfo().GetBridgeGroupInfo().GetBridgeGroupState();
                    const auto *pile = NBridge::GetGroupPile(state, TBridgePileId::FromPileIndex(1));
                    if (pile && pile->GetStage() == NKikimrBridge::TGroupState::SYNCED) {
                        syncedCommitCookies.insert(ev->Cookie);
                    }
                }
            } else if (holding && ev->GetTypeRewrite() == NStorage::TEvNodeConfigInvokeOnRootResult::EventType &&
                    syncedCommitCookies.contains(ev->Cookie) && isBsc(ev->GetRecipientRewrite())) {
                held.emplace_back(nodeId, std::move(ev));
                return false;
            }
            return true;
        };

        SwitchPileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        env.Sim(TDuration::Seconds(10));
        checker.Write(groupId);
        RejoinPile(env, 1); // the static part gets SYNCED while BSC doesn't know its commit has finished
        UNIT_ASSERT(!held.empty());

        // the pile goes out again, its static part gets reset and BSC makes new syncer entry for it
        SwitchPileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        env.Sim(TDuration::Seconds(10));

        holding = false;
        for (auto& [nodeId, ev] : std::exchange(held, {})) {
            env.Runtime->Send(ev.release(), nodeId);
        }
        env.Sim(TDuration::Seconds(10));

        checker.Write(groupId);
        RejoinPile(env, 1);
        checker.CheckAll();
        CheckPileData(env, checker, 1, 9);
    }
}
