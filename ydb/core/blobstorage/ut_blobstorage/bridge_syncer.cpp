#include "bridge_common.h"
#include "bridge_pile_ops.h"

#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/bridge/syncer/syncer.h>
#include <ydb/core/blobstorage/nodewarden/distconf.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_events.h>
#include <ydb/library/testlib/helpers.h>

#include <util/system/type_name.h>

#include <algorithm>
#include <map>
#include <set>

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

    // The chosen NodeWarden need not host a source VDisk, and this test keeps BSC updates away from it. Supply the source
    // group explicitly.
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

void TestSyncerQuotaDelay(bool cancel) {
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
    auto group = GetGroupInfo(env, groupId);
    auto *state = group.MutableBridgeGroupState();
    const auto source = env.GetGroupInfo(state->GetPile(0).GetGroupId());
    const auto target = env.GetGroupInfo(state->GetPile(1).GetGroupId());
    state->MutablePile(1)->SetStage(NKikimrBridge::TGroupState::WRITE_KEEP_BARRIER_DONOTKEEP_DATA);
    const auto info = TBlobStorageGroupInfo::Parse(group, nullptr, nullptr);
    UNIT_ASSERT(info);
    const TString data = "quota-delayed copy";
    const TLogoBlobID id(100500, 1, 1, 0, data.size(), 0);
    auto put = QueryPileGroup<TEvBlobStorage::TEvPutResult>(env, source->GroupID.GetRawId(), 1,
        new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
    UNIT_ASSERT_VALUES_EQUAL(put->Status, NKikimrProto::OK);

    auto quoter = std::make_shared<TReplQuoter>(1000); // metadata has to take a small part of this
    const auto edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
    env.Runtime->WrapInActorContext(edge, [&] {
        quoter->Take(TActivationContext::Monotonic(), 600'000); // reserve ten minutes before the syncer's read
    });
    TActorId syncer;
    bool targetChecked = false;
    ui32 sourceReads = 0;
    std::unique_ptr<IEventHandle> completion;
    env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
        if (ev->Sender == syncer && ev->GetTypeRewrite() == TEvBlobStorage::EvGet) {
            if (ev->Get<TEvBlobStorage::TEvGet>()->IsIndexOnly) {
                targetChecked = true;
            } else {
                ++sourceReads;
            }
        }
        if (ev->Sender == syncer && ev->GetTypeRewrite() == NStorage::TEvNodeWardenNotifySyncerFinished::EventType) {
            UNIT_ASSERT(!completion);
            completion = std::move(ev);
            return false;
        }
        return true;
    };
    syncer = env.Runtime->Register(NBridge::CreateSyncerActor(info, source->GroupID, target->GroupID,
        std::make_shared<NBridge::TSyncerDataStats>(), quoter), 1);
    WaitUntil(env, [&] { return targetChecked; }, "target index query before throttled read");
    env.Sim(TDuration::Minutes(6));
    UNIT_ASSERT(!completion);
    UNIT_ASSERT_VALUES_EQUAL_C(sourceReads, 0, "the read must wait for its quota");
    if (cancel) {
        env.Runtime->Send(new IEventHandle(syncer, edge, new TEvents::TEvPoison), 1);
        env.Sim(TDuration::Minutes(5));
        UNIT_ASSERT_VALUES_EQUAL_C(sourceReads, 0, "a stopped syncer must not dispatch its delayed request");
        UNIT_ASSERT(!completion);
    } else {
        WaitUntil(env, [&] { return bool(completion); }, "quota-delayed sync completion", TDuration::Seconds(1), 300);
        UNIT_ASSERT_VALUES_EQUAL(sourceReads, 1);
        UNIT_ASSERT(!completion->Get<NStorage::TEvNodeWardenNotifySyncerFinished>()->ErrorReason);
        UNIT_ASSERT_VALUES_EQUAL(ReadFromPileGroup(env, target->GroupID.GetRawId(), 9, id, data), NKikimrProto::OK);
    }
    env.Runtime->FilterFunction = {};
}

void SetBridgeSyncTotalRate(TEnvironmentSetup& env, ui64 total) {
    NKikimrBlobStorage::TConfigRequest request;
    request.AddCommand()->MutableUpdateSettings()->AddBridgeSyncTotalRateBytesPerSecond(total);
    const auto response = env.Invoke(request);
    UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
}

struct TSyncerRead {
    TInstant Time;
    ui32 NodeId;
};

// Disconnect the target piles, write blobs through the bridge proxy, bring the piles back and wait for them to get
// synchronized. Returns the reads of data from the source group of the first pile, ordered by time. The filter function
// set by the caller stays in effect.
std::vector<TSyncerRead> SyncBlobs(TEnvironmentSetup& env, ui32 groupId, const std::vector<ui32>& targetPiles,
        ui32 numBlobs, ui32 blobSize) {
    using namespace NBridgeTest;
    const ui32 sourceGroupId = GetGroupInfo(env, groupId).GetBridgeGroupState().GetPile(0).GetGroupId();
    for (ui32 pile : targetPiles) {
        SwitchBridgePileState(env, pile, NKikimrBridge::TClusterState::DISCONNECTED);
        WaitForPileState(env, pile, NKikimrBridge::TClusterState::DISCONNECTED);
    }
    env.Sim(TDuration::Seconds(10)); // let the bridge proxy learn the state

    const TString data(blobSize, 'x');
    const auto sender = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
    for (ui32 i = 0; i < numBlobs; ++i) {
        env.Runtime->WrapInActorContext(sender, [&] {
            SendToBSProxy(sender, groupId, new TEvBlobStorage::TEvPut(TLogoBlobID(100500, 1, i + 1, 0, data.size(), 0),
                data, TInstant::Max()));
        });
        const auto put = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(sender, false);
        UNIT_ASSERT_VALUES_EQUAL(put->Get()->Status, NKikimrProto::OK);
    }

    std::vector<TSyncerRead> reads;
    const TActorId sourceProxyId = MakeBlobStorageProxyID(sourceGroupId);
    auto callerFilter = std::move(env.Runtime->FilterFunction);
    env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == TEvBlobStorage::EvGet && ev->Recipient == sourceProxyId &&
                !ev->Get<TEvBlobStorage::TEvGet>()->IsIndexOnly) {
            reads.push_back({env.Runtime->GetClock(), nodeId});
        }
        return !callerFilter || callerFilter(nodeId, ev);
    };
    for (ui32 pile : targetPiles) {
        SwitchBridgePileState(env, pile, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
    }
    for (ui32 pile : targetPiles) {
        WaitForPileState(env, pile, NKikimrBridge::TClusterState::SYNCHRONIZED);
    }
    env.Runtime->FilterFunction = std::move(callerFilter);
    return reads;
}

// In bytes per second; with no limit all the reads may go at the same moment.
double GetReadRate(const std::vector<TSyncerRead>& reads, ui32 blobSize) {
    const TDuration span = reads.back().Time - reads.front().Time;
    return span ? (reads.size() - 1) * blobSize / span.SecondsFloat() : Max<double>();
}

// Checks that the node reads at the rate between the bounds, in bytes per second; both limits are applied before the sync.
void TestSyncRate(ui64 nodeLimit, ui64 totalRate, double minRate, double maxRate) {
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
    const auto replace = ReplaceMainConfig(env, [&](YAML::Node& config) {
        config["blob_storage_config"]["bridge_sync_rate_bytes_per_second"] = nodeLimit;
    });
    UNIT_ASSERT_EQUAL_C(replace.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK, replace.GetErrorReason());
    SetBridgeSyncTotalRate(env, totalRate);

    constexpr ui32 numBlobs = 20;
    constexpr ui32 blobSize = 64_KB;
    const auto reads = SyncBlobs(env, groupId, {1}, numBlobs, blobSize);
    UNIT_ASSERT_GE(reads.size(), numBlobs);
    const double rate = GetReadRate(reads, blobSize);
    UNIT_ASSERT_C(minRate <= rate && rate <= maxRate, "rate# " << rate << " minRate# " << minRate << " maxRate# " << maxRate
        << " numReads# " << reads.size());
}

// Every node has a share got from a BSC that is gone, and registers at a BSC telling nothing about it. A BSC that keeps
// the share of the node doesn't know it yet, while any other one doesn't support shares at all.
void TestShareAfterRegistration(bool keepShare) {
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

    const TActorId sender = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
    std::set<ui32> registeredNodes;
    env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == TEvBlobStorage::TEvControllerNodeServiceSetUpdate::EventType && ev->Sender != sender) {
            auto& record = ev->Get<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>()->Record;
            record.ClearBridgeSyncNodeRateBytesPerSecond();
            record.ClearKeepBridgeSyncNodeRate();
            if (record.GetComprehensive()) {
                registeredNodes.insert(nodeId);
                if (keepShare) {
                    record.SetKeepBridgeSyncNodeRate(true);
                }
            }
        }
        return true;
    };

    constexpr ui64 share = 100'000;
    const auto nodes = env.Runtime->GetNodes();
    for (ui32 nodeId : nodes) {
        auto update = std::make_unique<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>();
        update->Record.SetBridgeSyncNodeRateBytesPerSecond(share);
        env.Runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(nodeId), sender, update.release()), nodeId);
        env.Runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(nodeId), sender,
            new TEvTabletPipe::TEvClientDestroyed(env.TabletId, {}, {})), nodeId);
    }
    WaitUntil(env, [&] { return registeredNodes.size() == nodes.size(); }, "registration of the nodes", TDuration::Seconds(1));

    constexpr ui32 numBlobs = 20;
    constexpr ui32 blobSize = 64_KB;
    const auto reads = SyncBlobs(env, groupId, {1}, numBlobs, blobSize);
    env.Runtime->FilterFunction = {};
    UNIT_ASSERT_GE(reads.size(), numBlobs);
    const double rate = GetReadRate(reads, blobSize);
    const double minRate = keepShare ? 0 : 10'000'000; // the default limit per node is 500 MB/s
    const double maxRate = keepShare ? share * 1.2 : Max<double>();
    UNIT_ASSERT_C(minRate <= rate && rate <= maxRate, "rate# " << rate << " numReads# " << reads.size());
}

// A cluster with syncers kept on their nodes by holding their completions, and with the rates sent by BSC recorded.
struct THeldSyncersEnv {
    static constexpr ui32 NumGroups = 8;
    static constexpr ui64 TotalRate = 160'000'000;

    TEnvironmentSetup Env{{
        .NodeCount = 16,
        .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        .LocationGenerator = NBridgeTest::GetBridgedLocation,
        .SelfManagementConfig = true,
        .NumPiles = 2,
        .AutomaticBootstrap = true,
    }};
    std::map<ui32, ui64> Rates;
    std::map<ui32, ui32> NumNodeSyncers; // of all groups, as the last update from BSC says
    std::function<void(ui32, const NKikimrBlobStorage::TEvControllerNodeServiceSetUpdate&)> OnUpdate;
    bool DropRates = false;
    bool HoldSyncers = false;
    std::vector<std::pair<ui32, std::unique_ptr<IEventHandle>>> HeldSyncers;

    THeldSyncersEnv() {
        NBridgeTest::CreatePoolAndGetGroup(Env, NumGroups);
        Env.Runtime->FilterFunction = [this](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::TEvControllerNodeServiceSetUpdate::EventType) {
                const auto& record = ev->Get<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>()->Record;
                if (DropRates && !record.GetUpdateSyncers() && record.HasBridgeSyncNodeRateBytesPerSecond()) {
                    return false;
                }
                if (OnUpdate) {
                    OnUpdate(nodeId, record);
                }
                if (record.HasBridgeSyncNodeRateBytesPerSecond()) {
                    Rates[nodeId] = record.GetBridgeSyncNodeRateBytesPerSecond();
                }
                if (record.GetUpdateSyncers()) {
                    NumNodeSyncers[nodeId] = record.SyncersSize();
                }
            }
            if (HoldSyncers && ev->GetTypeRewrite() == NStorage::TEvNodeWardenNotifySyncerFinished::EventType) {
                HeldSyncers.emplace_back(nodeId, std::move(ev));
                return false;
            }
            return true;
        };
        SetBridgeSyncTotalRate(Env, TotalRate);
        Env.Sim(TDuration::Seconds(5));
        NBridgeTest::SwitchBridgePileState(Env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        NBridgeTest::WaitForPileState(Env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        HoldSyncers = true;
        NBridgeTest::SwitchBridgePileState(Env, 1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        NBridgeTest::WaitUntil(Env, [&] { return HeldSyncers.size() >= NumGroups; }, "syncers of all groups");
        Env.Sim(TDuration::Seconds(5));
        UNIT_ASSERT_GT(GetNumHostingNodes(), 1);
        CheckRates(TotalRate);
    }

    ~THeldSyncersEnv() {
        Env.Runtime->FilterFunction = {};
    }

    // Lets the held completions go; the filter applies to them again, so it stops holding them first.
    void ReleaseHeldSyncers() {
        HoldSyncers = false;
        for (auto& [nodeId, ev] : HeldSyncers) {
            Env.Runtime->Send(ev.release(), nodeId);
        }
        HeldSyncers.clear();
    }

    ui64 GetNumHostingNodes() const {
        ui64 res = 0;
        for (const auto& [nodeId, numSyncers] : NumNodeSyncers) {
            res += numSyncers != 0;
        }
        return res;
    }

    void CheckRates(ui64 total) {
        UNIT_ASSERT(GetNumHostingNodes());
        const ui64 share = total / GetNumHostingNodes();
        for (const auto& [nodeId, numSyncers] : NumNodeSyncers) {
            UNIT_ASSERT_VALUES_EQUAL_C(Rates[nodeId], numSyncers ? share : 0, "nodeId# " << nodeId);
        }
    }
};

} // namespace

Y_UNIT_TEST_SUITE(BridgeSyncerQuota) {
    Y_UNIT_TEST(QuotaDelayedRequestCompletes) {
        TestSyncerQuotaDelay(false);
    }

    Y_UNIT_TEST(StoppingSyncerCancelsQuotaDelayedRequest) {
        TestSyncerQuotaDelay(true);
    }

    // Reads are charged by the size of the blob when they are sent, so the ones still waiting for an in-flight slot get
    // the rate that is current by then.
    Y_UNIT_TEST(QuotaTakenWhenReadIsSent) {
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
        auto group = GetGroupInfo(env, groupId);
        auto *state = group.MutableBridgeGroupState();
        const auto source = env.GetGroupInfo(state->GetPile(0).GetGroupId());
        const auto target = env.GetGroupInfo(state->GetPile(1).GetGroupId());
        state->MutablePile(1)->SetStage(NKikimrBridge::TGroupState::WRITE_KEEP_BARRIER_DONOTKEEP_DATA);
        const auto info = TBlobStorageGroupInfo::Parse(group, nullptr, nullptr);
        UNIT_ASSERT(info);

        constexpr ui32 numBlobs = 20;
        const TString data(64_KB, 'x');
        for (ui32 i = 0; i < numBlobs; ++i) {
            const TLogoBlobID id(100500, 1, i + 1, 0, data.size(), 0);
            auto put = QueryPileGroup<TEvBlobStorage::TEvPutResult>(env, source->GroupID.GetRawId(), 1,
                new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
            UNIT_ASSERT_VALUES_EQUAL(put->Status, NKikimrProto::OK);
        }

        auto quoter = std::make_shared<TReplQuoter>(data.size() / 10); // ten seconds per blob, metadata takes a small part of this
        TActorId syncer;
        std::vector<TInstant> reads;
        std::unique_ptr<IEventHandle> completion;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->Sender == syncer && ev->GetTypeRewrite() == TEvBlobStorage::EvGet &&
                    !ev->Get<TEvBlobStorage::TEvGet>()->IsIndexOnly) {
                reads.push_back(env.Runtime->GetClock());
            }
            if (ev->Sender == syncer && ev->GetTypeRewrite() == NStorage::TEvNodeWardenNotifySyncerFinished::EventType) {
                completion = std::move(ev);
                return false;
            }
            return true;
        };
        syncer = env.Runtime->Register(NBridge::CreateSyncerActor(info, source->GroupID, target->GroupID,
            std::make_shared<NBridge::TSyncerDataStats>(), quoter), 1);

        // the first read goes at once and the second one waits for the time it has been charged; others hold no quota yet
        WaitUntil(env, [&] { return !reads.empty(); }, "first read", TDuration::Seconds(1), 60);
        quoter->UpdateBytesPerSecond(1'000'000);
        WaitUntil(env, [&] { return bool(completion); }, "sync completion", TDuration::Seconds(1), 300);
        env.Runtime->FilterFunction = {};

        UNIT_ASSERT(!completion->Get<NStorage::TEvNodeWardenNotifySyncerFinished>()->ErrorReason);
        UNIT_ASSERT_VALUES_EQUAL(reads.size(), numBlobs);
        const TDuration gap = reads[1] - reads[0];
        UNIT_ASSERT_C(TDuration::Seconds(9) <= gap && gap <= TDuration::Seconds(11), gap);
        // the time of two reads at the old rate, and the others take about 1 s at the new one; when charged for all the
        // reads having in-flight slots it would be 150 s
        const TDuration span = reads.back() - reads.front();
        UNIT_ASSERT_C(span <= TDuration::Seconds(25), span);
    }

    // The quota can't be returned, so what a stopped syncer has taken delays the next one. The syncer gets restarted whenever
    // another part of its group changes the stage.
    Y_UNIT_TEST(StoppedSyncerLeavesLittleQuotaTaken) {
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
        auto group = GetGroupInfo(env, groupId);
        auto *state = group.MutableBridgeGroupState();
        const auto source = env.GetGroupInfo(state->GetPile(0).GetGroupId());
        const auto target = env.GetGroupInfo(state->GetPile(1).GetGroupId());
        state->MutablePile(1)->SetStage(NKikimrBridge::TGroupState::WRITE_KEEP_BARRIER_DONOTKEEP_DATA);
        const auto info = TBlobStorageGroupInfo::Parse(group, nullptr, nullptr);
        UNIT_ASSERT(info);

        constexpr ui32 numBlobs = 20;
        const TString data(64_KB, 'x');
        for (ui32 i = 0; i < numBlobs; ++i) {
            const TLogoBlobID id(100500, 1, i + 1, 0, data.size(), 0);
            auto put = QueryPileGroup<TEvBlobStorage::TEvPutResult>(env, source->GroupID.GetRawId(), 1,
                new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
            UNIT_ASSERT_VALUES_EQUAL(put->Status, NKikimrProto::OK);
        }

        auto quoter = std::make_shared<TReplQuoter>(data.size() / 10); // ten seconds per blob
        TActorId syncer;
        ui32 numReads = 0;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            numReads += ev->Sender == syncer && ev->GetTypeRewrite() == TEvBlobStorage::EvGet &&
                !ev->Get<TEvBlobStorage::TEvGet>()->IsIndexOnly;
            return true;
        };
        syncer = env.Runtime->Register(NBridge::CreateSyncerActor(info, source->GroupID, target->GroupID,
            std::make_shared<NBridge::TSyncerDataStats>(), quoter), 1);
        WaitUntil(env, [&] { return numReads != 0; }, "first read", TDuration::Seconds(1), 60);
        const auto edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        env.Runtime->Send(new IEventHandle(syncer, edge, new TEvents::TEvPoison), 1);
        env.Sim(TDuration::Seconds(1));
        env.Runtime->FilterFunction = {};
        UNIT_ASSERT_VALUES_EQUAL(numReads, 1);

        TDuration wait;
        env.Runtime->WrapInActorContext(edge, [&] {
            wait = quoter->Take(TActivationContext::Monotonic(), 0);
        });
        // the read sent and the one waiting for its time; when charged for all the reads having in-flight slots it would be
        // 150 s
        UNIT_ASSERT_C(wait <= TDuration::Seconds(25), wait);
    }

    // All the blobs are in the target already, so nothing but metadata comes from the source pile.
    Y_UNIT_TEST(MetadataIsCharged) {
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
        auto group = GetGroupInfo(env, groupId);
        auto *state = group.MutableBridgeGroupState();
        const auto source = env.GetGroupInfo(state->GetPile(0).GetGroupId());
        const auto target = env.GetGroupInfo(state->GetPile(1).GetGroupId());
        state->MutablePile(1)->SetStage(NKikimrBridge::TGroupState::WRITE_KEEP_BARRIER_DONOTKEEP_DATA);
        const auto info = TBlobStorageGroupInfo::Parse(group, nullptr, nullptr);
        UNIT_ASSERT(info);

        constexpr ui32 numBlobs = 200;
        const TString data(100, 'x');
        for (const auto& [pile, nodeId] : {std::make_pair(source, 1u), std::make_pair(target, 9u)}) {
            for (ui32 i = 0; i < numBlobs; ++i) {
                const TLogoBlobID id(100500, 1, i + 1, 0, data.size(), 0);
                auto put = QueryPileGroup<TEvBlobStorage::TEvPutResult>(env, pile->GroupID.GetRawId(), nodeId,
                    new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
                UNIT_ASSERT_VALUES_EQUAL(put->Status, NKikimrProto::OK);
            }
        }

        constexpr ui64 rate = 1000;
        auto quoter = std::make_shared<TReplQuoter>(rate);
        const TActorId sourceProxy = MakeBlobStorageProxyID(source->GroupID.GetRawId());
        TActorId syncer;
        std::vector<ui64> sourcePageBytes; // as VDisks have sent them, for each request to the source group
        TInstant lastSourceRequest;
        std::multiset<ui64> resultBytes;
        ui32 sourceReads = 0;
        std::unique_ptr<IEventHandle> completion;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            const ui32 type = ev->GetTypeRewrite();
            if (type == TEvBlobStorage::EvAssimilate && ev->Sender == syncer && ev->Recipient == sourceProxy) {
                sourcePageBytes.push_back(0);
                lastSourceRequest = env.Runtime->GetClock();
            } else if (type == TEvBlobStorage::EvVAssimilateResult) {
                // the reply gets here from the VDisk and from the queue, and only the latter goes to the request actor
                const IActor *recipient = env.Runtime->GetActor(ev->GetRecipientRewrite());
                const auto& record = ev->Get<TEvBlobStorage::TEvVAssimilateResult>()->Record;
                if (recipient && TypeName(*recipient).Contains("AssimilateRequest") &&
                        VDiskIDFromVDiskID(record.GetVDiskID()).GroupID == source->GroupID) {
                    UNIT_ASSERT(!sourcePageBytes.empty());
                    sourcePageBytes.back() += record.ByteSizeLong();
                }
            } else if (type == TEvBlobStorage::EvAssimilateResult && ev->Recipient == syncer) {
                resultBytes.insert(ev->Get<TEvBlobStorage::TEvAssimilateResult>()->ReplyBytes);
            } else if (type == TEvBlobStorage::EvGet && ev->Sender == syncer && ev->Recipient == sourceProxy) {
                ++sourceReads;
            } else if (type == NStorage::TEvNodeWardenNotifySyncerFinished::EventType && ev->Sender == syncer) {
                completion = std::move(ev);
                return false;
            }
            return true;
        };
        syncer = env.Runtime->Register(NBridge::CreateSyncerActor(info, source->GroupID, target->GroupID,
            std::make_shared<NBridge::TSyncerDataStats>(), quoter), 1);
        WaitUntil(env, [&] { return bool(completion); }, "sync completion", TDuration::MilliSeconds(100), 3000);
        env.Runtime->FilterFunction = {};
        UNIT_ASSERT(!completion->Get<NStorage::TEvNodeWardenNotifySyncerFinished>()->ErrorReason);
        UNIT_ASSERT_VALUES_EQUAL(sourceReads, 0);

        UNIT_ASSERT_GE(sourcePageBytes.size(), 2);
        ui64 charged = 0;
        for (size_t i = 0; i + 1 < sourcePageBytes.size(); ++i) {
            UNIT_ASSERT_C(resultBytes.contains(sourcePageBytes[i]), "page# " << i << " bytes# " << sourcePageBytes[i]);
            charged += sourcePageBytes[i];
        }
        UNIT_ASSERT_GT(charged, numBlobs);

        // a request doesn't wait for its own charge, so what is left to wait is the charge minus the unused capacity and
        // the time passed since the last request
        const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        TDuration wait;
        env.Runtime->WrapInActorContext(edge, [&] {
            wait = quoter->Take(TActivationContext::Monotonic(), 0);
        });
        const TDuration chargedTime = TDuration::MicroSeconds(charged * 1'000'000 / rate);
        const TDuration passed = env.Runtime->GetClock() - lastSourceRequest + TReplQuoter::GetCapacity();
        UNIT_ASSERT_C(passed + TDuration::Seconds(1) < chargedTime, "the test has to check the wait before it is over"
            << " passed# " << passed << " chargedTime# " << chargedTime);
        const TDuration expected = chargedTime - passed;
        UNIT_ASSERT_C(expected * 0.9 <= wait && wait <= expected * 1.1, "wait# " << wait << " expected# " << expected
            << " charged# " << charged);
    }

    Y_UNIT_TEST(DataUsesBackgroundClasses) {
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
        const auto state = GetGroupInfo(env, groupId).GetBridgeGroupState();
        const TActorId sourceProxy = MakeBlobStorageProxyID(state.GetPile(0).GetGroupId());
        const TActorId targetProxy = MakeBlobStorageProxyID(state.GetPile(1).GetGroupId());
        constexpr ui32 numBlobs = 5;
        ui32 numReads = 0;
        ui32 numPuts = 0;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvGet && ev->Recipient == sourceProxy) {
                const auto *msg = ev->Get<TEvBlobStorage::TEvGet>();
                numReads += !msg->IsIndexOnly && msg->GetHandleClass == NKikimrBlobStorage::AsyncRead;
            } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvPut && ev->Recipient == targetProxy) {
                numPuts += ev->Get<TEvBlobStorage::TEvPut>()->HandleClass == NKikimrBlobStorage::AsyncBlob;
            }
            return true;
        };
        const auto reads = SyncBlobs(env, groupId, {1}, numBlobs, 1000);
        env.Runtime->FilterFunction = {};
        UNIT_ASSERT_VALUES_EQUAL(reads.size(), numBlobs);
        UNIT_ASSERT_VALUES_EQUAL(numReads, numBlobs);
        UNIT_ASSERT_VALUES_EQUAL(numPuts, numBlobs);
    }

    // Every read that has finished issues a put, so before the first put only the reads in flight may have been sent.
    Y_UNIT_TEST(PutsGoAheadOfQueuedReads) {
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
        const auto state = GetGroupInfo(env, groupId).GetBridgeGroupState();
        const TActorId sourceProxy = MakeBlobStorageProxyID(state.GetPile(0).GetGroupId());
        const TActorId targetProxy = MakeBlobStorageProxyID(state.GetPile(1).GetGroupId());
        constexpr ui32 maxQueriesInFlight = 16;
        ui32 numReads = 0;
        std::optional<ui32> readsBeforeFirstPut;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvGet && ev->Recipient == sourceProxy) {
                numReads += !ev->Get<TEvBlobStorage::TEvGet>()->IsIndexOnly;
            } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvPut && ev->Recipient == targetProxy && !readsBeforeFirstPut) {
                readsBeforeFirstPut = numReads;
            }
            return true;
        };
        constexpr ui32 numBlobs = 40;
        SyncBlobs(env, groupId, {1}, numBlobs, 1000);
        env.Runtime->FilterFunction = {};
        UNIT_ASSERT_VALUES_EQUAL(numReads, numBlobs);
        UNIT_ASSERT(readsBeforeFirstPut);
        UNIT_ASSERT_GT(*readsBeforeFirstPut, 0);
        UNIT_ASSERT_LE(*readsBeforeFirstPut, maxQueriesInFlight);
    }

    Y_UNIT_TEST(TotalRateSharedBetweenHostingNodes) {
        using namespace NBridgeTest;
        THeldSyncersEnv test;
        auto& env = test.Env;
        ui32 numRateOnlyUpdates = 0;
        test.OnUpdate = [&](ui32, const auto& record) {
            numRateOnlyUpdates += record.HasBridgeSyncNodeRateBytesPerSecond() && !record.GetUpdateSyncers();
        };
        auto setTotalRate = [&](ui64 total) {
            SetBridgeSyncTotalRate(env, total);
            env.Sim(TDuration::Seconds(5));
        };

        // nothing gets sent while the placement and the limit stay the same
        constexpr ui64 total = THeldSyncersEnv::TotalRate;
        env.Sim(TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(numRateOnlyUpdates, 0);

        setTotalRate(total / 2);
        UNIT_ASSERT_GT(numRateOnlyUpdates, 0);
        test.CheckRates(total / 2);

        setTotalRate(0);
        test.CheckRates(0);

        // the nodes that have stopped hosting syncers have no share left
        setTotalRate(total);
        test.CheckRates(total);
        test.ReleaseHeldSyncers();
        WaitForPileState(env, 1, NKikimrBridge::TClusterState::SYNCHRONIZED);
        env.Sim(TDuration::Seconds(5));
        for (const auto& [nodeId, rate] : test.Rates) {
            UNIT_ASSERT_VALUES_EQUAL_C(rate, 0, "nodeId# " << nodeId);
        }
    }

    Y_UNIT_TEST(TotalRateLimitsReads) {
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

        constexpr ui64 total = 1'000'000;
        SetBridgeSyncTotalRate(env, total);

        constexpr ui32 numBlobs = 20;
        constexpr ui32 blobSize = 64_KB;
        const auto reads = SyncBlobs(env, groupId, {1}, numBlobs, blobSize);

        // with the limit per node only, 500 MB/s by default, these reads take much less; the node may get a smaller share
        // than the whole limit, which may only make it longer
        UNIT_ASSERT_GE(reads.size(), numBlobs);
        const TDuration expected = TDuration::MicroSeconds(ui64(numBlobs - 1) * blobSize * 1'000'000 / total)
            - TReplQuoter::GetCapacity();
        const TDuration span = reads.back().Time - reads.front().Time;
        UNIT_ASSERT_C(expected <= span, "span# " << span << " expected# " << expected);
    }

    // Target piles consist of different nodes, so each of them gets a share; both share the link leaving the source pile.
    Y_UNIT_TEST(TotalRateLimitsReadsForTwoTargetPiles) {
        using namespace NBridgeTest;
        TEnvironmentSetup env{{
            .NodeCount = 24,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .SelfManagementConfig = true,
            .NumPiles = 3,
            .AutomaticBootstrap = true,
        }};
        const ui32 groupId = CreatePoolAndGetGroup(env);

        constexpr ui64 total = 100'000;
        SetBridgeSyncTotalRate(env, total);

        constexpr ui32 numBlobs = 40;
        constexpr ui32 blobSize = 64_KB;
        const auto reads = SyncBlobs(env, groupId, {1, 2}, numBlobs, blobSize);

        std::map<ui32, std::vector<TInstant>> readsByPile;
        for (const auto& read : reads) {
            readsByPile[(read.NodeId - 1) / 8].push_back(read.Time);
        }
        UNIT_ASSERT_VALUES_EQUAL(readsByPile.size(), 2);
        const auto& first = readsByPile.at(1);
        const auto& second = readsByPile.at(2);
        UNIT_ASSERT_GE(first.size(), numBlobs);
        UNIT_ASSERT_GE(second.size(), numBlobs);

        // Only the time when both piles are reading tells anything about the division of the limit. A syncer gets restarted
        // when another part of its group changes the stage, so neither the number of reads nor the total time is exact.
        const TInstant overlapBegin = Max(first.front(), second.front());
        const TInstant overlapEnd = Min(first.back(), second.back());
        const TString times = TStringBuilder() << "first# " << first.front() << ".." << first.back()
            << " second# " << second.front() << ".." << second.back();
        UNIT_ASSERT_C(overlapBegin + TDuration::Seconds(10) <= overlapEnd, times);
        const size_t numReads = std::ranges::count_if(reads, [&](const auto& read) {
            return overlapBegin <= read.Time && read.Time <= overlapEnd;
        });
        const double rate = (numReads - 1) * blobSize / (overlapEnd - overlapBegin).SecondsFloat();
        UNIT_ASSERT_C(rate <= total * 1.25, "rate# " << rate << " numReads# " << numReads << " " << times);
    }

    Y_UNIT_TEST(RegisteringNodeGetsItsShare) {
        THeldSyncersEnv test;
        auto& env = test.Env;
        ui32 restarted = 0;
        for (const auto& [nodeId, numSyncers] : test.NumNodeSyncers) {
            if (numSyncers) {
                restarted = nodeId;
                break;
            }
        }
        UNIT_ASSERT(restarted);

        // the quoter of the started node knows nothing about the share, so it has to come with the node's first syncers
        std::optional<bool> rateWithFirstSyncers;
        test.OnUpdate = [&](ui32 nodeId, const auto& record) {
            if (nodeId == restarted && record.GetUpdateSyncers() && !rateWithFirstSyncers) {
                rateWithFirstSyncers = record.HasBridgeSyncNodeRateBytesPerSecond();
            }
        };
        env.RestartNode(restarted);
        NBridgeTest::WaitUntil(env, [&] { return rateWithFirstSyncers.has_value(); }, "registration of the node");
        UNIT_ASSERT(*rateWithFirstSyncers);
        env.Sim(TDuration::Seconds(30));
        test.CheckRates(THeldSyncersEnv::TotalRate);
    }

    Y_UNIT_TEST(TotalRateSurvivesControllerRestart) {
        THeldSyncersEnv test;
        auto& env = test.Env;
        const auto ratesBeforeRestart = std::exchange(test.Rates, {}); // the new instance has to send them again
        std::optional<ui32> nodeWithChangedRate;
        test.OnUpdate = [&](ui32 nodeId, const auto& record) {
            if (record.HasBridgeSyncNodeRateBytesPerSecond()) {
                const auto it = ratesBeforeRestart.find(nodeId);
                if (record.GetBridgeSyncNodeRateBytesPerSecond() != (it != ratesBeforeRestart.end() ? it->second : 0)) {
                    nodeWithChangedRate = nodeId;
                }
            }
        };

        env.RebootTablet(env.TabletId);

        const auto settings = NBridgeTest::ReadSettings(env);
        UNIT_ASSERT_VALUES_EQUAL(settings.BridgeSyncTotalRateBytesPerSecondSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(settings.GetBridgeSyncTotalRateBytesPerSecond(0), THeldSyncersEnv::TotalRate);

        // the syncers stay where they were, so a different share would mean counting nodes that haven't reported them yet
        env.Sim(TDuration::Seconds(30));
        UNIT_ASSERT_C(!nodeWithChangedRate, "nodeId# " << *nodeWithChangedRate);
        test.CheckRates(THeldSyncersEnv::TotalRate);
    }

    // A finished syncer makes BSC update its node at once, while the other nodes may have not reported their syncers yet.
    Y_UNIT_TEST(NoShareWhileNodesReportSyncers) {
        constexpr TDuration reportingTime = TDuration::Seconds(10);
        THeldSyncersEnv test;
        auto& env = test.Env;
        std::optional<TInstant> startedAt; // when the first node has heard from the new instance
        bool rateSent = false;
        ui32 numRegistrations = 0;
        ui32 numRegistrationsKeepingShare = 0;
        test.OnUpdate = [&](ui32, const auto& record) {
            if (!startedAt) {
                startedAt = env.Runtime->GetClock();
            }
            rateSent |= record.HasBridgeSyncNodeRateBytesPerSecond();
            if (record.GetComprehensive()) {
                ++numRegistrations;
                numRegistrationsKeepingShare += record.GetKeepBridgeSyncNodeRate();
            }
        };
        env.RebootTablet(env.TabletId);
        NBridgeTest::WaitUntil(env, [&] { return startedAt.has_value(); }, "start of the new instance", TDuration::Seconds(1), 60);

        test.ReleaseHeldSyncers();
        env.Sim(TDuration::Seconds(2));
        UNIT_ASSERT_C(env.Runtime->GetClock() < *startedAt + reportingTime - TDuration::Seconds(1),
            "the test has to finish its check while BSC is waiting for the reports");
        UNIT_ASSERT(!rateSent);
        UNIT_ASSERT_GT(numRegistrations, 0);
        UNIT_ASSERT_VALUES_EQUAL(numRegistrationsKeepingShare, numRegistrations);
        NBridgeTest::WaitForPileState(env, 1, NKikimrBridge::TClusterState::SYNCHRONIZED);
    }

    Y_UNIT_TEST(RegistrationDropsShareWithOldController) {
        TestShareAfterRegistration(false);
    }

    Y_UNIT_TEST(RegistrationKeepsShareWhileControllerIsStarting) {
        TestShareAfterRegistration(true);
    }

    // The share changes while the registration of a node is being committed. The reply must not bring the old one back, as
    // BSC wouldn't send the new one again.
    Y_UNIT_TEST(RegistrationReplyHasCurrentShare) {
        THeldSyncersEnv test;
        auto& env = test.Env;
        ui32 registering = 0;
        for (const auto& [nodeId, numSyncers] : test.NumNodeSyncers) {
            if (numSyncers) {
                registering = nodeId;
                break;
            }
        }
        UNIT_ASSERT(registering);
        const ui64 oldShare = test.Rates.at(registering);

        bool replied = false;
        test.OnUpdate = [&](ui32 nodeId, const auto& record) {
            replied |= nodeId == registering && record.GetComprehensive();
        };
        const TActorId sender = env.Runtime->AllocateEdgeActor(env.Settings.ControllerNodeId, __FILE__, __LINE__);
        std::optional<NKikimrBlobStorage::TConfigResponse> configResponse;
        bool holdCommits = false;
        std::vector<std::pair<ui32, std::unique_ptr<IEventHandle>>> heldCommits;
        auto fixtureFilter = env.Runtime->FilterFunction;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetRecipientRewrite() == sender) { // this edge actor can't wait for anything during the commit
                if (ev->GetTypeRewrite() == TEvBlobStorage::TEvControllerConfigResponse::EventType) {
                    configResponse = ev->Get<TEvBlobStorage::TEvControllerConfigResponse>()->Record.GetResponse();
                }
                return false;
            }
            if (ev->GetTypeRewrite() == TEvBlobStorage::TEvControllerRegisterNode::EventType &&
                    ev->Get<TEvBlobStorage::TEvControllerRegisterNode>()->Record.GetNodeID() == registering) {
                holdCommits = true;
            }
            if (holdCommits && ev->GetTypeRewrite() == TEvBlobStorage::EvPut &&
                    ev->Get<TEvBlobStorage::TEvPut>()->Id.TabletID() == env.TabletId) {
                heldCommits.emplace_back(nodeId, std::move(ev));
                return false;
            }
            return fixtureFilter(nodeId, ev);
        };

        env.Runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(registering), sender,
            new TEvTabletPipe::TEvClientDestroyed(env.TabletId, {}, {})), registering);
        NBridgeTest::WaitUntil(env, [&] { return !heldCommits.empty(); }, "commit of the registration", TDuration::MilliSeconds(100));
        env.Sim(TDuration::Seconds(1));
        UNIT_ASSERT(!replied);

        // the limit gets applied before its commit
        auto request = std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
        request->Record.MutableRequest()->AddCommand()->MutableUpdateSettings()->AddBridgeSyncTotalRateBytesPerSecond(
            THeldSyncersEnv::TotalRate * 2);
        env.Runtime->SendToPipe(env.TabletId, sender, request.release(), 0, TTestActorSystem::GetPipeConfigWithRetries());
        NBridgeTest::WaitUntil(env, [&] { return test.Rates.at(registering) != oldShare; }, "update of the share", TDuration::Seconds(1));
        const ui64 newShare = test.Rates.at(registering);
        UNIT_ASSERT_GT(newShare, oldShare);
        UNIT_ASSERT(!replied);

        holdCommits = false;
        for (auto& [nodeId, ev] : heldCommits) {
            env.Runtime->Send(ev.release(), nodeId);
        }
        NBridgeTest::WaitUntil(env, [&] { return replied && configResponse; }, "replies to the registration and the command",
            TDuration::Seconds(1));
        UNIT_ASSERT_C(configResponse->GetSuccess(), configResponse->GetErrorDescription());
        env.Sim(TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(test.Rates.at(registering), newShare);
    }

    // A share of the previous BSC instance comes in the middle of a sync. The current instance has no reason to send the
    // share again, so the node would stay on the wrong one.
    Y_UNIT_TEST(LateShareFromPreviousController) {
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
        const TActorId sourceProxy = MakeBlobStorageProxyID(GetGroupInfo(env, groupId).GetBridgeGroupState().GetPile(0).GetGroupId());
        const TActorId sender = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        constexpr ui64 share = 100'000;
        ui32 generation = 0;
        bool sent = false;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::TEvControllerNodeServiceSetUpdate::EventType) {
                generation = Max(generation, ev->Get<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>()->Record.GetControllerGeneration());
            } else if (!sent && ev->GetTypeRewrite() == TEvBlobStorage::EvGet && ev->Recipient == sourceProxy &&
                    !ev->Get<TEvBlobStorage::TEvGet>()->IsIndexOnly) {
                sent = true;
                UNIT_ASSERT(generation);
                for (ui32 nodeId : env.Runtime->GetNodes()) {
                    auto update = std::make_unique<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>();
                    update->Record.SetControllerGeneration(generation - 1);
                    update->Record.SetBridgeSyncNodeRateBytesPerSecond(share);
                    env.Runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(nodeId), sender, update.release()), nodeId);
                }
            }
            return true;
        };

        constexpr ui32 numBlobs = 40;
        constexpr ui32 blobSize = 64_KB;
        const auto reads = SyncBlobs(env, groupId, {1}, numBlobs, blobSize);
        env.Runtime->FilterFunction = {};
        UNIT_ASSERT(sent);
        UNIT_ASSERT_GE(reads.size(), numBlobs);
        const double rate = GetReadRate(reads, blobSize);
        UNIT_ASSERT_C(rate >= 10'000'000, "rate# " << rate << " numReads# " << reads.size()); // the default limit is 500 MB/s
    }

    // BSC restarts having persisted a new limit, but not having sent it to the nodes.
    Y_UNIT_TEST(ZeroTotalRateReachesNodesAfterControllerRestart) {
        THeldSyncersEnv test;
        auto& env = test.Env;
        test.DropRates = true;
        SetBridgeSyncTotalRate(env, 0);
        env.Sim(TDuration::Seconds(5));
        test.CheckRates(THeldSyncersEnv::TotalRate);

        test.DropRates = false;
        env.RebootTablet(env.TabletId);
        env.Sim(TDuration::Seconds(30));
        test.CheckRates(0);
    }

    Y_UNIT_TEST(NodeLimitLowerThanShare) {
        TestSyncRate(1'000'000, 100'000'000, 600'000, 1'200'000);
    }

    Y_UNIT_TEST(ShareLowerThanNodeLimit) {
        // the group may share the limit with the static one for a while
        TestSyncRate(100'000'000, 1'000'000, 300'000, 1'200'000);
    }

    Y_UNIT_TEST(ZeroNodeLimitMeansNoLimit) {
        TestSyncRate(0, 1'000'000, 300'000, 1'200'000);
    }

    Y_UNIT_TEST(ZeroTotalRateMeansNoLimit) {
        TestSyncRate(1'000'000, 0, 600'000, 1'200'000);
    }

    Y_UNIT_TEST(ZeroLimitsMeanNoLimit) {
        TestSyncRate(0, 0, 10'000'000, Max<double>());
    }
}

Y_UNIT_TEST_SUITE(BridgeSyncerPlacement) {
    // Each group has VDisks on all 8 nodes of the pile, so 8 syncers of dynamic groups and the one of the static group
    // fit 8 nodes with no more than 2 syncers on a node. Random placement would hardly ever do this.
    Y_UNIT_TEST(SyncersSpreadOverNodes) {
        THeldSyncersEnv test;
        std::map<ui32, ui32> numSyncers;
        for (const auto& [nodeId, syncers] : test.NumNodeSyncers) {
            if (syncers) {
                numSyncers.emplace(nodeId, syncers);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(numSyncers.size(), 8);
        for (const auto& [nodeId, syncers] : numSyncers) {
            UNIT_ASSERT_LE_C(syncers, 2, "nodeId# " << nodeId);
        }
    }
}

Y_UNIT_TEST_SUITE(BridgeSyncerLimit) {
    // Pile 1 gets back with the completions of the syncers of dynamic groups held.
    struct TLimitedSyncersEnv {
        static constexpr ui32 NumGroups = 8;

        TEnvironmentSetup Env{{
            .NodeCount = 16,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = NBridgeTest::GetBridgedLocation,
            .SelfManagementConfig = true,
            .NumPiles = 2,
            .AutomaticBootstrap = true,
        }};
        NBridgeTest::TSyncerTracker Tracker;

        // the number of syncers expected to run in the end is the limit, or all the groups if there is none
        explicit TLimitedSyncersEnv(std::optional<ui32> maxSyncers) {
            NBridgeTest::CreatePoolAndGetGroup(Env, NumGroups);
            Env.Runtime->FilterFunction = [this](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
                return Tracker.OnEvent(nodeId, ev);
            };
            if (maxSyncers) {
                NBridgeTest::SetBridgeSyncMaxSyncers(Env, *maxSyncers);
            }
            Env.Sim(TDuration::Seconds(5));
            NBridgeTest::SwitchBridgePileState(Env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
            NBridgeTest::WaitForPileState(Env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
            Tracker.Hold = true;
            NBridgeTest::SwitchBridgePileState(Env, 1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
            const size_t expected = maxSyncers && *maxSyncers ? Min<size_t>(*maxSyncers, NumGroups) : NumGroups;
            NBridgeTest::WaitUntil(Env, [&] { return Tracker.GetNumRunning() >= expected; }, "syncers running");
            Env.Sim(TDuration::Seconds(5)); // the static group gets synced by now, so it takes no slots
            UNIT_ASSERT_VALUES_EQUAL(Tracker.GetNumRunning(), expected);
        }

        ~TLimitedSyncersEnv() {
            Env.Runtime->FilterFunction = {};
        }

        ui32 ReadLimit() {
            const auto settings = NBridgeTest::ReadSettings(Env);
            UNIT_ASSERT_VALUES_EQUAL(settings.BridgeSyncMaxSyncersSize(), 1);
            return settings.GetBridgeSyncMaxSyncers(0);
        }
    };

    Y_UNIT_TEST(RejoiningPileIsLimited) {
        TLimitedSyncersEnv test(2);
        test.Tracker.RunUntil(test.Env, 2, "pile synchronized", [&] {
            return NBridgeTest::GetPileState(test.Env, 1) == NKikimrBridge::TClusterState::SYNCHRONIZED;
        });
    }

    // A group that has finished a stage goes behind the waiting ones, so all of them soon get writes up to the last stage.
    Y_UNIT_TEST(GroupsTakeTurnsBetweenStages) {
        TLimitedSyncersEnv test(1);
        const ui32 numGroups = TLimitedSyncersEnv::NumGroups;
        test.Tracker.RunUntil(test.Env, 1, "syncers of all groups started", [&] { return test.Tracker.Started.size() >= numGroups; });
        std::set<ui32> targets;
        for (ui32 i = 0; i < numGroups; ++i) {
            targets.insert(test.Tracker.Started[i].second);
        }
        UNIT_ASSERT_VALUES_EQUAL(targets.size(), numGroups);
    }

    Y_UNIT_TEST(RaisedLimitStartsWaitingSyncers) {
        TLimitedSyncersEnv test(2);
        NBridgeTest::SetBridgeSyncMaxSyncers(test.Env, 5);
        test.Env.Sim(TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(test.Tracker.GetNumRunning(), 5);
    }

    // Stopping a syncer would throw away its progress, so the number of them just comes down to the new limit.
    Y_UNIT_TEST(LoweredLimitKeepsRunningSyncers) {
        TLimitedSyncersEnv test(4);
        NBridgeTest::SetBridgeSyncMaxSyncers(test.Env, 1);
        test.Env.Sim(TDuration::Seconds(5));
        for (ui32 expected : {4, 3, 2, 1, 1}) {
            UNIT_ASSERT_VALUES_EQUAL(test.Tracker.GetNumRunning(), expected);
            test.Tracker.ReleaseOne(test.Env);
            test.Env.Sim(TDuration::Seconds(5));
        }
    }

    Y_UNIT_TEST(ZeroMeansNoLimit) {
        TLimitedSyncersEnv test(0);
        UNIT_ASSERT_VALUES_EQUAL(test.ReadLimit(), 0);
    }

    Y_UNIT_TEST(DefaultLimit) {
        TLimitedSyncersEnv test(std::nullopt);
        UNIT_ASSERT_VALUES_EQUAL(test.ReadLimit(), 64);
    }

    Y_UNIT_TEST(LimitSurvivesControllerRestart) {
        TLimitedSyncersEnv test(3);
        test.Env.RebootTablet(test.Env.TabletId);
        UNIT_ASSERT_VALUES_EQUAL(test.ReadLimit(), 3);
        test.Env.Sim(TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(test.Tracker.GetNumRunning(), 3);
    }

    // The groups need the static one, so its syncer doesn't wait for the slots. It comes to that when another pile gets
    // back while the syncers of the first one take all the slots: alone, a pile gets its static part synced before them.
    Y_UNIT_TEST(StaticGroupDoesNotWaitForSlot) {
        constexpr ui32 pileIndex = 2;
        constexpr ui32 firstNodeOfPile = 17;
        NBridgeTest::TSyncerTracker tracker;
        bool staticSyncerStarted = false;
        TEnvironmentSetup env{{
            .NodeCount = 24,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .SelfManagementConfig = true,
            .NumPiles = 3,
            .AutomaticBootstrap = true,
        }};
        NBridgeTest::CreatePoolAndGetGroup(env, 4);
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvControllerNodeServiceSetUpdate && nodeId >= firstNodeOfPile) {
                for (const auto& syncer : ev->Get<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>()->Record.GetSyncers()) {
                    staticSyncerStarted |= TGroupID(syncer.GetBridgeProxyGroupId()).ConfigurationType() ==
                        EGroupConfigurationType::Static;
                }
            }
            return tracker.OnEvent(nodeId, ev);
        };
        NBridgeTest::SetBridgeSyncMaxSyncers(env, 1);
        env.Sim(TDuration::Seconds(5));
        for (ui32 pile : {1, 2}) {
            NBridgeTest::SwitchBridgePileState(env, pile, NKikimrBridge::TClusterState::DISCONNECTED);
            NBridgeTest::WaitForPileState(env, pile, NKikimrBridge::TClusterState::DISCONNECTED);
        }

        tracker.Hold = true;
        NBridgeTest::SwitchBridgePileState(env, 1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        NBridgeTest::WaitUntil(env, [&] { return tracker.GetNumRunning() >= 1; }, "syncer running");
        env.Sim(TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetNumRunning(), 1);
        const size_t numStarted = tracker.Started.size();
        UNIT_ASSERT(!staticSyncerStarted);

        NBridgeTest::SwitchBridgePileState(env, pileIndex, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        NBridgeTest::WaitUntil(env, [&] {
            if (!staticSyncerStarted) {
                return false;
            }
            const auto config = NBridgeTest::QueryStorageConfig(env);
            for (const auto& group : config.GetBlobStorageConfig().GetServiceSet().GetGroups()) {
                if (group.HasBridgeGroupState()) {
                    const auto *pile = NBridge::GetGroupPile(group.GetBridgeGroupState(), TBridgePileId::FromPileIndex(pileIndex));
                    if (!pile || pile->GetStage() != NKikimrBridge::TGroupState::SYNCED) {
                        return false;
                    }
                }
            }
            return true;
        }, "static group synced");
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetNumRunning(), 1);
        UNIT_ASSERT_VALUES_EQUAL(tracker.Started.size(), numStarted);
        env.Runtime->FilterFunction = {};
    }

    // A finished syncer makes BSC go through the queue at once, while the running syncer of another node is not reported
    // yet: starting new ones would exceed the limit when it gets reported.
    Y_UNIT_TEST(NoSyncersStartWhileNodesReport) {
        constexpr TDuration reportingTime = TDuration::Seconds(10);
        TLimitedSyncersEnv test(2);
        auto& env = test.Env;
        UNIT_ASSERT_VALUES_EQUAL(test.Tracker.Held.size(), 2);
        const ui32 finishingNode = test.Tracker.Held.front().first;
        const ui32 silentNode = test.Tracker.Held.back().first;
        UNIT_ASSERT_UNEQUAL(finishingNode, silentNode);

        std::optional<TInstant> startedAt; // when the first node has heard from the new instance
        std::unique_ptr<IEventHandle> registration;
        ui32 registrationNodeId = 0;
        bool holdRegistration = true;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvControllerNodeServiceSetUpdate && !startedAt) {
                startedAt = env.Runtime->GetClock();
            }
            if (holdRegistration && ev->GetTypeRewrite() == TEvBlobStorage::EvControllerRegisterNode &&
                    ev->Get<TEvBlobStorage::TEvControllerRegisterNode>()->Record.GetNodeID() == silentNode) {
                registration = std::move(ev);
                registrationNodeId = nodeId;
                return false;
            }
            return test.Tracker.OnEvent(nodeId, ev);
        };
        env.RebootTablet(env.TabletId);
        NBridgeTest::WaitUntil(env, [&] { return startedAt.has_value(); }, "start of the new instance", TDuration::Seconds(1), 60);
        UNIT_ASSERT(registration);

        const size_t numStarted = test.Tracker.Started.size();
        test.Tracker.ReleaseOne(env);
        env.Sim(TDuration::Seconds(2));
        UNIT_ASSERT_C(env.Runtime->GetClock() < *startedAt + reportingTime - TDuration::Seconds(1),
            "the test has to finish its check while BSC is waiting for the reports");
        UNIT_ASSERT_VALUES_EQUAL(test.Tracker.Started.size(), numStarted);

        env.Sim(reportingTime);
        holdRegistration = false;
        env.Runtime->Send(registration.release(), registrationNodeId);
        test.Tracker.Hold = false;
        while (!test.Tracker.Held.empty()) {
            test.Tracker.ReleaseOne(env);
        }
        NBridgeTest::WaitForPileState(env, 1, NKikimrBridge::TClusterState::SYNCHRONIZED);
    }

    // The parts of a pile going out and getting back during its sync get reset and their syncers get restarted. They keep
    // their slots meanwhile, and no slot is left taken in the end.
    Y_UNIT_TEST(PileGoingOutAndBackDuringSync) {
        TLimitedSyncersEnv test(2);
        auto& env = test.Env;
        test.Tracker.RunUntil(env, 2, "sync going on", [&] { return test.Tracker.Started.size() >= 4; });

        NBridgeTest::SwitchBridgePileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        NBridgeTest::WaitForPileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        env.Sim(TDuration::Seconds(5));
        UNIT_ASSERT_LE(test.Tracker.GetNumRunning(), 2);

        NBridgeTest::SwitchBridgePileState(env, 1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        test.Tracker.RunUntil(env, 2, "pile synchronized", [&] {
            return NBridgeTest::GetPileState(env, 1) == NKikimrBridge::TClusterState::SYNCHRONIZED;
        });
        env.Sim(TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(test.Tracker.GetNumRunning(), 0);
    }

    // The syncers of a lost node give their slots up, so that the waiting ones start, and the sync goes on without the node.
    Y_UNIT_TEST(LostNodeGivesSlotsUp) {
        TLimitedSyncersEnv test(2);
        auto& env = test.Env;
        auto& tracker = test.Tracker;
        const ui32 lostNode = tracker.Started.front().first;
        const size_t numStarted = tracker.Started.size();

        env.StopNode(lostNode);
        tracker.NodeSyncers.erase(lostNode); // BSC can't tell it to the node
        std::erase_if(tracker.Held, [&](const auto& held) { return held.first == lostNode; });

        NBridgeTest::WaitUntil(env, [&] { return tracker.Started.size() > numStarted; }, "syncer started instead of the lost one",
            TDuration::Seconds(1), 30);
        UNIT_ASSERT_UNEQUAL(tracker.Started.back().first, lostNode);
        UNIT_ASSERT_LE(tracker.GetNumRunning(), 2);

        tracker.RunUntil(env, 2, "pile synchronized", [&] {
            return NBridgeTest::GetPileState(env, 1) == NKikimrBridge::TClusterState::SYNCHRONIZED;
        });
    }

    Y_UNIT_TEST(LimitFromBscSettings) {
        constexpr ui32 limit = 5;
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .ConfigPreprocessor = [](ui32, TNodeWardenConfig& conf) {
                conf.BlobStorageConfig->MutableBscSettings()->SetBridgeSyncMaxSyncers(limit);
            },
        }};
        env.Sim(TDuration::Seconds(5));
        const auto settings = NBridgeTest::ReadSettings(env);
        UNIT_ASSERT_VALUES_EQUAL(settings.BridgeSyncMaxSyncersSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(settings.GetBridgeSyncMaxSyncers(0), limit);
    }
}

Y_UNIT_TEST_SUITE(BridgeSyncerGeneration) {
    Y_UNIT_TEST_TWIN(InvalidSourceAssignmentFails, missingSource) {
        using namespace NBridgeTest;
        TEnvironmentSetup env{{
            .NodeCount = 16,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .SelfManagementConfig = true,
            .NumPiles = 2,
            .AutomaticBootstrap = true,
        }};
        auto group = GetGroupInfo(env, CreatePoolAndGetGroup(env));
        if (!missingSource) {
            group.MutableBridgeGroupState()->MutablePile(0)->SetStage(NKikimrBridge::TGroupState::BLOCKS);
        }
        const auto info = TBlobStorageGroupInfo::Parse(group, nullptr, nullptr);
        UNIT_ASSERT(info);
        const auto& state = group.GetBridgeGroupState();
        const TGroupId source = TGroupId::FromValue(missingSource ? 0 : state.GetPile(0).GetGroupId());
        const TGroupId target = TGroupId::FromValue(state.GetPile(1).GetGroupId());
        TActorId syncer;
        std::unique_ptr<IEventHandle> completion;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->Sender == syncer && ev->GetTypeRewrite() == NStorage::TEvNodeWardenNotifySyncerFinished::EventType) {
                completion = std::move(ev);
                return false;
            }
            return true;
        };
        syncer = env.Runtime->Register(NBridge::CreateSyncerActor(info, source, target,
            std::make_shared<NBridge::TSyncerDataStats>(), nullptr), 1);
        WaitUntil(env, [&] { return bool(completion); }, "invalid assignment completion");
        env.Runtime->FilterFunction = {};
        const auto& error = completion->Get<NStorage::TEvNodeWardenNotifySyncerFinished>()->ErrorReason;
        UNIT_ASSERT(error);
        UNIT_ASSERT_STRING_CONTAINS(*error, missingSource ? "source group not found" : "group is not synced");
    }

    Y_UNIT_TEST(LateCompletionWhileWaitingForNewConfiguration) {
        TestLateSyncerCompletion(ECompletionRace::NewGeneration);
    }

    Y_UNIT_TEST(LateCompletionFromReplacedActor) {
        TestLateSyncerCompletion(ECompletionRace::ReplacedActor);
    }

    Y_UNIT_TEST(RegistrationPreservesCompletedGeneration) {
        TestLateSyncerCompletion(ECompletionRace::ReregisterAfterCompletion);
    }

    // A stage change of one part bumps the generation of the whole group, and a syncer of another part can do nothing with
    // the old one. BSC has to tell its node without waiting for a report, which the syncer may have nothing to fill with.
    Y_UNIT_TEST(RunningSyncerLearnsNewGenerationAtOnce) {
        using namespace NBridgeTest;
        TEnvironmentSetup env{{
            .NodeCount = 24,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .SelfManagementConfig = true,
            .NumPiles = 3,
            .AutomaticBootstrap = true,
        }};
        const ui32 groupId = CreatePoolAndGetGroup(env);
        const ui32 heldTarget = GetGroupInfo(env, groupId).GetBridgeGroupState().GetPile(2).GetGroupId();
        for (ui32 pile : {1, 2}) {
            SwitchBridgePileState(env, pile, NKikimrBridge::TClusterState::DISCONNECTED);
            WaitForPileState(env, pile, NKikimrBridge::TClusterState::DISCONNECTED);
        }
        env.Sim(TDuration::Seconds(10));

        // Syncers of the part in pile 2 never finish, and nodes of this pile don't report anything to BSC.
        constexpr ui32 firstNodeOfPile2 = 17;
        bool holding = true;
        ui32 firstAssigned = 0;
        ui32 lastFinished = 0;
        std::vector<std::pair<ui32, std::unique_ptr<IEventHandle>>> held;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (!holding) {
                return true;
            }
            if (ev->GetTypeRewrite() == TEvBlobStorage::TEvControllerNodeServiceSetUpdate::EventType) {
                for (const auto& syncer : ev->Get<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>()->Record.GetSyncers()) {
                    if (syncer.GetTargetGroupId() == heldTarget && !firstAssigned) {
                        firstAssigned = syncer.GetBridgeProxyGroupGeneration();
                    }
                }
            } else if (ev->GetTypeRewrite() == TEvBlobStorage::TEvControllerUpdateSyncerState::EventType &&
                    ev->Sender.NodeId() >= firstNodeOfPile2) {
                return false;
            } else if (ev->GetTypeRewrite() == NStorage::TEvNodeWardenNotifySyncerFinished::EventType) {
                const auto *msg = ev->Get<NStorage::TEvNodeWardenNotifySyncerFinished>();
                if (msg->TargetGroupId.GetRawId() == heldTarget) {
                    lastFinished = msg->BridgeProxyGroupGeneration;
                    held.emplace_back(nodeId, std::move(ev));
                    return false;
                }
            }
            return true;
        };
        for (ui32 pile : {1, 2}) {
            SwitchBridgePileState(env, pile, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        }
        WaitUntil(env, [&] { return firstAssigned && lastFinished > firstAssigned; }, "syncer running with new generation",
            TDuration::Seconds(1), 120);

        holding = false;
        env.Runtime->FilterFunction = {};
        for (auto& [nodeId, ev] : held) {
            env.Runtime->Send(ev.release(), nodeId);
        }
        const TActorId sender = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        for (ui32 nodeId = firstNodeOfPile2; nodeId <= 24; ++nodeId) { // the nodes repeat what they couldn't report
            env.Runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(nodeId), sender,
                new TEvTabletPipe::TEvClientDestroyed(env.TabletId, {}, {})), nodeId);
        }
        for (ui32 pile : {1, 2}) {
            WaitForPileState(env, pile, NKikimrBridge::TClusterState::SYNCHRONIZED);
        }
    }

    // A list of syncers of the previous BSC instance comes when the node runs the syncers assigned by the current one.
    // The node would stop them and ignore their completions, while BSC would wait for those completions.
    Y_UNIT_TEST(LateSyncersFromPreviousController) {
        using namespace NBridgeTest;
        THeldSyncersEnv test;
        auto& env = test.Env;
        std::set<ui32> hostingNodes;
        for (const auto& [nodeId, numSyncers] : test.NumNodeSyncers) {
            if (numSyncers) {
                hostingNodes.insert(nodeId);
            }
        }
        std::set<ui32> registeredNodes;
        ui32 generation = 0;
        test.OnUpdate = [&](ui32 nodeId, const auto& record) {
            if (record.GetComprehensive()) { // only the new instance sends these from now on
                registeredNodes.insert(nodeId);
                generation = record.GetControllerGeneration();
            }
        };
        env.RebootTablet(env.TabletId);
        WaitUntil(env, [&] { return std::ranges::includes(registeredNodes, hostingNodes); }, "registration at the new instance",
            TDuration::Seconds(1));
        test.OnUpdate = {};
        UNIT_ASSERT(generation);

        const TActorId sender = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        for (ui32 nodeId : hostingNodes) {
            auto update = std::make_unique<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>();
            update->Record.SetControllerGeneration(generation - 1);
            update->Record.SetUpdateSyncers(true);
            env.Runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(nodeId), sender, update.release()), nodeId);
        }
        env.Sim(TDuration::Seconds(1));

        test.ReleaseHeldSyncers();
        WaitForPileState(env, 1, NKikimrBridge::TClusterState::SYNCHRONIZED);
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

    // BSC doesn't check piles to sync while it notifies distconf of a synced pile, and the check requested meanwhile (here
    // by the other pile coming back) must run once the notification is answered: it is the check that resets the parts
    // of the pile coming back, and nothing else makes it run on a quiet cluster
    Y_UNIT_TEST(RecheckAfterSyncNotification) {
        using namespace NBridgeTest;

        TEnvironmentSetup env{{
            .NodeCount = 24,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .SelfManagementConfig = true,
            .NumPiles = 3,
            .AutomaticBootstrap = true,
        }};
        const ui32 groupId = CreatePoolAndGetGroup(env);
        TDataChecker checker(env);
        checker.Write(groupId);
        for (ui32 pile : {1, 2}) {
            SwitchPileState(env, pile, NKikimrBridge::TClusterState::DISCONNECTED);
        }
        env.Sim(TDuration::Seconds(10));
        checker.Write(groupId);

        auto isBsc = [&](const TActorId& actorId) {
            IActor *actor = env.Runtime->GetActor(actorId);
            return actor && TypeName(*actor).Contains("TBlobStorageController");
        };
        bool holding = true;
        THashSet<ui64> notifyCookies;
        std::vector<std::tuple<ui32, std::unique_ptr<IEventHandle>>> held;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NStorage::TEvNodeConfigInvokeOnRoot::EventType && isBsc(ev->Sender)) {
                const auto& record = ev->Get<NStorage::TEvNodeConfigInvokeOnRoot>()->Record;
                if (record.HasNotifyBridgeSyncFinished()) {
                    const auto& cmd = record.GetNotifyBridgeSyncFinished();
                    using T = std::decay_t<decltype(cmd)>;
                    if (cmd.GetBSC() && TBridgePileId::FromProto(&cmd, &T::GetBridgePileId) ==
                            TBridgePileId::FromPileIndex(1)) {
                        notifyCookies.insert(ev->Cookie);
                    }
                }
            } else if (holding && ev->GetTypeRewrite() == NStorage::TEvNodeConfigInvokeOnRootResult::EventType &&
                    notifyCookies.contains(ev->Cookie) && isBsc(ev->GetRecipientRewrite())) {
                held.emplace_back(nodeId, std::move(ev));
                return false;
            }
            return true;
        };

        SwitchPileState(env, 1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        WaitUntil(env, [&] { return !held.empty(); }, "BSC notification of pile 1 synced", TDuration::Seconds(1), 300);

        // pile 2 comes back while BSC waits for the answer to the notification
        SwitchPileState(env, 2, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        WaitForPileState(env, 2, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_2);
        env.Sim(TDuration::Seconds(10));

        holding = false;
        for (auto& [nodeId, ev] : std::exchange(held, {})) {
            env.Runtime->Send(ev.release(), nodeId);
        }
        env.Runtime->FilterFunction = {};
        WaitForPileState(env, 1, NKikimrBridge::TClusterState::SYNCHRONIZED);
        WaitForPileState(env, 2, NKikimrBridge::TClusterState::SYNCHRONIZED);
        checker.CheckAll();
        CheckPileData(env, checker, 2, 17);
    }
}
