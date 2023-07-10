#include "kqp_rm_service.h"

#include <ydb/core/base/location.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/mind/tenant_pool.h>
#include <ydb/core/kqp/common/kqp_event_ids.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/interconnect/interconnect.h>

#include <ydb/core/util/ulid.h>

namespace NKikimr {
namespace NKqp {
namespace NRm {

#define LOG_C(stream) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::KQP_RESOURCE_MANAGER, stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_RESOURCE_MANAGER, stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::KQP_RESOURCE_MANAGER, stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_RESOURCE_MANAGER, stream)
#define LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::KQP_RESOURCE_MANAGER, stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::KQP_RESOURCE_MANAGER, stream)

class TKqpResourceInfoExchangerActor : public TActorBootstrapped<TKqpResourceInfoExchangerActor> {
    using TBase = TActorBootstrapped<TKqpResourceInfoExchangerActor>;

    struct TEvPrivate {
        enum EEv {
            EvRetrySending = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvRegularSending,
            EvRetrySubscriber,
            EvUpdateSnapshotState,
        };

        struct TEvRetrySending :
                public TEventLocal<TEvRetrySending, EEv::EvRetrySending> {
            ui32 NodeId;

            TEvRetrySending(ui32 nodeId) : NodeId(nodeId) {
            }
        };

        struct TEvRegularSending :
                public TEventLocal<TEvRegularSending, EEv::EvRegularSending> {
        };

        struct TEvRetrySubscriber :
                public TEventLocal<TEvRetrySubscriber, EEv::EvRetrySubscriber> {
        };

        struct TEvUpdateSnapshotState:
                public TEventLocal<TEvUpdateSnapshotState, EEv::EvUpdateSnapshotState> {
        };
    };

    struct TRetryState {
        bool IsScheduled = false;
        NMonotonic::TMonotonic LastRetryAt = TMonotonic::Zero();
        TDuration CurrentDelay = TDuration::MilliSeconds(50);
    };

    struct TNodeState {
        TRetryState RetryState;
        NMonotonic::TMonotonic LastUpdateAt = TMonotonic::Zero();
        NKikimrKqp::TResourceExchangeNodeData NodeData;
    };

public:

    TKqpResourceInfoExchangerActor(TIntrusivePtr<TKqpCounters> counters,
        std::shared_ptr<TResourceSnapshotState> resourceSnapshotState)
        : ResourceSnapshotState(std::move(resourceSnapshotState))
        , Counters(counters)
    {
        Y_UNUSED(counters);
    }

    void Bootstrap() {
        LOG_D("Start KqpResourceInfoExchangerActor at " << SelfId());

        Become(&TKqpResourceInfoExchangerActor::WorkState);

        Send(MakeTenantPoolRootID(), new TEvents::TEvSubscribe);
    }

private:

    static TString MakeKqpInfoExchangerBoardPath(TStringBuf database) {
        return TStringBuilder() << "kqpexch+" << database;
    }

    void CreateSubscriber() {
        auto& retryState = BoardState.RetryStateForSubscriber;

        auto now = TlsActivationContext->Monotonic();
        if (now - retryState.LastRetryAt < retryState.CurrentDelay) {
            auto at = retryState.LastRetryAt + GetRetryDelay(retryState);
            Schedule(at - now, new TEvPrivate::TEvRetrySubscriber);
            return;
        }

        if (BoardState.Subscriber) {
            LOG_I("Kill previous info exchanger subscriber for '" << BoardState.Path
                << "' at " << BoardState.Subscriber << ", reason: tenant updated");
            Send(BoardState.Subscriber, new TEvents::TEvPoison);
        }
        BoardState.Subscriber = TActorId();

        auto subscriber = CreateBoardLookupActor(
            BoardState.Path, SelfId(), BoardState.StateStorageGroupId, EBoardLookupMode::Subscription);
        BoardState.Subscriber = Register(subscriber);

        retryState.LastRetryAt = now;
    }

    void CreatePublisher() {
        if (BoardState.Publisher) {
            LOG_I("Kill previous info exchanger publisher for '" << BoardState.Path
                << "' at " << BoardState.Publisher << ", reason: tenant updated");
            Send(BoardState.Publisher, new TEvents::TEvPoison);
        }
        BoardState.Publisher = TActorId();

        auto publisher = CreateBoardPublishActor(BoardState.Path, SelfInfo, SelfId(),
            BoardState.StateStorageGroupId, /* ttlMs */ 0, /* reg */ true);
        BoardState.Publisher = Register(publisher);

        auto& nodeState = NodesState[SelfId().NodeId()];
        auto& info = nodeState.NodeData;
        auto* boardData = info.MutableResourceExchangeBoardData();
        ActorIdToProto(BoardState.Publisher, boardData->MutablePublisher());
    }

    TVector<ui32> UpdateBoardInfo(const TMap<TActorId, TEvStateStorage::TBoardInfoEntry>& infos) {
        auto nodeIds = TVector<ui32>();

        auto now = TlsActivationContext->Monotonic();
        for (const auto& [id, entry] : infos) {
            if (entry.Dropped) {
                auto nodesStateIt = NodesState.find(id.NodeId());
                if (nodesStateIt == NodesState.end()) {
                    continue;
                }
                auto& currentBoardData = nodesStateIt->second.NodeData.GetResourceExchangeBoardData();
                auto currentPublisher = ActorIdFromProto(currentBoardData.GetPublisher());
                if (currentPublisher == id) {
                    NodesState.erase(nodesStateIt);
                }
                continue;
            }

            NKikimrKqp::TResourceExchangeBoardData boardData;
            Y_PROTOBUF_SUPPRESS_NODISCARD boardData.ParseFromString(entry.Payload);
            auto owner = ActorIdFromProto(boardData.GetOwner());
            auto ulidString = boardData.GetUlid();

            auto& nodeState = NodesState[owner.NodeId()];
            auto& currentInfo = nodeState.NodeData;
            auto* currentBoardData = currentInfo.MutableResourceExchangeBoardData();
            auto currentUlidString = currentBoardData->GetUlid();

            if (currentUlidString.empty()) {
                *currentBoardData = std::move(boardData);
                ActorIdToProto(id, currentBoardData->MutablePublisher());
                nodeIds.push_back(owner.NodeId());
                nodeState.LastUpdateAt = now;
                continue;
            }

            auto currentUlid = TULID::FromBinary(currentUlidString);
            auto ulid = TULID::FromBinary(ulidString);

            if (currentUlid < ulid) {
                *currentBoardData = std::move(boardData);
                ActorIdToProto(id, currentBoardData->MutablePublisher());
                currentInfo.SetRound(0);
                (*currentInfo.MutableResources()) = NKikimrKqp::TKqpNodeResources();
                nodeIds.push_back(owner.NodeId());
                nodeState.LastUpdateAt = now;
                continue;
            }
        }

        return nodeIds;
    }

    void UpdateResourceInfo(const TVector<NKikimrKqp::TResourceExchangeNodeData>& infos) {
        auto now = TlsActivationContext->Monotonic();

        for (const auto& info : infos) {
            const auto& boardData = info.GetResourceExchangeBoardData();
            auto owner = ActorIdFromProto(boardData.GetOwner());

            auto nodesStateIt = NodesState.find(owner.NodeId());
            if (nodesStateIt == NodesState.end()) {
                continue;
            }

            auto& nodeState = nodesStateIt->second;
            auto& currentInfo = nodeState.NodeData;
            auto* currentBoardData = currentInfo.MutableResourceExchangeBoardData();
            auto currentUlidString = (*currentBoardData).GetUlid();

            auto round = info.GetRound();
            auto ulidString = boardData.GetUlid();
            auto ulid = TULID::FromBinary(boardData.GetUlid());

            auto currentUlid = TULID::FromBinary(currentUlidString);
            auto currentRound = currentInfo.GetRound();

            if (currentUlid < ulid || (currentUlid == ulid && currentRound < round)) {
                currentInfo = info;
                auto latency = now - nodeState.LastUpdateAt;
                Counters->RmSnapshotLatency->Collect(latency.MilliSeconds());
                nodeState.LastUpdateAt = now;
                continue;
            }
        }
    }

    void UpdateResourceSnapshotState() {
        if (ResourceSnapshotRetryState.IsScheduled) {
            return;
        }
        auto now = TlsActivationContext->Monotonic();
        if (now - ResourceSnapshotRetryState.LastRetryAt < ResourceSnapshotRetryState.CurrentDelay) {
            auto at = ResourceSnapshotRetryState.LastRetryAt + GetRetryDelay(ResourceSnapshotRetryState);
            ResourceSnapshotRetryState.IsScheduled = true;
            Schedule(at - now, new TEvPrivate::TEvUpdateSnapshotState);
            return;
        }
        TVector<NKikimrKqp::TKqpNodeResources> resources;
        resources.reserve(NodesState.size());

        for (const auto& [id, state] : NodesState) {
            auto currentResources = state.NodeData.GetResources();
            if (currentResources.GetNodeId()) {
                resources.push_back(currentResources);
            }
        }

        with_lock (ResourceSnapshotState->Lock) {
            ResourceSnapshotState->Snapshot =
                std::make_shared<TVector<NKikimrKqp::TKqpNodeResources>>(std::move(resources));
        }

        ResourceSnapshotRetryState.LastRetryAt = now;
    }

    void SendInfos(TVector<ui32> infoNodeIds, TVector<ui32> nodeIds = {}) {
        auto& nodeState = NodesState[SelfId().NodeId()];
        nodeState.NodeData.SetRound(Round++);

        if (!nodeIds.empty()) {
            auto snapshotMsg = CreateSnapshotMessage(infoNodeIds, true);

            for (const auto& nodeId : nodeIds) {
                auto nodesStateIt = NodesState.find(nodeId);

                if (nodesStateIt == NodesState.end()) {
                    return;
                }
                SendingToNode(nodesStateIt->second, true, {}, snapshotMsg);
            }
        } else {
            auto snapshotMsg = CreateSnapshotMessage(infoNodeIds, false);

            TDuration currentLatency = TDuration::MilliSeconds(0);
            auto nowMonotic = TlsActivationContext->Monotonic();

            for (auto& [id, state] : NodesState) {
                SendingToNode(state, true, {}, snapshotMsg);
                if (id != SelfId().NodeId()) {
                    currentLatency = Max(currentLatency, nowMonotic - state.LastUpdateAt);
                }
            }

            Counters->RmMaxSnapshotLatency->Set(currentLatency.MilliSeconds());
        }
    }

private:

    void Handle(TEvTenantPool::TEvTenantPoolStatus::TPtr& ev) {
        TString tenant;
        for (auto &slot : ev->Get()->Record.GetSlots()) {
            if (slot.HasAssignedTenant()) {
                if (tenant.empty()) {
                    tenant = slot.GetAssignedTenant();
                } else {
                    LOG_E("Multiple tenants are served by the node: " << ev->Get()->Record.ShortDebugString());
                }
            }
        }

        BoardState.Tenant = tenant;
        BoardState.Path = MakeKqpInfoExchangerBoardPath(tenant);

        if (auto* domainInfo = AppData()->DomainsInfo->GetDomainByName(ExtractDomain(tenant))) {
            BoardState.StateStorageGroupId = domainInfo->DefaultStateStorageGroup;
        } else {
            BoardState.StateStorageGroupId = std::numeric_limits<ui32>::max();
        }

        if (BoardState.StateStorageGroupId == std::numeric_limits<ui32>::max()) {
            LOG_E("Can not find default state storage group for database " << BoardState.Tenant);
            return;
        }

        TULIDGenerator ulidGen;
        auto now = TInstant::Now();
        Ulid = ulidGen.Next(now);
        UlidString = Ulid.ToBinary();

        NKikimrKqp::TResourceExchangeBoardData payload;
        ActorIdToProto(SelfId(), payload.MutableOwner());
        payload.SetUlid(UlidString);

        auto nowMonotic = TlsActivationContext->Monotonic();

        auto& nodeState = NodesState[SelfId().NodeId()];
        nodeState.LastUpdateAt = nowMonotic;

        (*nodeState.NodeData.MutableResourceExchangeBoardData()) = payload;

        SelfInfo = payload.SerializeAsString();

        CreatePublisher();
        CreateSubscriber();

        LOG_I("Received tenant pool status for exchanger, serving tenant: " << BoardState.Tenant
            << ", board: " << BoardState.Path
            << ", ssGroupId: " << BoardState.StateStorageGroupId);

        Schedule(TDuration::Seconds(2), new TEvPrivate::TEvRegularSending);
    }


    void Handle(TEvStateStorage::TEvBoardInfo::TPtr& ev) {
        if (ev->Get()->Status == TEvStateStorage::TEvBoardInfo::EStatus::NotAvailable) {
            LOG_I("Subcriber is not available for info exchanger, serving tenant: " << BoardState.Tenant
                << ", board: " << BoardState.Path
                << ", ssGroupId: " << BoardState.StateStorageGroupId);
            CreateSubscriber();
            return;
        }

        LOG_D("Get board info from subscriber, serving tenant: " << BoardState.Tenant
                << ", board: " << BoardState.Path
                << ", ssGroupId: " << BoardState.StateStorageGroupId
                << ", with size: " << ev->Get()->InfoEntries.size());

        auto nodeIds = UpdateBoardInfo(ev->Get()->InfoEntries);

        SendInfos({SelfId().NodeId()}, std::move(nodeIds));

        UpdateResourceSnapshotState();
    }

    void Handle(TEvStateStorage::TEvBoardInfoUpdate::TPtr& ev) {
        if (ev->Get()->Status == TEvStateStorage::TEvBoardInfo::EStatus::NotAvailable) {
            LOG_I("Subcriber is not available for info exchanger, serving tenant: " << BoardState.Tenant
                << ", board: " << BoardState.Path
                << ", ssGroupId: " << BoardState.StateStorageGroupId);
            CreateSubscriber();
            return;
        }
        LOG_D("Get board info update from subscriber, serving tenant: " << BoardState.Tenant
                << ", board: " << BoardState.Path
                << ", ssGroupId: " << BoardState.StateStorageGroupId
                << ", with size: " << ev->Get()->Updates.size());

        auto nodeIds = UpdateBoardInfo(ev->Get()->Updates);

        SendInfos({SelfId().NodeId()}, std::move(nodeIds));

        UpdateResourceSnapshotState();
    }

    void Handle(TEvKqpResourceInfoExchanger::TEvPublishResource::TPtr& ev) {
        auto& nodeState = NodesState[SelfId().NodeId()];

        (*nodeState.NodeData.MutableResources()) = std::move(ev->Get()->Resources);
        SendInfos({SelfId().NodeId()});

        UpdateResourceSnapshotState();
    }

    void Handle(TEvKqpResourceInfoExchanger::TEvSendResources::TPtr& ev) {
        auto nodeId = ev->Sender.NodeId();

        LOG_D("Get resources info from node: " << nodeId);

        const TVector<NKikimrKqp::TResourceExchangeNodeData> resourceInfos(
            ev->Get()->Record.GetSnapshot().begin(), ev->Get()->Record.GetSnapshot().end());

        UpdateResourceInfo(resourceInfos);

        if (ev->Get()->Record.GetNeedResending()) {
            auto nodesStateIt = NodesState.find(nodeId);

            if (nodesStateIt == NodesState.end()) {
                return;
            }
            SendingToNode(nodesStateIt->second, false, {SelfId().NodeId()});
        }

        UpdateResourceSnapshotState();
    }

    void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr &ev) {
        const auto& nodeId = ev->Get()->NodeId;
        auto nodesStateIt = NodesState.find(nodeId);

        if (nodesStateIt == NodesState.end()) {
            return;
        }
        SendingToNode(nodesStateIt->second, true, {SelfId().NodeId()});
    }

    void Handle(TEvPrivate::TEvRetrySending::TPtr& ev) {
        const auto& nodeId = ev->Get()->NodeId;
        auto nodesStateIt = NodesState.find(nodeId);

        if (nodesStateIt == NodesState.end()) {
            return;
        }
        nodesStateIt->second.RetryState.IsScheduled = false;

        SendingToNode(nodesStateIt->second, true, {SelfId().NodeId()});
    }

    void Handle(TEvPrivate::TEvRetrySubscriber::TPtr&) {
        CreateSubscriber();
    }

    void Handle(TEvents::TEvPoison::TPtr&) {
        PassAway();
    }

    void Handle(TEvPrivate::TEvUpdateSnapshotState::TPtr&) {
        ResourceSnapshotRetryState.IsScheduled = false;

        UpdateResourceSnapshotState();
    }

    void Handle(TEvPrivate::TEvRegularSending::TPtr&) {
        SendInfos({SelfId().NodeId()});

        Schedule(TDuration::Seconds(2), new TEvPrivate::TEvRegularSending);
    }


private:
    STATEFN(WorkState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvBoardInfo, Handle);
            hFunc(TEvStateStorage::TEvBoardInfoUpdate, Handle);
            hFunc(TEvTenantPool::TEvTenantPoolStatus, Handle);
            hFunc(TEvKqpResourceInfoExchanger::TEvPublishResource, Handle);
            hFunc(TEvKqpResourceInfoExchanger::TEvSendResources, Handle);
            hFunc(TEvPrivate::TEvRetrySubscriber, Handle);
            hFunc(TEvPrivate::TEvRetrySending, Handle);
            hFunc(TEvPrivate::TEvRegularSending, Handle);
            hFunc(TEvPrivate::TEvUpdateSnapshotState, Handle);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
            hFunc(TEvents::TEvPoison, Handle);
        }
    }

private:

    void PassAway() override {
        Send(BoardState.Publisher, new TEvents::TEvPoison);
        Send(BoardState.Subscriber, new TEvents::TEvPoison);

        TActor::PassAway();
    }

    TDuration GetRetryDelay(TRetryState& state) {
        auto newDelay = state.CurrentDelay;
        newDelay *= 2;
        if (newDelay > TDuration::Seconds(2)) {
            newDelay = TDuration::Seconds(2);
        }
        newDelay *= AppData()->RandomProvider->Uniform(10, 200);
        newDelay /= 100;
        state.CurrentDelay = newDelay;
        return state.CurrentDelay;
    }

    void SendingToNode(TNodeState& state, bool retry,
            TVector<ui32> nodeIdsForSnapshot = {}, NKikimrKqp::TResourceExchangeSnapshot snapshotMsg = {}) {
        auto& retryState = state.RetryState;
        if (retryState.IsScheduled) {
            return;
        }

        auto owner = ActorIdFromProto(state.NodeData.GetResourceExchangeBoardData().GetOwner());
        auto nodeId = owner.NodeId();

        auto now = TlsActivationContext->Monotonic();
        if (retry && now - retryState.LastRetryAt < retryState.CurrentDelay) {
            auto at = retryState.LastRetryAt + GetRetryDelay(retryState);
            retryState.IsScheduled = true;
            Schedule(at - now, new TEvPrivate::TEvRetrySending(nodeId));
            return;
        }

        if (owner != SelfId()) {
            auto msg = MakeHolder<TEvKqpResourceInfoExchanger::TEvSendResources>();
            if (nodeIdsForSnapshot.empty()) {
                msg->Record = std::move(snapshotMsg);
            } else {
                msg->Record = CreateSnapshotMessage(nodeIdsForSnapshot);
            }
            Send(owner, msg.Release(), IEventHandle::FlagSubscribeOnSession);
        }

        retryState.LastRetryAt = now;
    }

    NKikimrKqp::TResourceExchangeSnapshot CreateSnapshotMessage(const TVector<ui32>& nodeIds,
            bool needResending = false) {
        NKikimrKqp::TResourceExchangeSnapshot snapshotMsg;
        snapshotMsg.SetNeedResending(needResending);
        auto snapshot = snapshotMsg.MutableSnapshot();
        snapshot->Reserve(nodeIds.size());

        for (const auto& nodeId: nodeIds) {
            auto* el = snapshot->Add();
            *el = NodesState[nodeId].NodeData;
        }

        return snapshotMsg;
    }

private:

    TString SelfInfo;
    TULID Ulid;
    TString UlidString;

    ui64 Round = 0;

    struct TBoardState {
        TString Tenant;
        TString Path;
        ui32 StateStorageGroupId = std::numeric_limits<ui32>::max();
        TActorId Publisher = TActorId();
        TActorId Subscriber = TActorId();
        TRetryState RetryStateForSubscriber;
        std::optional<TInstant> LastPublishTime;
    };
    TBoardState BoardState;

    TRetryState ResourceSnapshotRetryState;

    THashMap<ui32, TNodeState> NodesState;
    std::shared_ptr<TResourceSnapshotState> ResourceSnapshotState;

    TIntrusivePtr<TKqpCounters> Counters;
};

NActors::IActor* CreateKqpResourceInfoExchangerActor(TIntrusivePtr<TKqpCounters> counters,
    std::shared_ptr<TResourceSnapshotState> resourceSnapshotState)
{
    return new TKqpResourceInfoExchangerActor(counters, std::move(resourceSnapshotState));
}

} // namespace NRm
} // namespace NKqp
} // namespace NKikimr
