#include "kqp_rm_service.h"

#include <ydb/core/base/location.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/mind/tenant_pool.h>
#include <ydb/core/kqp/common/kqp_event_ids.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/interconnect/interconnect.h>

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

    static constexpr ui32 BUCKETS_COUNT = 32;

    struct TEvPrivate {
        enum EEv {
            EvSendToNode = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvRegularSending,
            EvRetrySubscriber,
            EvUpdateSnapshotState,
            EvRetryNode,
        };

        struct TEvSendToNode :
                public TEventLocal<TEvSendToNode, EEv::EvSendToNode> {
            ui32 BucketInd;

            TEvSendToNode(ui32 bucketInd) : BucketInd(bucketInd) {
            }
        };

        struct TEvRetryNode :
                public TEventLocal<TEvRetryNode, EEv::EvRetryNode> {
            ui32 BucketInd;

            TEvRetryNode(ui32 bucketInd) : BucketInd(bucketInd) {
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

    struct TDelayState {
        bool IsScheduled = false;
        NMonotonic::TMonotonic LastRetryAt = TMonotonic::Zero();
        TDuration CurrentDelay = TDuration::Zero();
    };

    struct TDelaySettings {
        TDuration StartDelayMs = TDuration::MilliSeconds(500);
        TDuration MaxDelayMs = TDuration::MilliSeconds(2000);
    };

    struct TNodeState {
        NMonotonic::TMonotonic LastUpdateAt = TMonotonic::Zero();
        NKikimrKqp::TResourceExchangeNodeData NodeData;
    };

    struct TBucketState {
        TDelayState RetryState;
        TDelayState SendState;
        THashSet<ui32> NodeIdsToSend;
        THashSet<ui32> NodeIdsToRetry;
    };

public:

    TKqpResourceInfoExchangerActor(TIntrusivePtr<TKqpCounters> counters,
        std::shared_ptr<TResourceSnapshotState> resourceSnapshotState,
        const NKikimrConfig::TTableServiceConfig::TResourceManager::TInfoExchangerSettings& settings)
        : ResourceSnapshotState(std::move(resourceSnapshotState))
        , Counters(counters)
        , Settings(settings)
    {
        Buckets.resize(BUCKETS_COUNT);

        const auto& publisherSettings = settings.GetPublisherSettings();
        const auto& subscriberSettings = settings.GetSubscriberSettings();
        const auto& exchangerSettings = settings.GetExchangerSettings();

        UpdateBoardRetrySettings(publisherSettings, PublisherSettings);
        UpdateBoardRetrySettings(subscriberSettings, SubscriberSettings);

        if (exchangerSettings.HasStartDelayMs()) {
            ExchangerSettings.StartDelayMs = TDuration::MilliSeconds(exchangerSettings.GetStartDelayMs());
        }
        if (exchangerSettings.HasMaxDelayMs()) {
            ExchangerSettings.MaxDelayMs = TDuration::MilliSeconds(exchangerSettings.GetMaxDelayMs());
        }
    }

    void Bootstrap() {
        LOG_D("Start KqpResourceInfoExchangerActor at " << SelfId());

        ui32 tableServiceConfigKind = (ui32) NKikimrConsole::TConfigItem::TableServiceConfigItem;

        Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()),
             new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest({tableServiceConfigKind}),
             IEventHandle::FlagTrackDelivery);

        Become(&TKqpResourceInfoExchangerActor::WorkState);

        Send(MakeTenantPoolRootID(), new TEvents::TEvSubscribe);
    }

private:

    bool UpdateBoardRetrySettings(
            const NKikimrConfig::TTableServiceConfig::TResourceManager::TRetrySettings& settings,
            TBoardRetrySettings& retrySetting) {
        bool ret = false;
        if (settings.HasStartDelayMs()) {
            ret = true;
            retrySetting.StartDelayMs = TDuration::MilliSeconds(settings.GetStartDelayMs());
        }
        if (settings.HasMaxDelayMs()) {
            ret = true;
            retrySetting.MaxDelayMs = TDuration::MilliSeconds(settings.GetMaxDelayMs());
        }
        return ret;
    }

    static TString MakeKqpInfoExchangerBoardPath(TStringBuf database) {
        return TStringBuilder() << "kqpexch+" << database;
    }

    void CreateSubscriber() {
        auto& retryState = BoardState.RetryStateForSubscriber;

        auto now = TlsActivationContext->Monotonic();
        if (now - retryState.LastRetryAt < GetCurrentDelay(retryState)) {
            auto at = retryState.LastRetryAt + GetDelay(retryState);
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
            BoardState.Path, SelfId(), EBoardLookupMode::Subscription,
            SubscriberSettings);
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
            /* ttlMs */ 0, /* reg */ true, PublisherSettings);
        BoardState.Publisher = Register(publisher);

        auto& nodeState = NodesState[SelfId().NodeId()];
        auto& info = nodeState.NodeData;
        auto* boardData = info.MutableResourceExchangeBoardData();
        ActorIdToProto(BoardState.Publisher, boardData->MutablePublisher());
    }

    std::pair<TVector<ui32>, bool> UpdateBoardInfo(const TMap<TActorId, TEvStateStorage::TBoardInfoEntry>& infos) {
        auto nodeIds = TVector<ui32>();
        bool isChanged = false;

        auto now = TlsActivationContext->Monotonic();
        for (const auto& [publisherId, entry] : infos) {
            if (entry.Dropped) {
                auto nodesStateIt = NodesState.find(publisherId.NodeId());

                if (nodesStateIt == NodesState.end()) {
                    continue;
                }

                auto bucketInd = GetBucketInd(publisherId.NodeId());
                auto& bucket = Buckets[bucketInd];

                auto& currentBoardData = nodesStateIt->second.NodeData.GetResourceExchangeBoardData();
                auto currentPublisher = ActorIdFromProto(currentBoardData.GetPublisher());
                if (currentPublisher == publisherId) {
                    NodesState.erase(nodesStateIt);
                    isChanged = true;
                    bucket.NodeIdsToSend.erase(publisherId.NodeId());
                    bucket.NodeIdsToRetry.erase(publisherId.NodeId());
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

            auto bucketInd = GetBucketInd(owner.NodeId());
            auto& bucket = Buckets[bucketInd];

            if (currentUlidString.empty()) {
                *currentBoardData = std::move(boardData);
                ActorIdToProto(publisherId, currentBoardData->MutablePublisher());
                nodeIds.push_back(owner.NodeId());
                isChanged = true;
                nodeState.LastUpdateAt = now;
                bucket.NodeIdsToSend.insert(owner.NodeId());
                continue;
            }

            auto currentUlid = TULID::FromBinary(currentUlidString);
            auto ulid = TULID::FromBinary(ulidString);

            if (currentUlid < ulid) {
                *currentBoardData = std::move(boardData);
                ActorIdToProto(publisherId, currentBoardData->MutablePublisher());
                currentInfo.SetRound(0);
                isChanged = true;
                (*currentInfo.MutableResources()) = NKikimrKqp::TKqpNodeResources();
                nodeIds.push_back(owner.NodeId());
                nodeState.LastUpdateAt = now;
                bucket.NodeIdsToSend.insert(owner.NodeId());
                continue;
            }
        }

        Counters->RmNodeNumberInSnapshot->Set(NodesState.size());

        return {nodeIds, isChanged};
    }

    bool UpdateResourceInfo(const TVector<NKikimrKqp::TResourceExchangeNodeData>& infos) {
        bool isChanged = false;

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
                isChanged = true;
                continue;
            }
        }

        return isChanged;
    }

    void UpdateResourceSnapshotState() {
        if (ResourceSnapshotRetryState.IsScheduled) {
            return;
        }
        auto now = TlsActivationContext->Monotonic();
        if (now - ResourceSnapshotRetryState.LastRetryAt < GetCurrentDelay(ResourceSnapshotRetryState)) {
            auto at = ResourceSnapshotRetryState.LastRetryAt + GetDelay(ResourceSnapshotRetryState);
            ResourceSnapshotRetryState.IsScheduled = true;
            Schedule(at - now, new TEvPrivate::TEvUpdateSnapshotState);
            return;
        }
        TVector<NKikimrKqp::TKqpNodeResources> resources;
        resources.reserve(NodesState.size());

        for (const auto& [nodeId, state] : NodesState) {
            const auto& currentResources = state.NodeData.GetResources();
            if (currentResources.HasNodeId()) {
                resources.push_back(std::move(currentResources));
            }
        }

        with_lock (ResourceSnapshotState->Lock) {
            ResourceSnapshotState->Snapshot =
                std::make_shared<TVector<NKikimrKqp::TKqpNodeResources>>(std::move(resources));
        }

        ResourceSnapshotRetryState.LastRetryAt = now;
    }

    void SendInfos(TVector<ui32> infoNodeIds, bool resending = false, TVector<ui32> nodeIds = {}) {
        auto& nodeState = NodesState[SelfId().NodeId()];
        nodeState.NodeData.SetRound(Round++);

        if (!nodeIds.empty()) {
            auto snapshotMsg = CreateSnapshotMessage(infoNodeIds, resending);

            for (const auto& nodeId : nodeIds) {
                auto nodesStateIt = NodesState.find(nodeId);

                if (nodesStateIt == NodesState.end()) {
                    continue;
                }

                auto owner = ActorIdFromProto(nodesStateIt->second.NodeData.GetResourceExchangeBoardData().GetOwner());
                if (owner != SelfId()) {
                    auto msg = MakeHolder<TEvKqpResourceInfoExchanger::TEvSendResources>();
                    msg->Record = snapshotMsg;
                    Send(owner, msg.Release(), IEventHandle::FlagSubscribeOnSession);
                }
            }
        } else {
            auto snapshotMsg = CreateSnapshotMessage(infoNodeIds, false);

            for (size_t idx = 0; idx < BUCKETS_COUNT; idx++) {
                auto& bucket = Buckets[idx];
                if (!bucket.NodeIdsToSend.empty()) {
                    SendingToBucketNodes(idx, bucket, false, {}, snapshotMsg);
                }
            }
        }
    }

private:

    void Handle(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr&) {
        LOG_D("Subscribed for config changes.");
    }

    void Handle(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev) {
        auto &event = ev->Get()->Record;

        NKikimrConfig::TTableServiceConfig tableServiceConfig;

        tableServiceConfig.Swap(event.MutableConfig()->MutableTableServiceConfig());
        LOG_D("Updated table service config.");

        const auto& infoExchangerSettings = tableServiceConfig.GetResourceManager().GetInfoExchangerSettings();
        const auto& publisherSettings = infoExchangerSettings.GetPublisherSettings();
        const auto& subscriberSettings = infoExchangerSettings.GetSubscriberSettings();
        const auto& exchangerSettings = infoExchangerSettings.GetExchangerSettings();

        if (UpdateBoardRetrySettings(publisherSettings, PublisherSettings)) {
            CreatePublisher();
        }

        if (UpdateBoardRetrySettings(subscriberSettings, SubscriberSettings)) {
            CreateSubscriber();
        }

        if (exchangerSettings.HasStartDelayMs()) {
            ExchangerSettings.StartDelayMs = TDuration::MilliSeconds(exchangerSettings.GetStartDelayMs());
        }
        if (exchangerSettings.HasMaxDelayMs()) {
            ExchangerSettings.MaxDelayMs = TDuration::MilliSeconds(exchangerSettings.GetMaxDelayMs());
        }

        auto responseEv = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationResponse>(event);
        Send(ev->Sender, responseEv.Release(), IEventHandle::FlagTrackDelivery, ev->Cookie);
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        switch (ev->Get()->SourceType) {
            case NConsole::TEvConfigsDispatcher::EvSetConfigSubscriptionRequest:
                LOG_C("Failed to deliver subscription request to config dispatcher.");
                break;

            case NConsole::TEvConsole::EvConfigNotificationResponse:
                LOG_E("Failed to deliver config notification response.");
                break;

            default:
                break;
        }
    }

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

        if (auto *domain = AppData()->DomainsInfo->GetDomain(); domain->Name != ExtractDomain(tenant)) {
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
            << ", board: " << BoardState.Path);
    }


    void Handle(TEvStateStorage::TEvBoardInfo::TPtr& ev) {
        if (ev->Get()->Status == TEvStateStorage::TEvBoardInfo::EStatus::NotAvailable) {
            LOG_I("Subcriber is not available for info exchanger, serving tenant: " << BoardState.Tenant
                << ", board: " << BoardState.Path);
            CreateSubscriber();
            return;
        }

        LOG_D("Get board info from subscriber, serving tenant: " << BoardState.Tenant
                << ", board: " << BoardState.Path
                << ", with size: " << ev->Get()->InfoEntries.size());

        auto [nodeIds, isChanged] = UpdateBoardInfo(ev->Get()->InfoEntries);

        if (!nodeIds.empty()) {
            SendInfos({SelfId().NodeId()}, true, std::move(nodeIds));
        }

        if (isChanged) {
            UpdateResourceSnapshotState();
        }
    }

    void Handle(TEvStateStorage::TEvBoardInfoUpdate::TPtr& ev) {
        if (ev->Get()->Status == TEvStateStorage::TEvBoardInfo::EStatus::NotAvailable) {
            LOG_I("Subcriber is not available for info exchanger, serving tenant: " << BoardState.Tenant
                << ", board: " << BoardState.Path);
            CreateSubscriber();
            return;
        }
        LOG_D("Get board info update from subscriber, serving tenant: " << BoardState.Tenant
                << ", board: " << BoardState.Path
                << ", with size: " << ev->Get()->Updates.size());

        auto [nodeIds, isChanged] = UpdateBoardInfo(ev->Get()->Updates);

        if (!nodeIds.empty()) {
            SendInfos({SelfId().NodeId()}, true, std::move(nodeIds));
        }

        if (isChanged) {
            UpdateResourceSnapshotState();
        }
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

        bool isChanged = UpdateResourceInfo(resourceInfos);

        if (ev->Get()->Record.GetNeedResending()) {
            auto nodesStateIt = NodesState.find(nodeId);

            if (nodesStateIt != NodesState.end()) {
                SendInfos({SelfId().NodeId()}, false, {nodesStateIt->first});
            }
        }

        if (isChanged) {
            UpdateResourceSnapshotState();
        }
    }

    void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr &ev) {
        const auto& nodeId = ev->Get()->NodeId;

        if (NodesState.find(nodeId) == NodesState.end()) {
            return;
        }

        auto bucketInd = GetBucketInd(nodeId);
        auto& bucket = Buckets[bucketInd];

        bucket.NodeIdsToRetry.insert(nodeId);

        SendingToBucketNodes(bucketInd, bucket, true, {SelfId().NodeId()});
    }

    void Handle(TEvPrivate::TEvSendToNode::TPtr& ev) {
        const auto& bucketInd = ev->Get()->BucketInd;
        auto& bucket = Buckets[bucketInd];

        bucket.SendState.IsScheduled = false;

        if (!bucket.NodeIdsToSend.empty()) {
            SendingToBucketNodes(bucketInd, bucket, false, {SelfId().NodeId()});
        }
    }

    void Handle(TEvPrivate::TEvRetryNode::TPtr& ev) {
        const auto& bucketInd = ev->Get()->BucketInd;
        auto& bucket = Buckets[bucketInd];

        bucket.RetryState.IsScheduled = false;

        if (!bucket.NodeIdsToRetry.empty()) {
            SendingToBucketNodes(bucketInd, bucket, true, {SelfId().NodeId()});
        }
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
            hFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, Handle);
            hFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
            hFunc(TEvStateStorage::TEvBoardInfoUpdate, Handle);
            hFunc(TEvTenantPool::TEvTenantPoolStatus, Handle);
            hFunc(TEvKqpResourceInfoExchanger::TEvPublishResource, Handle);
            hFunc(TEvKqpResourceInfoExchanger::TEvSendResources, Handle);
            hFunc(TEvPrivate::TEvRetrySubscriber, Handle);
            hFunc(TEvPrivate::TEvSendToNode, Handle);
            hFunc(TEvPrivate::TEvRetryNode, Handle);
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

    const TDuration& GetCurrentDelay(TDelayState& state) {
        if (state.CurrentDelay == TDuration::Zero()) {
            state.CurrentDelay = ExchangerSettings.StartDelayMs;
        }
        return state.CurrentDelay;
    }

    TDuration GetDelay(TDelayState& state) {
        auto newDelay = state.CurrentDelay;
        newDelay *= 2;
        if (newDelay > ExchangerSettings.MaxDelayMs) {
            newDelay = ExchangerSettings.MaxDelayMs;
        }
        newDelay *= AppData()->RandomProvider->Uniform(50, 200);
        newDelay /= 100;
        state.CurrentDelay = newDelay;
        return state.CurrentDelay;
    }

    ui32 GetBucketInd(ui32 nodeId) {
        return nodeId & (BUCKETS_COUNT - 1);
    }

    void SendingToBucketNodes(ui32 bucketInd, TBucketState& bucketState, bool retry,
            TVector<ui32> nodeIdsForSnapshot = {}, NKikimrKqp::TResourceExchangeSnapshot snapshotMsg = {}) {
        TDelayState* retryState;
        if (retry) {
            retryState = &bucketState.RetryState;
        } else {
            retryState = &bucketState.SendState;
        }

        if (retryState->IsScheduled) {
            return;
        }

        auto now = TlsActivationContext->Monotonic();
        if (now - retryState->LastRetryAt < GetCurrentDelay(*retryState)) {
            auto at = retryState->LastRetryAt + GetDelay(*retryState);
            retryState->IsScheduled = true;
            if (retry) {
                Schedule(at - now, new TEvPrivate::TEvRetryNode(bucketInd));
            } else {
                Schedule(at - now, new TEvPrivate::TEvSendToNode(bucketInd));
            }
            return;
        }

        if (!nodeIdsForSnapshot.empty()) {
            snapshotMsg = CreateSnapshotMessage(nodeIdsForSnapshot);
        }

        THashSet<ui32>* nodeIds;
        if (retry) {
            nodeIds = &bucketState.NodeIdsToRetry;
        } else {
            nodeIds = &bucketState.NodeIdsToSend;
        }

        for (const auto& nodeId: *nodeIds) {
            auto nodesStateIt = NodesState.find(nodeId);
            if (nodesStateIt == NodesState.end()) {
                continue;
            }
            auto& nodeState = nodesStateIt->second;
            auto owner = ActorIdFromProto(nodeState.NodeData.GetResourceExchangeBoardData().GetOwner());
            if (owner != SelfId()) {
                auto msg = MakeHolder<TEvKqpResourceInfoExchanger::TEvSendResources>();
                msg->Record = snapshotMsg;
                Send(owner, msg.Release(), IEventHandle::FlagSubscribeOnSession);
            }
        }

        if (retry) {
            bucketState.NodeIdsToRetry.clear();
        }

        retryState->LastRetryAt = now;
    }

    NKikimrKqp::TResourceExchangeSnapshot CreateSnapshotMessage(const TVector<ui32>& nodeIds,
            bool needResending = false) {
        NKikimrKqp::TResourceExchangeSnapshot snapshotMsg;
        snapshotMsg.SetNeedResending(needResending);
        auto snapshot = snapshotMsg.MutableSnapshot();
        snapshot->Reserve(nodeIds.size());

        for (const auto& nodeId: nodeIds) {
            auto* el = snapshot->Add();

            auto nodesStateIt = NodesState.find(nodeId);
            if (nodesStateIt == NodesState.end()) {
                continue;
            }

            *el = nodesStateIt->second.NodeData;
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
        TActorId Publisher = TActorId();
        TActorId Subscriber = TActorId();
        TDelayState RetryStateForSubscriber;
        std::optional<TInstant> LastPublishTime;
    };
    TBoardState BoardState;

    TDelayState ResourceSnapshotRetryState;

    TVector<TBucketState> Buckets;
    THashMap<ui32, TNodeState> NodesState;
    std::shared_ptr<TResourceSnapshotState> ResourceSnapshotState;

    TBoardRetrySettings PublisherSettings;
    TBoardRetrySettings SubscriberSettings;
    TDelaySettings ExchangerSettings;

    TIntrusivePtr<TKqpCounters> Counters;
    NKikimrConfig::TTableServiceConfig::TResourceManager::TInfoExchangerSettings Settings;
};

NActors::IActor* CreateKqpResourceInfoExchangerActor(TIntrusivePtr<TKqpCounters> counters,
    std::shared_ptr<TResourceSnapshotState> resourceSnapshotState,
    const NKikimrConfig::TTableServiceConfig::TResourceManager::TInfoExchangerSettings& settings)
{
    return new TKqpResourceInfoExchangerActor(counters, std::move(resourceSnapshotState), settings);
}

} // namespace NRm
} // namespace NKqp
} // namespace NKikimr
