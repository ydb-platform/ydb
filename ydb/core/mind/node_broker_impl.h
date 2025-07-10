#pragma once

#include "node_broker.h"
#include "slot_indexes_pool.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/subdomain.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/tx_processor.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/interconnect.h>

#include <util/generic/bitmap.h>

namespace NKikimr {
namespace NNodeBroker {

using NTabletFlatExecutor::TTabletExecutedFlat;
using NTabletFlatExecutor::ITransaction;
using NTabletFlatExecutor::TTransactionBase;
using NTabletFlatExecutor::TTransactionContext;
namespace TEvConsole = NConsole::TEvConsole;
using NConsole::ITxExecutor;
using NConsole::TTxProcessor;

class INodeBrokerHooks {
protected:
    ~INodeBrokerHooks() = default;

public:
    virtual void OnActivateExecutor(ui64 tabletId);

public:
    static INodeBrokerHooks* Get();
    static void Set(INodeBrokerHooks* hooks);
};

enum class ENodeState : ui8 {
    Active = 0,
    Expired = 1,
    Removed = 2,
};

class TNodeBroker : public TActor<TNodeBroker>
                  , public TTabletExecutedFlat {
public:
    struct TEvPrivate {
        enum EEv {
            EvUpdateEpoch = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvResolvedRegistrationRequest,
            EvProcessSubscribersQueue,

            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE)");

        struct TEvUpdateEpoch : public TEventLocal<TEvUpdateEpoch, EvUpdateEpoch> {};

        struct TEvResolvedRegistrationRequest : public TEventLocal<TEvResolvedRegistrationRequest, EvResolvedRegistrationRequest> {

            TEvResolvedRegistrationRequest(
                    TEvNodeBroker::TEvRegistrationRequest::TPtr request,
                    NActors::TScopeId scopeId,
                    TSubDomainKey servicedSubDomain,
                    std::optional<TBridgePileId> bridgePileId,
                    TString error)
                : Request(request)
                , ScopeId(scopeId)
                , ServicedSubDomain(servicedSubDomain)
                , BridgePileId(bridgePileId)
                , Error(std::move(error))
            {}

            TEvNodeBroker::TEvRegistrationRequest::TPtr Request;
            NActors::TScopeId ScopeId;
            TSubDomainKey ServicedSubDomain;
            std::optional<TBridgePileId> BridgePileId;
            TString Error;
        };

        struct TEvProcessSubscribersQueue : public TEventLocal<TEvProcessSubscribersQueue, EvProcessSubscribersQueue> {};
    };

private:
    using TActorBase = TActor<TNodeBroker>;

    static constexpr TDuration MIN_LEASE_DURATION = TDuration::Minutes(5);


    struct TNodeInfo : public TEvInterconnect::TNodeInfo {
        TNodeInfo() = delete;

        TNodeInfo(ui32 nodeId,
                  const TString &address,
                  const TString &host,
                  const TString &resolveHost,
                  ui16 port,
                  const TNodeLocation &location)
            : TEvInterconnect::TNodeInfo(nodeId, address, host, resolveHost,
                                         port, location)
        {
        }

        TNodeInfo(ui32 nodeId, ENodeState state, ui64 version, const NKikimrNodeBroker::TNodeInfoSchema& schema);
        TNodeInfo(ui32 nodeId, ENodeState state, ui64 version);

        TNodeInfo(const TNodeInfo &other) = default;

        NKikimrNodeBroker::TNodeInfoSchema SerializeToSchema() const;
        bool EqualCachedData(const TNodeInfo &other) const;
        bool EqualExceptVersion(const TNodeInfo &other) const;

        bool IsFixed() const
        {
            return Expire == TInstant::Max();
        }

        static TString ExpirationString(TInstant expire)
        {
            if (expire == TInstant::Max())
                return "NEVER";
            return expire.ToRfc822StringLocal();
        }

        TString IdString() const;
        TString IdShortString() const;
        TString ToString() const;

        TString ExpirationString() const
        {
            return ExpirationString(Expire);
        }

        // Lease is incremented each time node extends its lifetime.
        ui32 Lease = 0;
        TInstant Expire;
        bool AuthorizedByCertificate = false;
        std::optional<ui32> SlotIndex;
        TSubDomainKey ServicedSubDomain;
        ENodeState State = ENodeState::Removed;
        ui64 Version = 0;
        std::optional<TBridgePileId> BridgePileId;
    };

    // State changes to apply while moving to the next epoch.
    struct TStateDiff {
        TVector<ui32> NodesToExpire;
        TVector<ui32> NodesToRemove;
        TEpochInfo NewEpoch;
        TApproximateEpochStartInfo NewApproxEpochStart;
    };

    struct TCacheVersion {
        ui64 Version;
        ui64 CacheEndOffset;

        bool operator<(ui64 version) const {
            return Version < version;
        }
    };

    struct TPipeServerInfo {
        TPipeServerInfo(TActorId id, TActorId icSession)
            : Id(id)
            , IcSession(icSession)
        {}

        TActorId Id;
        TActorId IcSession;
        THashSet<TActorId> Subscribers;
    };

    struct TSubscriberInfo : public TIntrusiveListItem<TSubscriberInfo> {
        TSubscriberInfo(TActorId id, ui64 seqNo, ui64 version, TPipeServerInfo* pipeServerInfo)
            : Id(id)
            , SeqNo(seqNo)
            , SentVersion(version)
            , PipeServerInfo(pipeServerInfo)
        {}

        TActorId Id;
        ui64 SeqNo = 0;
        ui64 SentVersion = 0;
        TPipeServerInfo* PipeServerInfo;
    };

    class TTxExtendLease;
    class TTxInitScheme;
    class TTxLoadState;
    class TTxMigrateState;
    class TTxRegisterNode;
    class TTxGracefulShutdown;
    class TTxUpdateConfig;
    class TTxUpdateConfigSubscription;
    class TTxUpdateEpoch;

    struct TDbChanges;

    ITransaction *CreateTxExtendLease(TEvNodeBroker::TEvExtendLeaseRequest::TPtr &ev);
    ITransaction *CreateTxInitScheme();
    ITransaction *CreateTxLoadState();
    ITransaction *CreateTxMigrateState(TDbChanges&& dbChanges);
    ITransaction *CreateTxRegisterNode(TEvPrivate::TEvResolvedRegistrationRequest::TPtr &ev);
    ITransaction *CreateTxGracefulShutdown(TEvNodeBroker::TEvGracefulShutdownRequest::TPtr &ev);
    ITransaction *CreateTxUpdateConfig(TEvConsole::TEvConfigNotificationRequest::TPtr &ev);
    ITransaction *CreateTxUpdateConfig(TEvNodeBroker::TEvSetConfigRequest::TPtr &ev);
    ITransaction *CreateTxUpdateConfigSubscription(TEvConsole::TEvReplaceConfigSubscriptionsResponse::TPtr &ev);
    ITransaction *CreateTxUpdateEpoch();

    void OnActivateExecutor(const TActorContext &ctx) override;
    void OnDetach(const TActorContext &ctx) override;
    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr &ev,
                      const TActorContext &ctx) override;
    void DefaultSignalTabletActive(const TActorContext &ctx) override;
    bool OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev,
                             const TActorContext &ctx) override;
    void Cleanup(const TActorContext &ctx);
    void Die(const TActorContext &ctx) override;

    template<typename TResponseEvent>
    void ReplyWithError(TActorId sender,
                        NKikimrNodeBroker::TStatus::ECode code,
                        const TString &reason,
                        const TActorContext &ctx)
    {
        LOG_DEBUG(ctx, NKikimrServices::NODE_BROKER, "Reply with %s (%s)",
                  NKikimrNodeBroker::TStatus::ECode_Name(code).data(), reason.data());

        TAutoPtr<TResponseEvent> resp = new TResponseEvent;
        resp->Record.MutableStatus()->SetCode(code);
        resp->Record.MutableStatus()->SetReason(reason);
        ctx.Send(sender, resp.Release());
    }

    STFUNC(StateInit)
    {
        LOG_DEBUG(*TlsActivationContext, NKikimrServices::NODE_BROKER, "StateInit event type: %" PRIx32 " event: %s",
                  ev->GetTypeRewrite(), ev->ToString().data());
        StateInitImpl(ev, SelfId());
    }

    STFUNC(StateWork)
    {
        TRACE_EVENT(NKikimrServices::NODE_BROKER);
        switch (ev->GetTypeRewrite()) {
            HFuncTraced(TEvConsole::TEvConfigNotificationRequest, Handle);
            HFuncTraced(TEvConsole::TEvReplaceConfigSubscriptionsResponse, Handle);
            HFuncTraced(TEvNodeBroker::TEvListNodes, Handle);
            HFuncTraced(TEvNodeBroker::TEvResolveNode, Handle);
            HFuncTraced(TEvNodeBroker::TEvRegistrationRequest, Handle);
            HFuncTraced(TEvNodeBroker::TEvGracefulShutdownRequest, Handle);
            HFuncTraced(TEvNodeBroker::TEvExtendLeaseRequest, Handle);
            HFuncTraced(TEvNodeBroker::TEvCompactTables, Handle);
            HFuncTraced(TEvNodeBroker::TEvGetConfigRequest, Handle);
            HFuncTraced(TEvNodeBroker::TEvSetConfigRequest, Handle);
            HFuncTraced(TEvNodeBroker::TEvSubscribeNodesRequest, Handle);
            HFuncTraced(TEvNodeBroker::TEvSyncNodesRequest, Handle);
            HFuncTraced(TEvPrivate::TEvUpdateEpoch, Handle);
            HFuncTraced(TEvPrivate::TEvResolvedRegistrationRequest, Handle);
            HFuncTraced(TEvPrivate::TEvProcessSubscribersQueue, Handle);
            HFunc(TEvTabletPipe::TEvServerDisconnected, Handle);
            hFunc(TEvTabletPipe::TEvServerConnected, Handle);
            IgnoreFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse);
            IgnoreFunc(NConsole::TEvConfigsDispatcher::TEvRemoveConfigSubscriptionResponse);

        default:
            if (!HandleDefaultEvents(ev, SelfId())) {
                Y_ABORT("TNodeBroker::StateWork unexpected event type: %" PRIx32 " event: %s from %s",
                       ev->GetTypeRewrite(), ev->ToString().data(),
                       ev->Sender.ToString().data());
            }
        }
    }

    void AddDelayedListNodesRequest(ui64 epoch,
                                    TEvNodeBroker::TEvListNodes::TPtr &ev);
    void ProcessListNodesRequest(TEvNodeBroker::TEvListNodes::TPtr &ev);
    void ProcessDelayedListNodesRequests();

    void ScheduleEpochUpdate(const TActorContext &ctx);
    void ScheduleProcessSubscribersQueue(const TActorContext &ctx);
    void FillNodeInfo(const TNodeInfo &node,
                      NKikimrNodeBroker::TNodeInfo &info) const;
    void FillNodeName(const std::optional<ui32> &slotIndex,
                      NKikimrNodeBroker::TNodeInfo &info) const;

    void PrepareEpochCache();
    void PrepareUpdateNodesLog();
    void AddNodeToEpochCache(const TNodeInfo &node);
    void AddDeltaToEpochDeltasCache(const TString& delta, ui64 version);
    void AddNodeToUpdateNodesLog(const TNodeInfo &node);

    void SubscribeForConfigUpdates(const TActorContext &ctx);

    void SendUpdateNodes(TSubscriberInfo &subscriber, const TActorContext &ctx);
    void SendToSubscriber(const TSubscriberInfo &subscriber, IEventBase* event, const TActorContext &ctx) const;
    void SendToSubscriber(const TSubscriberInfo &subscriber, IEventBase* event, ui64 cookie, const TActorContext &ctx) const;

    TSubscriberInfo& AddSubscriber(TActorId subscriberId, TActorId pipeServerId, ui64 seqNo, ui64 version, const TActorContext &ctx);
    void RemoveSubscriber(TActorId subscriber, const TActorContext &ctx);
    bool HasOutdatedSubscription(TActorId subscriber, ui64 newSeqNo) const;

    void UpdateCommittedStateCounters();

    void Handle(TEvConsole::TEvConfigNotificationRequest::TPtr &ev,
                const TActorContext &ctx);
    void Handle(TEvConsole::TEvReplaceConfigSubscriptionsResponse::TPtr &ev,
                const TActorContext &ctx);
    void Handle(TEvNodeBroker::TEvListNodes::TPtr &ev,
                const TActorContext &ctx);
    void Handle(TEvNodeBroker::TEvResolveNode::TPtr &ev,
                const TActorContext &ctx);
    void Handle(TEvNodeBroker::TEvRegistrationRequest::TPtr &ev,
                const TActorContext &ctx);
    void Handle(TEvNodeBroker::TEvGracefulShutdownRequest::TPtr &ev,
                const TActorContext &ctx);
    void Handle(TEvNodeBroker::TEvExtendLeaseRequest::TPtr &ev,
                const TActorContext &ctx);
    void Handle(TEvNodeBroker::TEvCompactTables::TPtr &ev,
                const TActorContext &ctx);
    void Handle(TEvNodeBroker::TEvGetConfigRequest::TPtr &ev,
                const TActorContext &ctx);
    void Handle(TEvNodeBroker::TEvSetConfigRequest::TPtr &ev,
                const TActorContext &ctx);
    void Handle(TEvNodeBroker::TEvSubscribeNodesRequest::TPtr &ev,
                const TActorContext &ctx);
    void Handle(TEvNodeBroker::TEvSyncNodesRequest::TPtr &ev,
                const TActorContext &ctx);
    void Handle(TEvTabletPipe::TEvServerConnected::TPtr &ev);
    void Handle(TEvTabletPipe::TEvServerDisconnected::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvUpdateEpoch::TPtr &ev,
                const TActorContext &ctx);
    void Handle(TEvPrivate::TEvResolvedRegistrationRequest::TPtr &ev,
                const TActorContext &ctx);
    void Handle(TEvPrivate::TEvProcessSubscribersQueue::TPtr &ev,
                const TActorContext &ctx);

    bool EnableStableNodeNames = false;
    ui64 MaxStaticId;
    ui64 MinDynamicId;
    ui64 MaxDynamicId;
    // Events collected during initialization phase.
    TMultiMap<ui64, TEvNodeBroker::TEvListNodes::TPtr> DelayedListNodesRequests;
    TSchedulerCookieHolder EpochTimerCookieHolder;

    // old epoch protocol
    TString EpochCache;
    TString EpochDeltasCache;
    TVector<TCacheVersion> EpochDeltasVersions;

    // new delta protocol
    TString UpdateNodesLog;
    TVector<TCacheVersion> UpdateNodesLogVersions;
    THashMap<TActorId, TPipeServerInfo> PipeServers;
    THashMap<TActorId, TSubscriberInfo> Subscribers;
    TIntrusiveList<TSubscriberInfo> SubscribersQueue; // sorted by version
    bool ScheduledProcessSubscribersQueue = false;

    TTabletCountersBase* TabletCounters;
    TAutoPtr<TTabletCountersBase> TabletCountersPtr;

    struct TState {
        TState(TNodeBroker* self);
        virtual ~TState() = default;

        // Internal state modifiers. Don't affect DB.
        void RegisterNewNode(const TNodeInfo &info);
        void AddNode(const TNodeInfo &info);
        void ExtendLease(TNodeInfo &node);
        void FixNodeId(TNodeInfo &node);
        void RecomputeFreeIds();
        void RecomputeSlotIndexesPools();
        bool IsBannedId(ui32 id) const;
        void ComputeNextEpochDiff(TStateDiff &diff);
        void ApplyStateDiff(const TStateDiff &diff);
        void UpdateEpochVersion();
        void LoadConfigFromProto(const NKikimrNodeBroker::TConfig &config);
        void ReleaseSlotIndex(TNodeInfo &node);
        void ClearState();
        void UpdateLocation(TNodeInfo &node, const TNodeLocation &location);
        TNodeInfo* FindNode(ui32 nodeId);

        // All registered dynamic nodes.
        THashMap<ui32, TNodeInfo> Nodes;
        THashMap<ui32, TNodeInfo> ExpiredNodes;
        THashMap<ui32, TNodeInfo> RemovedNodes;
        // Maps <Host/Addr:Port> to NodeID.
        THashMap<std::tuple<TString, TString, ui16>, ui32> Hosts;
        // Bitmap with free Node IDs (with no lower 5 bits).
        TDynBitMap FreeIds;
        // Maps tenant to its slot indexes pool.
        std::unordered_map<TSubDomainKey, TSlotIndexesPool, THash<TSubDomainKey>> SlotIndexesPools;
        // Epoch info.
        TEpochInfo Epoch;
        TApproximateEpochStartInfo ApproxEpochStart;
        // Current config.
        NKikimrNodeBroker::TConfig Config;
        TDuration EpochDuration = TDuration::Hours(1);
        TVector<std::pair<ui32, ui32>> BannedIds;
        ui64 ConfigSubscriptionId = 0;
        TString StableNodeNamePrefix = "slot-";

    protected:
        virtual TStringBuf LogPrefix() const;

        TNodeBroker* Self;
    };

    struct TDbChanges {
        bool Ready = true;
        bool UpdateEpoch = false;
        bool UpdateApproxEpochStart = false;
        bool UpdateMainNodesTable = false;
        TVector<ui32> NewVersionUpdateNodes;
        TVector<ui32> UpdateNodes;

        void Merge(const TDbChanges &other);
        bool HasNodeUpdates() const;
    };

    struct TDirtyState : public TState {
        TDirtyState(TNodeBroker* self);

        // Local database manipulations.
        void DbAddNode(const TNodeInfo &node,
                TTransactionContext &txc);
        void DbRemoveNode(const TNodeInfo &node, TTransactionContext &txc);
        void DbUpdateNode(ui32 nodeId, TTransactionContext &txc);
        void DbApplyStateDiff(const TStateDiff &diff,
                    TTransactionContext &txc);
        void DbFixNodeId(const TNodeInfo &node,
                TTransactionContext &txc);
        void DbUpdateNodes(const TVector<ui32> &nodes, TTransactionContext &txc);
        void DbUpdateConfig(const NKikimrNodeBroker::TConfig &config,
                    TTransactionContext &txc);
        void DbUpdateConfigSubscription(ui64 subscriptionId,
                                TTransactionContext &txc);
        void DbUpdateEpoch(const TEpochInfo &epoch,
                    TTransactionContext &txc);
        void DbUpdateApproxEpochStart(const TApproximateEpochStartInfo &approxEpochStart, TTransactionContext &txc);
        void DbUpdateMainNodesTable(TTransactionContext &txc);
        void DbUpdateEpochVersion(ui64 version,
                        TTransactionContext &txc);
        void DbUpdateNodeLease(const TNodeInfo &node,
                        TTransactionContext &txc);
        void DbUpdateNodeLocation(const TNodeInfo &node,
                        TTransactionContext &txc);
        void DbReleaseSlotIndex(const TNodeInfo &node,
                        TTransactionContext &txc);
        void DbUpdateNodeAuthorizedByCertificate(const TNodeInfo &node,
                        TTransactionContext &txc);

        TDbChanges DbLoadState(TTransactionContext &txc, const TActorContext &ctx);
        TDbChanges DbLoadNodes(auto &nodesRowset, const TActorContext &ctx);
        TDbChanges DbMigrateNodes(auto &nodesV2Rowset, const TActorContext &ctx);
        TDbChanges DbLoadNodesV2(auto &nodesV2Rowset, const TActorContext &ctx);
        TDbChanges DbMigrateNodesV2();

    protected:
        TStringBuf LogPrefix() const override;
        TStringBuf DbLogPrefix() const;
    };

    /**
     * NodeBroker in-memory state is divided into two parts to take advantage of pipelining:
     *
     *  1) Dirty - state that is read and updated only during Execute() of LocalDB transactions.
     *  2) Committed - state that is updated only in Complete() of LocalDB transactions. 
     *     This state is safe to use when responding to requests, its data is persistently stored. 
     */
    TDirtyState Dirty;
    TState Committed;

public:
    TNodeBroker(const TActorId &tablet, TTabletStorageInfo *info);

    static constexpr NKikimrServices::TActivity::EType ActorActivityType()
    {
        return NKikimrServices::TActivity::NODE_BROKER_ACTOR;
    }
};

} // NNodeBroker
} // NKikimr
