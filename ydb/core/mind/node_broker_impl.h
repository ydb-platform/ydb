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
using NConsole::TEvConsole;
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

class TNodeBroker : public TActor<TNodeBroker>
                  , public TTabletExecutedFlat
                  , public ITxExecutor {
public:
    struct TEvPrivate {
        enum EEv {
            EvUpdateEpoch = EventSpaceBegin(TEvents::ES_PRIVATE),

            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE)");

        struct TEvUpdateEpoch : public TEventLocal<TEvUpdateEpoch, EvUpdateEpoch> {};
    };

private:
    using TActorBase = TActor<TNodeBroker>;

    static constexpr TDuration MIN_LEASE_DURATION = TDuration::Minutes(5);

    enum EConfigKey {
        ConfigKeyConfig = 1,
    };

    enum EParamKey {
        ParamKeyConfigSubscription = 1,
        ParamKeyCurrentEpochId,
        ParamKeyCurrentEpochVersion,
        ParamKeyCurrentEpochStart,
        ParamKeyCurrentEpochEnd,
        ParamKeyNextEpochEnd,
    };

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
            , Lease(0)
        {
        }

        TNodeInfo(const TNodeInfo &other) = default;

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

        TString IdString() const
        {
            return TStringBuilder() << "#" << NodeId << " " << Host << ":" << Port;
        }

        TString ExpirationString() const
        {
            return ExpirationString(Expire);
        }

        // Lease is incremented each time node extends its lifetime.
        ui32 Lease;
        TInstant Expire;
        bool AuthorizedByCertificate = false;
        std::optional<ui32> SlotIndex;
        TSubDomainKey ServicedSubDomain;
    };

    // State changes to apply while moving to the next epoch.
    struct TStateDiff {
        TVector<ui32> NodesToExpire;
        TVector<ui32> NodesToRemove;
        TEpochInfo NewEpoch;
    };

    class TTxExtendLease;
    class TTxInitScheme;
    class TTxLoadState;
    class TTxRegisterNode;
    class TTxUpdateConfig;
    class TTxUpdateConfigSubscription;
    class TTxUpdateEpoch;

    ITransaction *CreateTxExtendLease(TEvNodeBroker::TEvExtendLeaseRequest::TPtr &ev);
    ITransaction *CreateTxInitScheme();
    ITransaction *CreateTxLoadState();
    ITransaction *CreateTxRegisterNode(TEvNodeBroker::TEvRegistrationRequest::TPtr &ev,
                                       const NActors::TScopeId& scopeId,
                                       const TSubDomainKey& servicedSubDomain);
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
            HFuncTraced(TEvNodeBroker::TEvExtendLeaseRequest, Handle);
            HFuncTraced(TEvNodeBroker::TEvCompactTables, Handle);
            HFuncTraced(TEvNodeBroker::TEvGetConfigRequest, Handle);
            HFuncTraced(TEvNodeBroker::TEvSetConfigRequest, Handle);
            HFuncTraced(TEvPrivate::TEvUpdateEpoch, Handle);
            IgnoreFunc(TEvTabletPipe::TEvServerConnected);
            IgnoreFunc(TEvTabletPipe::TEvServerDisconnected);
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

    void ClearState();

    ui32 NodeIdStep() const {
        return SingleDomainAlloc ? 1 : (1 << DOMAIN_BITS);
    }

    ui32 NodeIdDomain(ui32 nodeId) const {
        return SingleDomainAlloc ? DomainId : (nodeId & DOMAIN_MASK);
    }

    ui32 RewriteNodeId(ui32 nodeId) const {
        return SingleDomainAlloc ? nodeId : ((nodeId & ~DOMAIN_MASK) | DomainId);
    }

    // Internal state modifiers. Don't affect DB.
    void AddNode(const TNodeInfo &info);
    void RemoveNode(ui32 nodeId);
    void ExtendLease(TNodeInfo &node);
    void FixNodeId(TNodeInfo &node);
    void RecomputeFreeIds();
    void RecomputeSlotIndexesPools();
    bool IsBannedId(ui32 id) const;

    void AddDelayedListNodesRequest(ui64 epoch,
                                    TEvNodeBroker::TEvListNodes::TPtr &ev);
    void ProcessListNodesRequest(TEvNodeBroker::TEvListNodes::TPtr &ev);
    void ProcessDelayedListNodesRequests();

    void ScheduleEpochUpdate(const TActorContext &ctx);
    void FillNodeInfo(const TNodeInfo &node,
                      NKikimrNodeBroker::TNodeInfo &info) const;
    void FillNodeName(const std::optional<ui32> &slotIndex,
                      NKikimrNodeBroker::TNodeInfo &info) const;

    void ComputeNextEpochDiff(TStateDiff &diff);
    void ApplyStateDiff(const TStateDiff &diff);
    void UpdateEpochVersion();
    void PrepareEpochCache();
    void AddNodeToEpochCache(const TNodeInfo &node);

    void SubscribeForConfigUpdates(const TActorContext &ctx);

    void ProcessTx(ITransaction *tx,
                   const TActorContext &ctx);
    void ProcessTx(ui32 nodeId,
                   ITransaction *tx,
                   const TActorContext &ctx);
    void TxCompleted(ITransaction *tx,
                     const TActorContext &ctx);
    void TxCompleted(ui32 nodeId,
                     ITransaction *tx,
                     const TActorContext &ctx);

    void LoadConfigFromProto(const NKikimrNodeBroker::TConfig &config);

    // Local database manipulations.
    void DbAddNode(const TNodeInfo &node,
                   TTransactionContext &txc);
    void DbApplyStateDiff(const TStateDiff &diff,
                          TTransactionContext &txc);
    void DbFixNodeId(const TNodeInfo &node,
                     TTransactionContext &txc);
    bool DbLoadState(TTransactionContext &txc,
                     const TActorContext &ctx);
    void DbRemoveNodes(const TVector<ui32> &nodes,
                       TTransactionContext &txc);
    void DbUpdateConfig(const NKikimrNodeBroker::TConfig &config,
                        TTransactionContext &txc);
    void DbUpdateConfigSubscription(ui64 subscriptionId,
                                    TTransactionContext &txc);
    void DbUpdateEpoch(const TEpochInfo &epoch,
                       TTransactionContext &txc);
    void DbUpdateEpochVersion(ui64 version,
                              TTransactionContext &txc);
    void DbUpdateNodeLease(const TNodeInfo &node,
                           TTransactionContext &txc);
    void DbUpdateNodeLocation(const TNodeInfo &node,
                              TTransactionContext &txc);

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
    void Handle(TEvNodeBroker::TEvExtendLeaseRequest::TPtr &ev,
                const TActorContext &ctx);
    void Handle(TEvNodeBroker::TEvCompactTables::TPtr &ev,
                const TActorContext &ctx);
    void Handle(TEvNodeBroker::TEvGetConfigRequest::TPtr &ev,
                const TActorContext &ctx);
    void Handle(TEvNodeBroker::TEvSetConfigRequest::TPtr &ev,
                const TActorContext &ctx);
    void Handle(TEvPrivate::TEvUpdateEpoch::TPtr &ev,
                const TActorContext &ctx);

    // ID of domain node broker is responsible for.
    ui32 DomainId;
    // All registered dynamic nodes.
    THashMap<ui32, TNodeInfo> Nodes;
    THashMap<ui32, TNodeInfo> ExpiredNodes;
    // Maps <Host/Addr:Port> to NodeID.
    THashMap<std::tuple<TString, TString, ui16>, ui32> Hosts;
    // Bitmap with free Node IDs (with no lower 5 bits).
    TDynBitMap FreeIds;
    // Maps tenant to its slot indexes pool.
    std::unordered_map<TSubDomainKey, TSlotIndexesPool, THash<TSubDomainKey>> SlotIndexesPools;
    bool EnableStableNodeNames = false;
    // Epoch info.
    TEpochInfo Epoch;
    // Current config.
    NKikimrNodeBroker::TConfig Config;
    ui64 MaxStaticId;
    ui64 MinDynamicId;
    ui64 MaxDynamicId;
    TDuration EpochDuration;
    TVector<std::pair<ui32, ui32>> BannedIds;
    ui64 ConfigSubscriptionId;
    TString StableNodeNamePrefix;

    // Events collected during initialization phase.
    TMultiMap<ui64, TEvNodeBroker::TEvListNodes::TPtr> DelayedListNodesRequests;
    // Transactions queue.
    TTxProcessor::TPtr TxProcessor;
    TSchedulerCookieHolder EpochTimerCookieHolder;
    TString EpochCache;

    bool SingleDomain = false;
    bool SingleDomainAlloc = false;

public:
    TNodeBroker(const TActorId &tablet, TTabletStorageInfo *info)
        : TActor(&TThis::StateInit)
        , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
        , EpochDuration(TDuration::Hours(1))
        , ConfigSubscriptionId(0)
        , StableNodeNamePrefix("slot-")
        , TxProcessor(new TTxProcessor(*this, "root", NKikimrServices::NODE_BROKER))
    {
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType()
    {
        return NKikimrServices::TActivity::NODE_BROKER_ACTOR;
    }

    void Execute(ITransaction *transaction, const TActorContext &ctx) override
    {
        TTabletExecutedFlat::Execute(transaction, ctx);
    }
};

} // NNodeBroker
} // NKikimr
