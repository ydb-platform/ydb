#pragma once

#include "dynamic_nameserver.h"
#include "node_broker.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/interconnect/events_local.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/util/intrusive_heap.h>

#include <ydb/library/services/services.pb.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/bitmap.h>

#if defined LOG_T || \
    defined LOG_D || \
    defined LOG_I || \
    defined LOG_N || \
    defined LOG_W || \
    defined LOG_W
#error log macro redefinition
#endif

#define LOG_T(stream) LOG_TRACE_S((TlsActivationContext->AsActorContext()), NKikimrServices::NAMESERVICE, stream)
#define LOG_D(stream) LOG_DEBUG_S((TlsActivationContext->AsActorContext()), NKikimrServices::NAMESERVICE, stream)
#define LOG_I(stream) LOG_INFO_S((TlsActivationContext->AsActorContext()), NKikimrServices::NAMESERVICE, stream)
#define LOG_N(stream) LOG_NOTICE_S((TlsActivationContext->AsActorContext()), NKikimrServices::NAMESERVICE, stream)
#define LOG_W(stream) LOG_WARN_S((TlsActivationContext->AsActorContext()), NKikimrServices::NAMESERVICE, stream)
#define LOG_E(stream) LOG_ERROR_S((TlsActivationContext->AsActorContext()), NKikimrServices::NAMESERVICE, stream)

namespace NKikimr {
namespace NNodeBroker {

struct TDynamicConfig;
using TDynamicConfigPtr = TIntrusivePtr<TDynamicConfig>;

class TDynamicNameserver;
class TListNodesCache;

class TCacheMiss {
public:
    TCacheMiss(ui32 nodeId, TDynamicConfigPtr config, TAutoPtr<IEventHandle> origRequest,
               TMonotonic deadline, ui32 syncCookie);
    virtual ~TCacheMiss() = default;
    virtual void OnSuccess(const TActorContext &);
    virtual void OnError(const TString &error, const TActorContext &);

    virtual void ConvertToActor(TDynamicNameserver* owner, TIntrusivePtr<TListNodesCache> listNodesCache,
                                const TActorContext &ctx) = 0;

    struct THeapIndexByDeadline {
        size_t& operator()(TCacheMiss& cacheMiss) const;
    };

    struct TCompareByDeadline {
        bool operator()(const TCacheMiss& a, const TCacheMiss& b) const;
    };

public:
    const ui32 NodeId;
    const TMonotonic Deadline;
    bool NeedScheduleDeadline;
    const ui32 SyncCookie;

protected:
    TDynamicConfigPtr Config;
    TAutoPtr<IEventHandle> OrigRequest;
    size_t DeadlineHeapIndex = -1;
};

struct TDynamicConfig : public TThrRefBase {
    struct TDynamicNodeInfo : public TTableNameserverSetup::TNodeInfo {
        TDynamicNodeInfo()
        {
        }

        TDynamicNodeInfo(const TString &address,
                         const TString &host,
                         const TString &resolveHost,
                         ui16 port,
                         const TNodeLocation &location,
                         TInstant expire)
            : TNodeInfo(address, host, resolveHost, port, location)
            , Expire(expire)
        {
        }

        TDynamicNodeInfo(const NKikimrNodeBroker::TNodeInfo &info)
            : TDynamicNodeInfo(info.GetAddress(),
                               info.GetHost(),
                               info.GetResolveHost(),
                               (ui16)info.GetPort(),
                               TNodeLocation(info.GetLocation()),
                               TInstant::MicroSeconds(info.GetExpire()))
        {
        }

        TDynamicNodeInfo(const TDynamicNodeInfo &other) = default;
        TDynamicNodeInfo &operator=(const TDynamicNodeInfo &other) = default;

        bool EqualExceptExpire(const TDynamicNodeInfo &other) const
        {
            return Host == other.Host
                && Address == other.Address
                && ResolveHost == other.ResolveHost
                && Port == other.Port
                && Location == other.Location;
        }

        TInstant Expire;
    };

    THashMap<ui32, TDynamicNodeInfo> DynamicNodes;
    THashSet<ui32> ExpiredNodes;
    TEpochInfo Epoch;
    TActorId NodeBrokerPipe;

    using TPendingCacheMissesQueue = TIntrusiveHeap<TCacheMiss, TCacheMiss::THeapIndexByDeadline, TCacheMiss::TCompareByDeadline>;
    TPendingCacheMissesQueue PendingCacheMisses;
    // Used to know who owns CacheMiss memory - ActorSystem or DynamicNameservice
    std::unordered_map<TCacheMiss*, THolder<TCacheMiss>> CacheMissHolders;
};

class TListNodesCache : public TSimpleRefCount<TListNodesCache> {
public:
    TListNodesCache();

    void Update(TIntrusiveVector<TEvInterconnect::TNodeInfo>::TConstPtr newNodes, TInstant newExpire,
        std::shared_ptr<const TEvInterconnect::TEvNodesInfo::TPileMap>&& pileMap);
    void Invalidate();
    bool NeedUpdate(TInstant now) const;
    TIntrusiveVector<TEvInterconnect::TNodeInfo>::TConstPtr GetNodes() const;
    std::shared_ptr<const TEvInterconnect::TEvNodesInfo::TPileMap> GetPileMap() const;
private:
    TIntrusiveVector<TEvInterconnect::TNodeInfo>::TConstPtr Nodes;
    TInstant Expire;
    std::shared_ptr<const TEvInterconnect::TEvNodesInfo::TPileMap> PileMap;
};

template<typename TCacheMiss>
class TActorCacheMiss;
class TCacheMissGet;
class TCacheMissResolve;

enum class EProtocolState {
    Connecting,
    UseEpochProtocol,
    UseDeltaProtocol,
};

class TDynamicNameserver : public TActorBootstrapped<TDynamicNameserver> {
public:
    using TBase = TActorBootstrapped<TDynamicNameserver>;

    friend TActorCacheMiss<TCacheMissGet>;
    friend TActorCacheMiss<TCacheMissResolve>;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::NAMESERVICE;
    }

    struct TEvPrivate {
        enum EEv {
            EvUpdateEpoch = EventSpaceBegin(TEvents::ES_PRIVATE),

            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE)");

        struct TEvUpdateEpoch : public TEventLocal<TEvUpdateEpoch, EvUpdateEpoch> {
            TEvUpdateEpoch(ui32 domain, ui64 epoch)
                : Domain(domain)
                , Epoch(epoch)
            {
            }

            ui32 Domain;
            ui64 Epoch;
        };
    };

    TDynamicNameserver(const TIntrusivePtr<TTableNameserverSetup> &setup, ui32 resolvePoolId)
        : StaticConfig(setup)
        , ListNodesCache(MakeIntrusive<TListNodesCache>())
        , ResolvePoolId(resolvePoolId)
    {
        Y_ABORT_UNLESS(StaticConfig->IsEntriesUnique());

        for (size_t i = 0; i < DynamicConfigs.size(); ++i)
            DynamicConfigs[i] = new TDynamicConfig;
    }

    TDynamicNameserver(const TIntrusivePtr<TTableNameserverSetup> &setup,
                       const NKikimrNodeBroker::TNodeInfo &node,
                       const TDomainsInfo &domains,
                       ui32 resolvePoolId)
        : TDynamicNameserver(setup, resolvePoolId)
    {
        ui32 domain = domains.GetDomain()->DomainUid;
        TDynamicConfig::TDynamicNodeInfo info(node);
        DynamicConfigs[domain]->DynamicNodes.emplace(node.GetNodeId(), info);
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvInterconnect::TEvResolveNode, Handle);
            HFunc(TEvResolveAddress, Handle);
            HFunc(TEvInterconnect::TEvListNodes, Handle);
            HFunc(TEvInterconnect::TEvGetNode, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvNodeBroker::TEvNodesInfo, Handle);
            HFunc(TEvNodeBroker::TEvUpdateNodes, Handle);
            HFunc(TEvNodeBroker::TEvSyncNodesResponse, Handle)
            HFunc(TEvPrivate::TEvUpdateEpoch, Handle);
            HFunc(NMon::TEvHttpInfo, Handle);
            hFunc(TEvents::TEvUnsubscribe, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup);

            hFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse, Handle);
            hFunc(NConsole::TEvConfigsDispatcher::TEvRemoveConfigSubscriptionResponse, Handle);
            HFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, Handle);

            hFunc(TEvNodeWardenStorageConfig, Handle);
        }
    }

    void Bootstrap(const TActorContext &ctx);
    void Handle(TEvNodeWardenStorageConfig::TPtr ev);
    void Die(const TActorContext &ctx) override;

    size_t GetTotalPendingCacheMissesSize() const;

private:
    void OpenPipe(ui32 domain);
    void OpenPipe(TActorId& pipe);
    void RequestEpochUpdate(ui32 domain,
                            ui32 epoch,
                            const TActorContext &ctx);
    void ResolveStaticNode(ui32 nodeId, TActorId sender, TMonotonic deadline, const TActorContext &ctx);
    void ResolveDynamicNode(ui32 nodeId, TAutoPtr<IEventHandle> ev, TMonotonic deadline, const TActorContext &ctx);
    void SendNodesList(TActorId recipient, const TActorContext &ctx);
    void SendNodesList(const TActorContext &ctx);
    void PendingRequestAnswered(ui32 domain, const TActorContext &ctx);
    void UpdateState(const NKikimrNodeBroker::TNodesInfo &rec,
                     const TActorContext &ctx);

    void OnPipeDestroyed(ui32 domain,
                         const TActorContext &ctx);

    void UpdateCounters();

    void Handle(TEvInterconnect::TEvResolveNode::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvResolveAddress::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvInterconnect::TEvListNodes::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvInterconnect::TEvGetNode::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvNodeBroker::TEvNodesInfo::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvNodeBroker::TEvUpdateNodes::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvNodeBroker::TEvSyncNodesResponse::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvUpdateEpoch::TPtr &ev, const TActorContext &ctx);
    void Handle(NMon::TEvHttpInfo::TPtr &ev, const TActorContext &ctx);

    void Handle(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr ev);
    void Handle(NConsole::TEvConfigsDispatcher::TEvRemoveConfigSubscriptionResponse::TPtr ev);
    void Handle(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr ev, const TActorContext &ctx);

    void Handle(TEvents::TEvUnsubscribe::TPtr ev);
    void HandleWakeup(const TActorContext &ctx);

    void ReplaceNameserverSetup(TIntrusivePtr<TTableNameserverSetup> newStaticConfig);
    void RegisterNewCacheMiss(TCacheMiss* cacheMiss, TDynamicConfigPtr config);
    void SendSyncRequest(TActorId pipe, const TActorContext &ctx);

private:
    TIntrusivePtr<TTableNameserverSetup> StaticConfig;
    std::array<TDynamicConfigPtr, DOMAINS_COUNT> DynamicConfigs;
    TVector<TActorId> ListNodesQueue;
    TIntrusivePtr<TListNodesCache> ListNodesCache;
    TBridgeInfo::TPtr BridgeInfo;

    // When ListNodes requests are sent to NodeBroker tablets this
    // bitmap indicates domains which didn't answer yet.
    TBitMap<DOMAINS_COUNT> PendingRequests;
    // Domain -> Epoch ID.
    THashMap<ui32, ui64> EpochUpdates;
    ui32 ResolvePoolId;
    THashSet<TActorId> StaticNodeChangeSubscribers;
    bool SubscribedToConsoleNSConfig = false;

    bool EnableDeltaProtocol = false;
    EProtocolState ProtocolState = EProtocolState::Connecting;
    bool SyncInProgress = false;
    ui64 SyncCookie = 0;
    ui64 SeqNo = 0;

    ::NMonitoring::TDynamicCounters::TCounterPtr StaticNodesCounter;
    ::NMonitoring::TDynamicCounters::TCounterPtr ActiveDynamicNodesCounter;
    ::NMonitoring::TDynamicCounters::TCounterPtr ExpiredDynamicNodesCounter;
    ::NMonitoring::TDynamicCounters::TCounterPtr EpochVersionCounter;
};

} // NNodeBroker
} // NKikimr
