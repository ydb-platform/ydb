#pragma once

#include "dynamic_nameserver.h"
#include "node_broker.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/interconnect/events_local.h>
#include <ydb/library/actors/interconnect/interconnect_impl.h>
#include <ydb/library/actors/interconnect/interconnect_address.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>

#include <ydb/library/services/services.pb.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/bitmap.h>

namespace NKikimr {
namespace NNodeBroker {

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
    THashMap<ui32, TDynamicNodeInfo> ExpiredNodes;
    TEpochInfo Epoch;
};

using TDynamicConfigPtr = TIntrusivePtr<TDynamicConfig>;

class TListNodesCache : public TSimpleRefCount<TListNodesCache> {
public:
    TListNodesCache();

    void Update(TIntrusiveVector<TEvInterconnect::TNodeInfo>::TConstPtr newNodes, TInstant newExpire);
    void Invalidate();
    bool NeedUpdate(TInstant now) const;
    TIntrusiveVector<TEvInterconnect::TNodeInfo>::TConstPtr GetNodes() const;
private:
    TIntrusiveVector<TEvInterconnect::TNodeInfo>::TConstPtr Nodes;
    TInstant Expire;
};

class TDynamicNodeResolverBase : public TActorBootstrapped<TDynamicNodeResolverBase> {
public:
    using TBase = TActorBootstrapped<TDynamicNodeResolverBase>;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::NAMESERVICE;
    }

    TDynamicNodeResolverBase(TActorId owner, ui32 nodeId, TDynamicConfigPtr config,
                             TIntrusivePtr<TListNodesCache> listNodesCache,
                             TAutoPtr<IEventHandle> origRequest, TInstant deadline)
        : Owner(owner)
        , NodeId(nodeId)
        , Config(config)
        , ListNodesCache(listNodesCache)
        , OrigRequest(origRequest)
        , Deadline(deadline)
    {
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            CFunc(TEvTabletPipe::EvClientDestroyed, ReplyWithErrorAndDie);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvNodeBroker::TEvResolvedNode, Handle);
            CFunc(TEvents::TSystem::Wakeup, ReplyWithErrorAndDie);
        }
    }

    void Bootstrap(const TActorContext &ctx);
    void Die(const TActorContext &ctx) override;

    virtual void OnSuccess(const TActorContext &ctx)
    {
        Y_UNUSED(ctx);
    }

    virtual void OnError(const TActorContext &ctx)
    {
        Y_UNUSED(ctx);
    }

private:
    void ReplyWithErrorAndDie(const TActorContext &ctx);
    void Handle(TEvNodeBroker::TEvResolvedNode::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx);

protected:
    TActorId Owner;
    ui32 NodeId;
    TDynamicConfigPtr Config;
    TIntrusivePtr<TListNodesCache> ListNodesCache;
    TAutoPtr<IEventHandle> OrigRequest;
    const TInstant Deadline;

private:
    TActorId NodeBrokerPipe;
};

class TDynamicNodeResolver : public TDynamicNodeResolverBase {
public:
    TDynamicNodeResolver(TActorId owner, ui32 nodeId, TDynamicConfigPtr config,
                         TIntrusivePtr<TListNodesCache> listNodesCache,
                         TAutoPtr<IEventHandle> origRequest, TInstant deadline)
        : TDynamicNodeResolverBase(owner, nodeId, config, listNodesCache, origRequest, deadline)
    {
    }

    void OnSuccess(const TActorContext &ctx) override;
    void OnError(const TActorContext &ctx) override;
};

class TDynamicNodeSearcher : public TDynamicNodeResolverBase {
public:
    TDynamicNodeSearcher(TActorId owner, ui32 nodeId, TDynamicConfigPtr config,
                         TIntrusivePtr<TListNodesCache> listNodesCache,
                         TAutoPtr<IEventHandle> origRequest, TInstant deadline)
        : TDynamicNodeResolverBase(owner, nodeId, config, listNodesCache, origRequest, deadline)
    {
    }

    void OnSuccess(const TActorContext &ctx) override;
    void OnError(const TActorContext &ctx) override;
};

class TDynamicNameserver : public TActorBootstrapped<TDynamicNameserver> {
public:
    using TBase = TActorBootstrapped<TDynamicNameserver>;

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
            HFunc(TEvPrivate::TEvUpdateEpoch, Handle);
            HFunc(NMon::TEvHttpInfo, Handle);
            hFunc(TEvents::TEvUnsubscribe, Handle);

            hFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse, Handle);
            hFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, Handle);
        }
    }

    void Bootstrap(const TActorContext &ctx);
    void Die(const TActorContext &ctx) override;

private:
    void OpenPipe(ui32 domain,
                  const TActorContext &ctx);
    void RequestEpochUpdate(ui32 domain,
                            ui32 epoch,
                            const TActorContext &ctx);
    void ResolveStaticNode(ui32 nodeId, TActorId sender, TInstant deadline, const TActorContext &ctx);
    void ResolveDynamicNode(ui32 nodeId, TAutoPtr<IEventHandle> ev, TInstant deadline, const TActorContext &ctx);
    void SendNodesList(const TActorContext &ctx);
    void PendingRequestAnswered(ui32 domain, const TActorContext &ctx);
    void UpdateState(const NKikimrNodeBroker::TNodesInfo &rec,
                     const TActorContext &ctx);

    void OnPipeDestroyed(ui32 domain,
                         const TActorContext &ctx);

    void Handle(TEvInterconnect::TEvResolveNode::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvResolveAddress::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvInterconnect::TEvListNodes::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvInterconnect::TEvGetNode::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvNodeBroker::TEvNodesInfo::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvUpdateEpoch::TPtr &ev, const TActorContext &ctx);
    void Handle(NMon::TEvHttpInfo::TPtr &ev, const TActorContext &ctx);

    void Handle(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr ev);
    void Handle(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr ev);

    void Handle(TEvents::TEvUnsubscribe::TPtr ev);

private:
    TIntrusivePtr<TTableNameserverSetup> StaticConfig;
    std::array<TDynamicConfigPtr, DOMAINS_COUNT> DynamicConfigs;
    TVector<TActorId> ListNodesQueue;
    TIntrusivePtr<TListNodesCache> ListNodesCache;

    std::array<TActorId, DOMAINS_COUNT> NodeBrokerPipes;
    // When ListNodes requests are sent to NodeBroker tablets this
    // bitmap indicates domains which didn't answer yet.
    TBitMap<DOMAINS_COUNT> PendingRequests;
    // Domain -> Epoch ID.
    THashMap<ui32, ui64> EpochUpdates;
    ui32 ResolvePoolId;
    THashSet<TActorId> StaticNodeChangeSubscribers;
};

} // NNodeBroker
} // NKikimr
