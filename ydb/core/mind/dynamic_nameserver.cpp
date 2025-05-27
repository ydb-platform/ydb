#include "dynamic_nameserver_impl.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/nameservice.h>
#include <ydb/core/mon/mon.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/protos/blobstorage_distributed_config.pb.h>

namespace NKikimr {
namespace NNodeBroker {

static void ResetInterconnectProxyConfig(ui32 nodeId, const TActorContext &ctx)
{
    auto aid = TActivationContext::InterconnectProxy(nodeId);
    if (!aid)
        return;
    ctx.Send(aid, new TEvInterconnect::TEvDisconnect);
}

template<typename TCacheMiss>
class TActorCacheMiss : public TActor<TActorCacheMiss<TCacheMiss>>, public TCacheMiss {
public:
    using TThis = TActorCacheMiss<TCacheMiss>;
    using TActorBase = TActor<TThis>;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::NAMESERVICE;
    }

    TActorCacheMiss(TDynamicNameserver* owner, ui32 nodeId, TDynamicConfigPtr config,
                    TIntrusivePtr<TListNodesCache> listNodesCache,
                    TAutoPtr<IEventHandle> origRequest, TMonotonic deadline)
        : TActorBase(&TThis::StateWork)
        , TCacheMiss(nodeId, config, origRequest, deadline, 0)
        , Owner(owner)
        , ListNodesCache(listNodesCache)
    {
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvNodeBroker::TEvResolvedNode, Handle);
        }
    }

    void SendRequest() {
        Owner->OpenPipe(Config->NodeBrokerPipe);
        TAutoPtr<TEvNodeBroker::TEvResolveNode> request = new TEvNodeBroker::TEvResolveNode;
        request->Record.SetNodeId(NodeId);
        NTabletPipe::SendData(TActorBase::SelfId(), Config->NodeBrokerPipe, request.Release());
    }

    void OnSuccess(const TActorContext &ctx) override {
        TCacheMiss::OnSuccess(ctx);
        TActorBase::PassAway();
    }

    void OnError(const TString& error, const TActorContext &ctx) override {
        TCacheMiss::OnError(error, ctx);
        TActorBase::PassAway();
    }

    void ConvertToActor(TDynamicNameserver*, TIntrusivePtr<TListNodesCache>, const TActorContext &) override {}

private:
    using TCacheMiss::Config;
    using TCacheMiss::NodeId;

    void Handle(TEvNodeBroker::TEvResolvedNode::TPtr &ev, const TActorContext &ctx) {
        LOG_D("Handle " << ev->Get()->ToString());

        if (Owner->ProtocolState != EProtocolState::UseEpochProtocol) {
            return;
        }

        Config->PendingCacheMisses.Remove(this);

        TDynamicConfig::TDynamicNodeInfo oldNode;
        auto it = Config->DynamicNodes.find(NodeId);
        bool exists = it != Config->DynamicNodes.end();

        if (exists) {
            oldNode = it->second;
            this->Config->DynamicNodes.erase(it);
        }

        auto &rec = ev->Get()->Record;
        if (rec.GetStatus().GetCode() != NKikimrNodeBroker::TStatus::OK) {
            // Reset proxy if node expired.
            if (exists) {
                ResetInterconnectProxyConfig(NodeId, ctx);
                ListNodesCache->Invalidate(); // node was erased
            }
            OnError(rec.GetStatus().GetReason(), ctx);
            return;
        }

        TDynamicConfig::TDynamicNodeInfo node(rec.GetNode());
        if (!exists || !oldNode.EqualExceptExpire(node)) {
            ListNodesCache->Invalidate();
        }

        // If ID is re-used by another node then proxy has to be reset.
        if (exists && !oldNode.EqualExceptExpire(node))
            ResetInterconnectProxyConfig(NodeId, ctx);
        Config->DynamicNodes.emplace(NodeId, node);

        OnSuccess(ctx);
    }

    TDynamicNameserver* Owner;
    TIntrusivePtr<TListNodesCache> ListNodesCache;
};

TCacheMiss::TCacheMiss(ui32 nodeId, TDynamicConfigPtr config, TAutoPtr<IEventHandle> origRequest,
                       TMonotonic deadline, ui32 syncCookie)
    : NodeId(nodeId)
    , Deadline(deadline)
    , NeedScheduleDeadline(Deadline != TMonotonic::Max())
    , SyncCookie(syncCookie)
    , Config(config)
    , OrigRequest(origRequest)
{
}

void TCacheMiss::OnSuccess(const TActorContext &) {
    LOG_D("Cache miss succeed"
        << ": nodeId=" << NodeId);
}

void TCacheMiss::OnError(const TString &error, const TActorContext &) {
    LOG_D("Cache miss failed"
        << ": nodeId=" << NodeId
        << ", error=" << error);
}

size_t& TCacheMiss::THeapIndexByDeadline::operator()(TCacheMiss& cacheMiss) const {
    return cacheMiss.DeadlineHeapIndex;
}

bool TCacheMiss::TCompareByDeadline::operator()(const TCacheMiss& a, const TCacheMiss& b) const {
    return a.Deadline < b.Deadline;
}

class TCacheMissGet : public TCacheMiss {
public:
    using TBase = TCacheMiss;

    TCacheMissGet(ui32 nodeId, TDynamicConfigPtr config, TAutoPtr<IEventHandle> origRequest,
                  TMonotonic deadline, ui32 syncCookie)
        : TCacheMiss(nodeId, config, origRequest, deadline, syncCookie)
    {
    }

    void OnSuccess(const TActorContext &ctx) override {
        TBase::OnSuccess(ctx);

        THolder<TEvInterconnect::TEvNodeInfo> reply(new TEvInterconnect::TEvNodeInfo(NodeId));
        auto it = Config->DynamicNodes.find(NodeId);
        if (it != Config->DynamicNodes.end())
            reply->Node = MakeHolder<TEvInterconnect::TNodeInfo>(it->first, it->second.Address,
                                                         it->second.Host, it->second.ResolveHost,
                                                         it->second.Port, it->second.Location);
        ctx.Send(OrigRequest->Sender, reply.Release());
    }

    void OnError(const TString &error, const TActorContext &ctx) override {
        TBase::OnError(error, ctx);

        THolder<TEvInterconnect::TEvNodeInfo> reply(new TEvInterconnect::TEvNodeInfo(NodeId));
        ctx.Send(OrigRequest->Sender, reply.Release());
    }

    void ConvertToActor(TDynamicNameserver* owner, TIntrusivePtr<TListNodesCache> listNodesCache,
                        const TActorContext &ctx) override {
        Config->PendingCacheMisses.Remove(this);

        auto* actor = new TActorCacheMiss<TCacheMissGet>(owner, NodeId, Config, listNodesCache, OrigRequest, Deadline);
        actor->NeedScheduleDeadline = NeedScheduleDeadline;
        ctx.RegisterWithSameMailbox(actor);
        actor->SendRequest();

        Config->PendingCacheMisses.Add(actor);
    }
};

class TCacheMissResolve : public TCacheMiss {
public:
    using TBase = TCacheMiss;

    TCacheMissResolve(ui32 nodeId, TDynamicConfigPtr config, TAutoPtr<IEventHandle> origRequest,
                      TMonotonic deadline, ui32 syncCookie)
        : TCacheMiss(nodeId, config, origRequest, deadline, syncCookie)
    {
    }

    void OnSuccess(const TActorContext &ctx) override {
        TBase::OnSuccess(ctx);

        ctx.Send(OrigRequest);
    }

    void OnError(const TString &error, const TActorContext &ctx) override {
        TBase::OnError(error, ctx);

        auto reply = new TEvLocalNodeInfo;
        reply->NodeId = NodeId;
        ctx.Send(OrigRequest->Sender, reply);
    }

    void ConvertToActor(TDynamicNameserver* owner, TIntrusivePtr<TListNodesCache> listNodesCache,
                        const TActorContext &ctx) override {
        Config->PendingCacheMisses.Remove(this);

        auto* actor = new TActorCacheMiss<TCacheMissResolve>(owner, NodeId, Config, listNodesCache, OrigRequest, Deadline);
        actor->NeedScheduleDeadline = NeedScheduleDeadline;
        ctx.RegisterWithSameMailbox(actor);
        actor->SendRequest();

        Config->PendingCacheMisses.Add(actor);
    }
};

void TDynamicNameserver::Bootstrap(const TActorContext &ctx)
{
    NActors::TMon* mon = AppData(ctx)->Mon;
    if (mon) {
        NMonitoring::TIndexMonPage *actorsMonPage = mon->RegisterIndexPage("actors", "Actors");
        mon->RegisterActorPage(actorsMonPage, "dnameserver", "Dynamic nameserver",
                               false, ctx.ActorSystem(), ctx.SelfID);
    }

    EnableDeltaProtocol = AppData()->FeatureFlags.GetEnableNodeBrokerDeltaProtocol();
    ui32 featureFlagsItem = NKikimrConsole::TConfigItem::FeatureFlagsItem;
    Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()),
        new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest(featureFlagsItem));

    auto dinfo = AppData(ctx)->DomainsInfo;
    if (const auto& domain = dinfo->Domain) {
        OpenPipe(domain->DomainUid);
    }

    Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), new TEvNodeWardenQueryStorageConfig(true));

    Become(&TDynamicNameserver::StateFunc);
}

void TDynamicNameserver::Handle(TEvNodeWardenStorageConfig::TPtr ev) {
    Y_ABORT_UNLESS(ev->Get()->Config);
    const auto& config = *ev->Get()->Config;

    if (ev->Get()->SelfManagementEnabled) {
        // self-management through distconf is enabled and we are operating based on their tables, so apply them now
        ReplaceNameserverSetup(BuildNameserverTable(config));

        // unsubscribe from console nameservice config if we were operating without self-management before
        if (std::exchange(SubscribedToConsoleNSConfig, false)) {
            auto unsubscribeAllQuery = std::make_unique<NConsole::TEvConfigsDispatcher::TEvRemoveConfigSubscriptionRequest>(SelfId());
            Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()), unsubscribeAllQuery.release());

            auto subscribeFFsQuery = std::make_unique<NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest>(
                NKikimrConsole::TConfigItem::FeatureFlagsItem,
                SelfId()
            );
            Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()), subscribeFFsQuery.release());
        }
    } else if (!std::exchange(SubscribedToConsoleNSConfig, true)) {
        auto subscribeNSQuery = std::make_unique<NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest>(
            NKikimrConsole::TConfigItem::NameserviceConfigItem,
            SelfId()
        );
        Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()), subscribeNSQuery.release());
    }
}

void TDynamicNameserver::ReplaceNameserverSetup(TIntrusivePtr<TTableNameserverSetup> newStaticConfig) {
    if (StaticConfig->StaticNodeTable != newStaticConfig->StaticNodeTable) {
        StaticConfig = std::move(newStaticConfig);
        ListNodesCache->Invalidate();
        for (const auto& subscriber : StaticNodeChangeSubscribers) {
            TActivationContext::Send(new IEventHandle(SelfId(), subscriber, new TEvInterconnect::TEvListNodes));
        }
    }
}

void TDynamicNameserver::Die(const TActorContext &ctx)
{
    for (auto &config : DynamicConfigs) {
        if (config->NodeBrokerPipe)
            NTabletPipe::CloseClient(ctx, config->NodeBrokerPipe);
    }
    TBase::Die(ctx);
}

void TDynamicNameserver::OpenPipe(ui32 domain)
{
    OpenPipe(DynamicConfigs[domain]->NodeBrokerPipe);
}

void TDynamicNameserver::OpenPipe(TActorId& pipe)
{
    if (!pipe) {
        NTabletPipe::TClientConfig clientConfig;
        clientConfig.RetryPolicy = {
            .MinRetryTime = TDuration::MilliSeconds(100),
            .MaxRetryTime = TDuration::Seconds(1),
            .DoFirstRetryInstantly = true
        };
        pipe = RegisterWithSameMailbox(NTabletPipe::CreateClient(SelfId(), MakeNodeBrokerID(), clientConfig));
    }
}

size_t TDynamicNameserver::GetTotalPendingCacheMissesSize() const {
    size_t total = 0;
    for (const auto &config : DynamicConfigs) {
        total += config->PendingCacheMisses.Size() + config->CacheMissHolders.size();
    }
    return total;
}

void TDynamicNameserver::RequestEpochUpdate(ui32 domain,
                                            ui32 epoch,
                                            const TActorContext &ctx)
{
    OpenPipe(domain);

    TAutoPtr<TEvNodeBroker::TEvListNodes> request = new TEvNodeBroker::TEvListNodes;
    request->Record.SetMinEpoch(epoch);
    NTabletPipe::SendData(ctx, DynamicConfigs[domain]->NodeBrokerPipe, request.Release());
    EpochUpdates[domain] = epoch;
}

void TDynamicNameserver::ResolveStaticNode(ui32 nodeId, TActorId sender, TMonotonic deadline, const TActorContext &ctx)
{
    auto it = StaticConfig->StaticNodeTable.find(nodeId);

    if (it == StaticConfig->StaticNodeTable.end()) {
        auto reply = new TEvLocalNodeInfo;
        reply->NodeId = nodeId;
        ctx.Send(sender, reply);
        return;
    }

    RegisterWithSameMailbox(CreateResolveActor(it->second.ResolveHost, it->second.Port, nodeId, it->second.Address, sender, SelfId(), deadline));
}

void TDynamicNameserver::ResolveDynamicNode(ui32 nodeId,
                                            TAutoPtr<IEventHandle> ev,
                                            TMonotonic deadline,
                                            const TActorContext &ctx)
{
    ui32 domain = AppData()->DomainsInfo->GetDomain()->DomainUid;
    const auto& config = DynamicConfigs[domain];
    auto it = config->DynamicNodes.find(nodeId);

    if (it != config->DynamicNodes.end()
        && it->second.Expire > ctx.Now())
    {
        RegisterWithSameMailbox(CreateResolveActor(it->second.ResolveHost, it->second.Port, nodeId, it->second.Address, ev->Sender, SelfId(), deadline));
    } else if (config->ExpiredNodes.contains(nodeId)
                && ctx.Now() < config->Epoch.End) {
        auto reply = new TEvLocalNodeInfo;
        reply->NodeId = nodeId;
        ctx.Send(ev->Sender, reply);
    } else {
        TCacheMiss* cacheMiss;
        if (ProtocolState == EProtocolState::UseEpochProtocol) {
            auto* actor = new TActorCacheMiss<TCacheMissResolve>(this, nodeId, config, ListNodesCache, ev, deadline);
            cacheMiss = actor;
            RegisterWithSameMailbox(actor);
            actor->SendRequest();
        } else {
            auto holder = MakeHolder<TCacheMissResolve>(nodeId, config, ev, deadline, SyncCookie + 1);
            cacheMiss = holder.get();
            config->CacheMissHolders.emplace(cacheMiss, std::move(holder));

            if (ProtocolState == EProtocolState::UseDeltaProtocol) {
                SendSyncRequest(config->NodeBrokerPipe, ctx);
            }
        }
        RegisterNewCacheMiss(cacheMiss, config);
    }
}

void TDynamicNameserver::SendNodesList(TActorId recipient, const TActorContext &ctx)
{
    auto now = ctx.Now();
    if (ListNodesCache->NeedUpdate(now)) {
        auto newNodes = MakeIntrusive<TIntrusiveVector<TEvInterconnect::TNodeInfo>>();
        auto newExpire = TInstant::Max();

        for (const auto &pr : StaticConfig->StaticNodeTable) {
            newNodes->emplace_back(pr.first,
                                   pr.second.Address, pr.second.Host, pr.second.ResolveHost,
                                   pr.second.Port, pr.second.Location, true);
        }

        for (auto &config : DynamicConfigs) {
            for (auto &pr : config->DynamicNodes) {
                if (pr.second.Expire > now) {
                    newNodes->emplace_back(pr.first, pr.second.Address,
                                           pr.second.Host, pr.second.ResolveHost,
                                           pr.second.Port, pr.second.Location, false);
                    newExpire = std::min(newExpire, pr.second.Expire);
                }
            }
        }

        ListNodesCache->Update(newNodes, newExpire);
    }

    ctx.Send(recipient, new TEvInterconnect::TEvNodesInfo(ListNodesCache->GetNodes()));
}

void TDynamicNameserver::SendNodesList(const TActorContext &ctx)
{
    for (auto &sender : ListNodesQueue) {
        SendNodesList(sender, ctx);
    }
    ListNodesQueue.clear();
}

void TDynamicNameserver::PendingRequestAnswered(ui32 domain, const TActorContext &ctx)
{
    PendingRequests.Reset(domain);
    if (PendingRequests.Empty())
        SendNodesList(ctx);
}

void TDynamicNameserver::UpdateState(const NKikimrNodeBroker::TNodesInfo &rec,
                                     const TActorContext &ctx)
{
    ui32 domain = rec.GetDomain();
    auto &config = DynamicConfigs[domain];

    if (rec.GetEpoch().GetVersion() <= config->Epoch.Version)
        return;

    // In case of new epoch we need to fully update state.
    // Otherwise only add new nodes.
    if (rec.GetEpoch().GetId() > config->Epoch.Id) {
        THashSet<ui32> toRemove;
        for (auto &pr : config->DynamicNodes)
            toRemove.insert(pr.first);

        for (auto &node : rec.GetNodes()) {
            auto nodeId = node.GetNodeId();

            toRemove.erase(nodeId);

            TDynamicConfig::TDynamicNodeInfo info(node);
            auto it = config->DynamicNodes.find(nodeId);
            if (it == config->DynamicNodes.end()) {
                config->DynamicNodes.emplace(nodeId, info);
            } else {
                if (it->second.EqualExceptExpire(info)) {
                    it->second.Expire = info.Expire;
                } else {
                    ResetInterconnectProxyConfig(nodeId, ctx);
                    it->second = info;
                }
            }
        }

        for (auto id : toRemove)
            config->DynamicNodes.erase(id);

        config->ExpiredNodes.clear();
        for (auto &node : rec.GetExpiredNodes()) {
            config->ExpiredNodes.emplace(node.GetNodeId());
        }

        ListNodesCache->Invalidate();
        config->Epoch = rec.GetEpoch();
        ctx.Schedule(config->Epoch.End - ctx.Now(),
                     new TEvPrivate::TEvUpdateEpoch(domain, config->Epoch.Id + 1));
    } else {
        // Note: this update may be optimized to only include new or updated nodes
        for (auto &node : rec.GetNodes()) {
            auto nodeId = node.GetNodeId();

            TDynamicConfig::TDynamicNodeInfo info(node);
            auto it = config->DynamicNodes.find(nodeId);
            if (it == config->DynamicNodes.end()) {
                config->DynamicNodes.emplace(nodeId, info);
            } else {
                if (it->second.EqualExceptExpire(info)) {
                    it->second.Expire = info.Expire;
                } else {
                    ResetInterconnectProxyConfig(nodeId, ctx);
                    it->second = info;
                }
            }
            ListNodesCache->Invalidate();
        }
        config->Epoch = rec.GetEpoch();
    }
}

void TDynamicNameserver::OnPipeDestroyed(ui32 domain, const TActorContext &ctx)
{
    DynamicConfigs[domain]->NodeBrokerPipe = TActorId();
    ++SeqNo; // ignore everything that may come from old pipe
    ProtocolState = EProtocolState::Connecting;
    PendingRequestAnswered(domain, ctx);

    if (EpochUpdates.contains(domain)) {
        EpochUpdates.erase(domain);
    }

    while (auto* cacheMiss = DynamicConfigs[domain]->PendingCacheMisses.Top()) {
        DynamicConfigs[domain]->PendingCacheMisses.Remove(cacheMiss);
        cacheMiss->OnError("Pipe was destroyed", ctx);
    }
    DynamicConfigs[domain]->CacheMissHolders.clear();

    SyncInProgress = false;

    OpenPipe(DynamicConfigs[domain]->NodeBrokerPipe);
}

void TDynamicNameserver::Handle(TEvInterconnect::TEvResolveNode::TPtr &ev,
                                const TActorContext &ctx)
{
    LOG_D("Handle " << ev->Get()->ToString());
    const ui32 nodeId = ev->Get()->NodeId;
    const TMonotonic deadline = ev->Get()->Deadline;
    auto config = AppData(ctx)->DynamicNameserviceConfig;

    if (!config || nodeId <= config->MaxStaticNodeId)
        ResolveStaticNode(nodeId, ev->Sender, deadline, ctx);
    else
        ResolveDynamicNode(nodeId, ev.Release(), deadline, ctx);
}

void TDynamicNameserver::Handle(TEvResolveAddress::TPtr &ev, const TActorContext &ctx) {
    Y_UNUSED(ctx);

    const TEvResolveAddress* request = ev->Get();

    RegisterWithSameMailbox(CreateResolveActor(request->Address, request->Port, ev->Sender, SelfId(), TMonotonic::Max()));
}

void TDynamicNameserver::Handle(TEvInterconnect::TEvListNodes::TPtr &ev,
                                const TActorContext &ctx)
{
    LOG_D("Handle " << ev->Get()->ToString());

    if (ProtocolState == EProtocolState::Connecting) {
        SendNodesList(ev->Sender, ctx);
    } else {
        if (ListNodesQueue.empty()) {
            auto dinfo = AppData(ctx)->DomainsInfo;
            if (const auto& d = dinfo->Domain) {
                ui32 domain = d->DomainUid;
                if (ProtocolState == EProtocolState::UseEpochProtocol) {
                    TAutoPtr<TEvNodeBroker::TEvListNodes> request = new TEvNodeBroker::TEvListNodes;
                    request->Record.SetCachedVersion(DynamicConfigs[domain]->Epoch.Version);
                    NTabletPipe::SendData(ctx, DynamicConfigs[domain]->NodeBrokerPipe, request.Release());
                } else if (ProtocolState == EProtocolState::UseDeltaProtocol) {
                    SendSyncRequest(DynamicConfigs[domain]->NodeBrokerPipe, ctx);
                }
                PendingRequests.Set(domain);
            }
        }
        ListNodesQueue.push_back(ev->Sender);
    }

    if (ev->Get()->SubscribeToStaticNodeChanges) {
        StaticNodeChangeSubscribers.insert(ev->Sender);
    }
}

void TDynamicNameserver::Handle(TEvInterconnect::TEvGetNode::TPtr &ev, const TActorContext &ctx)
{
    LOG_D("Handle " << ev->Get()->ToString());

    ui32 nodeId = ev->Get()->NodeId;
    THolder<TEvInterconnect::TEvNodeInfo> reply(new TEvInterconnect::TEvNodeInfo(nodeId));
    auto config = AppData(ctx)->DynamicNameserviceConfig;

    if (!config || nodeId <= config->MaxStaticNodeId) {
        auto it = StaticConfig->StaticNodeTable.find(nodeId);
        if (it != StaticConfig->StaticNodeTable.end())
            reply->Node = MakeHolder<TEvInterconnect::TNodeInfo>(it->first, it->second.Address,
                                                         it->second.Host, it->second.ResolveHost,
                                                         it->second.Port, it->second.Location);
        ctx.Send(ev->Sender, reply.Release());
    } else {
        ui32 domain = AppData()->DomainsInfo->GetDomain()->DomainUid;
        const auto& config = DynamicConfigs[domain];
        auto it = config->DynamicNodes.find(nodeId);
        if (it != config->DynamicNodes.end() && it->second.Expire > ctx.Now()) {
            reply->Node = MakeHolder<TEvInterconnect::TNodeInfo>(it->first, it->second.Address,
                                                         it->second.Host, it->second.ResolveHost,
                                                         it->second.Port, it->second.Location);
            ctx.Send(ev->Sender, reply.Release());
        } else if (config->ExpiredNodes.contains(nodeId)
                   && ctx.Now() < config->Epoch.End) {
            ctx.Send(ev->Sender, reply.Release());
        } else {
            TCacheMiss* cacheMiss;
            const TMonotonic deadline = ev->Get()->Deadline;
            if (ProtocolState == EProtocolState::UseEpochProtocol) {
                auto* actor = new TActorCacheMiss<TCacheMissGet>(this, nodeId, config, ListNodesCache, ev.Release(), deadline);
                cacheMiss = actor;
                RegisterWithSameMailbox(actor);
                actor->SendRequest();
            } else {
                auto holder = MakeHolder<TCacheMissGet>(nodeId, config, ev.Release(), deadline, SyncCookie + 1);
                cacheMiss = holder.get();
                config->CacheMissHolders.emplace(cacheMiss, std::move(holder));

                if (ProtocolState == EProtocolState::UseDeltaProtocol) {
                    SendSyncRequest(config->NodeBrokerPipe, ctx);
                }
            }
            RegisterNewCacheMiss(cacheMiss, config);
        }
    }
}

void TDynamicNameserver::RegisterNewCacheMiss(TCacheMiss* cacheMiss, TDynamicConfigPtr config) {
    LOG_D("New cache miss"
        << ": nodeId# " << cacheMiss->NodeId
        << ", deadline# " << cacheMiss->Deadline);
    
    bool newEarliestDeadline = config->PendingCacheMisses.Empty() || config->PendingCacheMisses.Top()->Deadline > cacheMiss->Deadline;
    if (cacheMiss->NeedScheduleDeadline && newEarliestDeadline) {
        LOG_D("Schedule wakeup for new earliest deadline " << cacheMiss->Deadline);
        Schedule(cacheMiss->Deadline, new TEvents::TEvWakeup);
        cacheMiss->NeedScheduleDeadline = false;
    }
    config->PendingCacheMisses.Add(cacheMiss);
};

void TDynamicNameserver::SendSyncRequest(TActorId pipe, const TActorContext &ctx)
{
    if (!SyncInProgress) {
        auto request = MakeHolder<TEvNodeBroker::TEvSyncNodesRequest>();
        request->Record.SetSeqNo(SeqNo);
        NTabletPipe::SendData(ctx, pipe, request.Release(), ++SyncCookie);
        SyncInProgress = true;
    }
}

void TDynamicNameserver::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr &ev, const TActorContext &ctx)
{
    LOG_D("Handle " << ev->Get()->ToString());
    ui32 domain = AppData()->DomainsInfo->GetDomain()->DomainUid;
    if (DynamicConfigs[domain]->NodeBrokerPipe == ev->Get()->ClientId)
        OnPipeDestroyed(domain, ctx);
}

void TDynamicNameserver::Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx)
{
    LOG_D("Handle " << ev->Get()->ToString());

    ui32 domain = AppData(ctx)->DomainsInfo->GetDomain()->DomainUid;
    if (DynamicConfigs[domain]->NodeBrokerPipe != ev->Get()->ClientId) {
        return;
    }

    if (ev->Get()->Status != NKikimrProto::OK) {
        NTabletPipe::CloseClient(ctx, DynamicConfigs[domain]->NodeBrokerPipe);
        OnPipeDestroyed(domain, ctx);
    } else {
        NKikimrNodeBroker::TVersionInfo versionInfo;
        Y_PROTOBUF_SUPPRESS_NODISCARD versionInfo.ParseFromString(ev->Get()->VersionInfo);

        if (EnableDeltaProtocol && versionInfo.GetSupportDeltaProtocol()) {
            ProtocolState = EProtocolState::UseDeltaProtocol;

            auto request = MakeHolder<TEvNodeBroker::TEvSubscribeNodesRequest>();
            request->Record.SetSeqNo(SeqNo);
            request->Record.SetCachedVersion(DynamicConfigs[domain]->Epoch.Version);
            NTabletPipe::SendData(ctx, DynamicConfigs[domain]->NodeBrokerPipe, request.Release());

            if (!ListNodesQueue.empty() || !DynamicConfigs[domain]->PendingCacheMisses.Empty()) {
                SendSyncRequest(DynamicConfigs[domain]->NodeBrokerPipe, ctx);
            }
        } else {
            ProtocolState = EProtocolState::UseEpochProtocol;

            ui64 epoch = DynamicConfigs[domain]->Epoch.Id + 1;
            RequestEpochUpdate(domain, epoch, ctx);

            if (!ListNodesQueue.empty()) {
                auto request = MakeHolder<TEvNodeBroker::TEvListNodes>();
                request->Record.SetCachedVersion(DynamicConfigs[domain]->Epoch.Version);
                NTabletPipe::SendData(ctx, DynamicConfigs[domain]->NodeBrokerPipe, request.Release());
            }

            for (const auto &[cacheMiss, _] : DynamicConfigs[domain]->CacheMissHolders) {
                cacheMiss->ConvertToActor(this, ListNodesCache, ctx);
            }
            // cache misses are managed by actorsystem in epoch protocol
            DynamicConfigs[domain]->CacheMissHolders.clear();
        }
    }
}

void TDynamicNameserver::Handle(TEvNodeBroker::TEvNodesInfo::TPtr &ev, const TActorContext &ctx)
{
    auto &rec = ev->Get()->GetRecord();
    Y_ABORT_UNLESS(rec.HasDomain());
    ui32 domain = rec.GetDomain();

    if (ProtocolState != EProtocolState::UseEpochProtocol) {
        return;
    }

    if (rec.GetEpoch().GetVersion() != DynamicConfigs[domain]->Epoch.Version)
        UpdateState(rec, ctx);

    if (EpochUpdates.contains(domain) && EpochUpdates.at(domain) <= rec.GetEpoch().GetId())
        EpochUpdates.erase(domain);

    PendingRequestAnswered(rec.GetDomain(), ctx);
}

void TDynamicNameserver::Handle(TEvNodeBroker::TEvUpdateNodes::TPtr &ev, const TActorContext &ctx)
{
    LOG_D("Handle " << ev->Get()->ToString());

    auto &rec = ev->Get()->GetRecord();
    if (rec.GetSeqNo() != SeqNo) {
        return;
    }

    if (ProtocolState != EProtocolState::UseDeltaProtocol) {
        return;
    }

    ui32 domain = AppData()->DomainsInfo->GetDomain()->DomainUid;
    auto &config = DynamicConfigs[domain];

    if (rec.GetEpoch().GetVersion() <= config->Epoch.Version)
        return;

    config->Epoch = rec.GetEpoch();
    for (const auto& u : rec.GetUpdates()) {
        switch (u.GetUpdateTypeCase()) {
            case NKikimrNodeBroker::TUpdateNode::kNode: {
                const auto& node = u.GetNode();
                ui32 nodeId = node.GetNodeId();
                TDynamicConfig::TDynamicNodeInfo info(node);
                auto it = config->DynamicNodes.find(nodeId);
                if (it == config->DynamicNodes.end()) {
                    config->DynamicNodes.emplace(nodeId, info);
                } else {
                    if (it->second.EqualExceptExpire(info)) {
                        it->second.Expire = info.Expire;
                    } else {
                        ResetInterconnectProxyConfig(nodeId, ctx);
                        it->second = info;
                    }
                }
                ListNodesCache->Invalidate();
                config->ExpiredNodes.erase(nodeId);
                break;
            }
            case NKikimrNodeBroker::TUpdateNode::kExpiredNode: {
                ui32 nodeId = u.GetExpiredNode();
                if (config->DynamicNodes.erase(nodeId) > 0) {
                    ResetInterconnectProxyConfig(nodeId, ctx);
                    ListNodesCache->Invalidate();
                }
                config->ExpiredNodes.insert(nodeId);
                break;
            }
            case NKikimrNodeBroker::TUpdateNode::kRemovedNode: {
                ui32 nodeId = u.GetRemovedNode();
                if (config->DynamicNodes.erase(nodeId) > 0) {
                    ResetInterconnectProxyConfig(nodeId, ctx);
                    ListNodesCache->Invalidate();
                }
                config->ExpiredNodes.erase(nodeId);
                break;
            }
            case NKikimrNodeBroker::TUpdateNode::UPDATETYPE_NOT_SET:
                break;
        }
    }
}

void TDynamicNameserver::Handle(TEvNodeBroker::TEvSyncNodesResponse::TPtr &ev, const TActorContext &ctx)
{
    LOG_D("Handle " << ev->Get()->ToString());

    auto &rec = ev->Get()->Record;
    if (rec.GetSeqNo() != SeqNo) {
        return;
    }

    if (ProtocolState != EProtocolState::UseDeltaProtocol) {
        return;
    }

    SyncInProgress = false;

    ui32 domain = AppData()->DomainsInfo->GetDomain()->DomainUid;
    PendingRequestAnswered(domain, ctx);

    ui64 syncCookie = ev->Cookie;
    const auto& config = DynamicConfigs[domain];
    for (auto it = config->CacheMissHolders.begin(); it != config->CacheMissHolders.end();) {
        const auto& cacheMiss = it->first;
        if (config->DynamicNodes.contains(cacheMiss->NodeId)) {
            cacheMiss->OnSuccess(ctx);
            config->PendingCacheMisses.Remove(cacheMiss);
            it = config->CacheMissHolders.erase(it);
        } else if (syncCookie >= cacheMiss->SyncCookie) {
            cacheMiss->OnError("Unknown node", ctx);
            config->PendingCacheMisses.Remove(cacheMiss);
            it = config->CacheMissHolders.erase(it);
        } else {
            ++it;
        }
    }

    if (!config->PendingCacheMisses.Empty() && config->PendingCacheMisses.Top()->NeedScheduleDeadline) {
        auto* cacheMiss = config->PendingCacheMisses.Top();
        LOG_D("Schedule next wakeup at " << cacheMiss->Deadline);
        Schedule(cacheMiss->Deadline, new TEvents::TEvWakeup);
        cacheMiss->NeedScheduleDeadline = false;
    }
}

void TDynamicNameserver::Handle(TEvPrivate::TEvUpdateEpoch::TPtr &ev, const TActorContext &ctx)
{
    ui32 domain = ev->Get()->Domain;
    ui64 epoch = ev->Get()->Epoch;

    if (ProtocolState != EProtocolState::UseEpochProtocol) {
        return;
    }

    if (DynamicConfigs[domain]->Epoch.Id < epoch
        && (!EpochUpdates.contains(domain)
            || EpochUpdates.at(domain) < epoch))
        RequestEpochUpdate(domain, epoch, ctx);
}

void TDynamicNameserver::Handle(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr /*ev*/)
{}

void TDynamicNameserver::Handle(NConsole::TEvConfigsDispatcher::TEvRemoveConfigSubscriptionResponse::TPtr /*ev*/)
{}

void TDynamicNameserver::Handle(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr ev, const TActorContext &ctx) {
    auto& record = ev->Get()->Record;
    if (SubscribedToConsoleNSConfig && record.HasConfig() && record.GetConfig().HasNameserviceConfig()) {
        ReplaceNameserverSetup(BuildNameserverTable(record.GetConfig().GetNameserviceConfig()));
    }
    if (record.HasConfig() && record.GetConfig().HasFeatureFlags()) {
        const auto& featureFlags = record.GetConfig().GetFeatureFlags();
        if (EnableDeltaProtocol != featureFlags.GetEnableNodeBrokerDeltaProtocol()) {
            EnableDeltaProtocol = featureFlags.GetEnableNodeBrokerDeltaProtocol();
            ui32 domain = AppData()->DomainsInfo->GetDomain()->DomainUid;
            NTabletPipe::CloseClient(ctx, DynamicConfigs[domain]->NodeBrokerPipe);
        }
    }
    Send(ev->Sender, new NConsole::TEvConsole::TEvConfigNotificationResponse(record), 0, ev->Cookie);
}

void TDynamicNameserver::Handle(TEvents::TEvUnsubscribe::TPtr ev) {
    StaticNodeChangeSubscribers.erase(ev->Sender);
}

void TDynamicNameserver::HandleWakeup(const TActorContext &ctx) {
    auto now = ctx.Monotonic();
    LOG_D("HandleWakeup at " << now);

    ui32 domain = AppData()->DomainsInfo->GetDomain()->DomainUid;
    auto &pendingCacheMisses = DynamicConfigs[domain]->PendingCacheMisses;
    auto &cacheMissHolders = DynamicConfigs[domain]->CacheMissHolders;

    while (!pendingCacheMisses.Empty() && pendingCacheMisses.Top()->Deadline <= now) {
        auto* cacheMiss = pendingCacheMisses.Top();
        pendingCacheMisses.Remove(cacheMiss);
        cacheMiss->OnError("Deadline exceeded", ctx);
        cacheMissHolders.erase(cacheMiss);
    }

    if (!pendingCacheMisses.Empty() && pendingCacheMisses.Top()->NeedScheduleDeadline) {
        auto* cacheMiss = pendingCacheMisses.Top();
        LOG_D("Schedule next wakeup at " << cacheMiss->Deadline);
        Schedule(cacheMiss->Deadline, new TEvents::TEvWakeup);
        cacheMiss->NeedScheduleDeadline = false;
    }
}

IActor *CreateDynamicNameserver(const TIntrusivePtr<TTableNameserverSetup> &setup, ui32 poolId) {
    return new TDynamicNameserver(setup, poolId);
}

IActor *CreateDynamicNameserver(const TIntrusivePtr<TTableNameserverSetup> &setup,
        const NKikimrNodeBroker::TNodeInfo &node, const TDomainsInfo &domains, ui32 poolId) {
    return new TDynamicNameserver(setup, node, domains, poolId);
}

TIntrusivePtr<TTableNameserverSetup> BuildNameserverTable(const NKikimrConfig::TStaticNameserviceConfig& nsConfig) {
    auto table = MakeIntrusive<TTableNameserverSetup>();
    for (const auto &node : nsConfig.GetNode()) {
        const ui32 nodeId = node.GetNodeId();
        const TString host = node.HasHost() ? node.GetHost() : TString();
        const ui32 port = node.GetPort();
        const TString resolveHost = node.HasInterconnectHost() ?  node.GetInterconnectHost() : host;
        const TString addr = resolveHost ? TString() : node.GetAddress();
        TNodeLocation location;
        if (node.HasWalleLocation()) {
            location = TNodeLocation(node.GetWalleLocation());
        } else if (node.HasLocation()) {
            location = TNodeLocation(node.GetLocation());
        }
        table->StaticNodeTable[nodeId] = TTableNameserverSetup::TNodeInfo(addr, host, resolveHost, port, location);
    }
    return table;
}

TIntrusivePtr<TTableNameserverSetup> BuildNameserverTable(const NKikimrBlobStorage::TStorageConfig& config) {
    auto table = MakeIntrusive<TTableNameserverSetup>();
    for (const auto &node : config.GetAllNodes()) {
        table->StaticNodeTable[node.GetNodeId()] = TTableNameserverSetup::TNodeInfo(
            TString(), node.GetHost(), node.GetHost(), node.GetPort(), TNodeLocation(node.GetLocation())
        );
    }
    return table;
}

TListNodesCache::TListNodesCache()
    : Nodes(nullptr)
    , Expire(TInstant::Zero())
{}


void TListNodesCache::Update(TIntrusiveVector<TEvInterconnect::TNodeInfo>::TConstPtr newNodes, TInstant newExpire) {
    Nodes = newNodes;
    Expire = newExpire;
}

void TListNodesCache::Invalidate() {
    Nodes = nullptr;
    Expire = TInstant::Zero();
}

bool TListNodesCache::NeedUpdate(TInstant now) const {
    return Nodes == nullptr || now > Expire;
}

TIntrusiveVector<TEvInterconnect::TNodeInfo>::TConstPtr TListNodesCache::GetNodes() const {
    return Nodes;
}

} // NNodeBroker
} // NKikimr
