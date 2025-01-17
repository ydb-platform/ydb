#include "dynamic_nameserver_impl.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/nameservice.h>
#include <ydb/core/mon/mon.h>
#include <ydb/library/services/services.pb.h>

namespace NKikimr {
namespace NNodeBroker {

static void ResetInterconnectProxyConfig(ui32 nodeId, const TActorContext &ctx)
{
    auto aid = TActivationContext::InterconnectProxy(nodeId);
    if (!aid)
        return;
    ctx.Send(aid, new TEvInterconnect::TEvDisconnect);
}

void TDynamicNodeResolverBase::Bootstrap(const TActorContext &ctx)
{
    auto dinfo = AppData(ctx)->DomainsInfo;

    NTabletPipe::TClientRetryPolicy retryPolicy = {
        .RetryLimitCount = 12,
        .MinRetryTime = TDuration::MilliSeconds(50),
        .MaxRetryTime = TDuration::Seconds(2)
    };

    auto pipe = NTabletPipe::CreateClient(ctx.SelfID, MakeNodeBrokerID(), NTabletPipe::TClientConfig(retryPolicy));
    NodeBrokerPipe = ctx.RegisterWithSameMailbox(pipe);

    TAutoPtr<TEvNodeBroker::TEvResolveNode> request = new TEvNodeBroker::TEvResolveNode;
    request->Record.SetNodeId(NodeId);
    NTabletPipe::SendData(ctx, NodeBrokerPipe, request.Release());

    Become(&TDynamicNodeResolverBase::StateWork);
    if (Deadline != TInstant::Max()) {
        Schedule(Deadline, new TEvents::TEvWakeup);
    }
}

void TDynamicNodeResolverBase::Die(const TActorContext &ctx)
{
    if (NodeBrokerPipe)
        NTabletPipe::CloseClient(ctx, NodeBrokerPipe);
    TBase::Die(ctx);
}

void TDynamicNodeResolverBase::ReplyWithErrorAndDie(const TActorContext &ctx)
{
    OnError(ctx);
    Die(ctx);
}

void TDynamicNodeResolverBase::Handle(TEvNodeBroker::TEvResolvedNode::TPtr &ev, const TActorContext &ctx)
{
    auto &rec = ev->Get()->Record;
    TDynamicConfig::TDynamicNodeInfo oldNode;
    auto it = Config->DynamicNodes.find(NodeId);
    bool exists = it != Config->DynamicNodes.end();

    if (exists) {
        oldNode = it->second;
        Config->DynamicNodes.erase(it);
    }

    if (rec.GetStatus().GetCode() != NKikimrNodeBroker::TStatus::OK) {
        // Reset proxy if node expired.
        if (exists) {
            ResetInterconnectProxyConfig(NodeId, ctx);
            ListNodesCache->Invalidate(); // node was erased
        }
        ReplyWithErrorAndDie(ctx);
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
    Die(ctx);
}

void TDynamicNodeResolverBase::Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx)
{
    if (ev->Get()->Status != NKikimrProto::OK)
        ReplyWithErrorAndDie(ctx);
}

void TDynamicNodeResolver::OnSuccess(const TActorContext &ctx)
{
    ctx.Send(OrigRequest);
}

void TDynamicNodeResolver::OnError(const TActorContext &ctx)
{
    auto reply = new TEvLocalNodeInfo;
    reply->NodeId = NodeId;
    ctx.Send(OrigRequest->Sender, reply);
}

void TDynamicNodeSearcher::OnSuccess(const TActorContext &ctx)
{
    THolder<TEvInterconnect::TEvNodeInfo> reply(new TEvInterconnect::TEvNodeInfo(NodeId));
    auto it = Config->DynamicNodes.find(NodeId);
    if (it != Config->DynamicNodes.end())
        reply->Node = MakeHolder<TEvInterconnect::TNodeInfo>(it->first, it->second.Address,
                                                     it->second.Host, it->second.ResolveHost,
                                                     it->second.Port, it->second.Location);
    ctx.Send(OrigRequest->Sender, reply.Release());
}

void TDynamicNodeSearcher::OnError(const TActorContext &ctx)
{
    THolder<TEvInterconnect::TEvNodeInfo> reply(new TEvInterconnect::TEvNodeInfo(NodeId));
    ctx.Send(OrigRequest->Sender, reply.Release());
}

void TDynamicNameserver::Bootstrap(const TActorContext &ctx)
{
    NActors::TMon* mon = AppData(ctx)->Mon;
    if (mon) {
        NMonitoring::TIndexMonPage *actorsMonPage = mon->RegisterIndexPage("actors", "Actors");
        mon->RegisterActorPage(actorsMonPage, "dnameserver", "Dynamic nameserver",
                               false, ctx.ExecutorThread.ActorSystem, ctx.SelfID);
    }

    auto dinfo = AppData(ctx)->DomainsInfo;
    if (const auto& domain = dinfo->Domain) {
        RequestEpochUpdate(domain->DomainUid, 1, ctx);
    }

    Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()), new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest(
        NKikimrConsole::TConfigItem::NameserviceConfigItem,
        SelfId()
    ));

    Become(&TDynamicNameserver::StateFunc);
}

void TDynamicNameserver::Die(const TActorContext &ctx)
{
    for (auto &pipe : NodeBrokerPipes) {
        if (pipe)
            NTabletPipe::CloseClient(ctx, pipe);
    }
    TBase::Die(ctx);
}

void TDynamicNameserver::OpenPipe(ui32 domain,
                                  const TActorContext &ctx)
{
    if (!NodeBrokerPipes[domain]) {
        auto pipe = NTabletPipe::CreateClient(ctx.SelfID, MakeNodeBrokerID());
        NodeBrokerPipes[domain] = ctx.RegisterWithSameMailbox(pipe);
    }
}

void TDynamicNameserver::RequestEpochUpdate(ui32 domain,
                                            ui32 epoch,
                                            const TActorContext &ctx)
{
    OpenPipe(domain, ctx);

    TAutoPtr<TEvNodeBroker::TEvListNodes> request = new TEvNodeBroker::TEvListNodes;
    request->Record.SetMinEpoch(epoch);
    NTabletPipe::SendData(ctx, NodeBrokerPipes[domain], request.Release());
    EpochUpdates[domain] = epoch;
}

void TDynamicNameserver::ResolveStaticNode(ui32 nodeId, TActorId sender, TInstant deadline, const TActorContext &ctx)
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
                                            TInstant deadline,
                                            const TActorContext &ctx)
{
    ui32 domain = AppData()->DomainsInfo->GetDomain()->DomainUid;
    auto it = DynamicConfigs[domain]->DynamicNodes.find(nodeId);

    if (it != DynamicConfigs[domain]->DynamicNodes.end()
        && it->second.Expire > ctx.Now())
    {
        RegisterWithSameMailbox(CreateResolveActor(it->second.ResolveHost, it->second.Port, nodeId, it->second.Address, ev->Sender, SelfId(), deadline));
    } else if (DynamicConfigs[domain]->ExpiredNodes.contains(nodeId)
                && ctx.Now() < DynamicConfigs[domain]->Epoch.End) {
        auto reply = new TEvLocalNodeInfo;
        reply->NodeId = nodeId;
        ctx.Send(ev->Sender, reply);
    } else {
        ctx.RegisterWithSameMailbox(new TDynamicNodeResolver(SelfId(), nodeId, DynamicConfigs[domain],
            ListNodesCache, ev, deadline));
    }
}

void TDynamicNameserver::SendNodesList(const TActorContext &ctx)
{   
    auto now = ctx.Now();
    if (ListNodesCache->NeedUpdate(now)) {
        auto newNodes = MakeIntrusive<TIntrusiveVector<TEvInterconnect::TNodeInfo>>();
        auto newExpire = now;

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

    for (auto &sender : ListNodesQueue) {
        ctx.Send(sender, new TEvInterconnect::TEvNodesInfo(ListNodesCache->GetNodes()));
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
            TDynamicConfig::TDynamicNodeInfo info(node);
            config->ExpiredNodes.emplace(node.GetNodeId(), info);
        }

        ListNodesCache->Invalidate();
        config->Epoch = rec.GetEpoch();
        ctx.Schedule(config->Epoch.End - ctx.Now(),
                     new TEvPrivate::TEvUpdateEpoch(domain, config->Epoch.Id + 1));
    } else {
        // Note: this update may be optimized to only include new nodes
        for (auto &node : rec.GetNodes()) {
            auto nodeId = node.GetNodeId();
            if (!config->DynamicNodes.contains(nodeId)) {
                config->DynamicNodes.emplace(nodeId, node);
                ListNodesCache->Invalidate();
            }                
        }
        config->Epoch = rec.GetEpoch();
    }
}

void TDynamicNameserver::OnPipeDestroyed(ui32 domain,
                                         const TActorContext &ctx)
{
    NodeBrokerPipes[domain] = TActorId();
    PendingRequestAnswered(domain, ctx);

    if (EpochUpdates.contains(domain)) {
        ctx.Schedule(TDuration::Seconds(1),
                     new TEvPrivate::TEvUpdateEpoch(domain, EpochUpdates.at(domain)));
        EpochUpdates.erase(domain);
    }
}

void TDynamicNameserver::Handle(TEvInterconnect::TEvResolveNode::TPtr &ev,
                                const TActorContext &ctx)
{
    auto& record = ev->Get()->Record;
    const ui32 nodeId = record.GetNodeId();
    const TInstant deadline = record.HasDeadline() ? TInstant::FromValue(record.GetDeadline()) : TInstant::Max();
    auto config = AppData(ctx)->DynamicNameserviceConfig;

    if (!config || nodeId <= config->MaxStaticNodeId)
        ResolveStaticNode(nodeId, ev->Sender, deadline, ctx);
    else
        ResolveDynamicNode(nodeId, ev.Release(), deadline, ctx);
}

void TDynamicNameserver::Handle(TEvResolveAddress::TPtr &ev, const TActorContext &ctx) {
    Y_UNUSED(ctx);

    const TEvResolveAddress* request = ev->Get();

    RegisterWithSameMailbox(CreateResolveActor(request->Address, request->Port, ev->Sender, SelfId(), TInstant::Max()));
}

void TDynamicNameserver::Handle(TEvInterconnect::TEvListNodes::TPtr &ev,
                                const TActorContext &ctx)
{
    if (ListNodesQueue.empty()) {
        auto dinfo = AppData(ctx)->DomainsInfo;
        if (const auto& d = dinfo->Domain) {
            ui32 domain = d->DomainUid;
            OpenPipe(domain, ctx);
            TAutoPtr<TEvNodeBroker::TEvListNodes> request = new TEvNodeBroker::TEvListNodes;
            request->Record.SetCachedVersion(DynamicConfigs[domain]->Epoch.Version);
            NTabletPipe::SendData(ctx, NodeBrokerPipes[domain], request.Release());
            PendingRequests.Set(domain);
        }
    }
    ListNodesQueue.push_back(ev->Sender);
    if (ev->Get()->SubscribeToStaticNodeChanges) {
        StaticNodeChangeSubscribers.insert(ev->Sender);
    }
}

void TDynamicNameserver::Handle(TEvInterconnect::TEvGetNode::TPtr &ev, const TActorContext &ctx)
{
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
        auto it = DynamicConfigs[domain]->DynamicNodes.find(nodeId);
        if (it != DynamicConfigs[domain]->DynamicNodes.end() && it->second.Expire > ctx.Now()) {
            reply->Node = MakeHolder<TEvInterconnect::TNodeInfo>(it->first, it->second.Address,
                                                         it->second.Host, it->second.ResolveHost,
                                                         it->second.Port, it->second.Location);
            ctx.Send(ev->Sender, reply.Release());
        } else if (DynamicConfigs[domain]->ExpiredNodes.contains(nodeId)
                   && ctx.Now() < DynamicConfigs[domain]->Epoch.End) {
            ctx.Send(ev->Sender, reply.Release());
        } else {
            const TInstant deadline = ev->Get()->Deadline;
            ctx.RegisterWithSameMailbox(new TDynamicNodeSearcher(SelfId(), nodeId, DynamicConfigs[domain],
                ListNodesCache, ev.Release(), deadline));
        }
    }
}

void TDynamicNameserver::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr &ev, const TActorContext &ctx)
{
    ui32 domain = AppData()->DomainsInfo->GetDomain()->DomainUid;
    if (NodeBrokerPipes[domain] == ev->Get()->ClientId)
        OnPipeDestroyed(domain, ctx);
}

void TDynamicNameserver::Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx)
{
    if (ev->Get()->Status != NKikimrProto::OK) {
        ui32 domain = AppData(ctx)->DomainsInfo->GetDomain()->DomainUid;
        if (NodeBrokerPipes[domain] == ev->Get()->ClientId) {
            NTabletPipe::CloseClient(ctx, NodeBrokerPipes[domain]);
            OnPipeDestroyed(domain, ctx);
        }
    }
}

void TDynamicNameserver::Handle(TEvNodeBroker::TEvNodesInfo::TPtr &ev, const TActorContext &ctx)
{
    auto &rec = ev->Get()->GetRecord();
    Y_ABORT_UNLESS(rec.HasDomain());
    ui32 domain = rec.GetDomain();

    if (rec.GetEpoch().GetVersion() != DynamicConfigs[domain]->Epoch.Version)
        UpdateState(rec, ctx);

    if (EpochUpdates.contains(domain) && EpochUpdates.at(domain) <= rec.GetEpoch().GetId())
        EpochUpdates.erase(domain);

    PendingRequestAnswered(rec.GetDomain(), ctx);
}

void TDynamicNameserver::Handle(TEvPrivate::TEvUpdateEpoch::TPtr &ev, const TActorContext &ctx)
{
    ui32 domain = ev->Get()->Domain;
    ui64 epoch = ev->Get()->Epoch;

    if (DynamicConfigs[domain]->Epoch.Id < epoch
        && (!EpochUpdates.contains(domain)
            || EpochUpdates.at(domain) < epoch))
        RequestEpochUpdate(domain, epoch, ctx);
}

void TDynamicNameserver::Handle(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr /*ev*/)
{}

void TDynamicNameserver::Handle(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr ev) {
    auto& record = ev->Get()->Record;
    if (record.HasConfig()) {
        if (const auto& config = record.GetConfig(); config.HasNameserviceConfig()) {
            auto newStaticConfig = BuildNameserverTable(config.GetNameserviceConfig());
            if (StaticConfig->StaticNodeTable != newStaticConfig->StaticNodeTable) {
                StaticConfig = std::move(newStaticConfig);
                ListNodesCache->Invalidate();
                for (const auto& subscriber : StaticNodeChangeSubscribers) {
                    TActivationContext::Send(new IEventHandle(SelfId(), subscriber, new TEvInterconnect::TEvListNodes));
                }
            }
        }
    }
    Send(ev->Sender, new NConsole::TEvConsole::TEvConfigNotificationResponse(record), 0, ev->Cookie);
}

void TDynamicNameserver::Handle(TEvents::TEvUnsubscribe::TPtr ev) {
    StaticNodeChangeSubscribers.erase(ev->Sender);
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
