#include "grpc_proxy_status.h"
#include "msgbus_servicereq.h"

#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <library/cpp/random_provider/random_provider.h>
#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/location.h>

//TODO: add here bucket counter for speed - find out borders from grpc
////////////////////////////////////////////
namespace NKikimr {

TActorId MakeGRpcProxyStatusID(ui32 node) {
    char x[12] = {'g','r','p','c','p','r','x','y','s','t','a','t'};
    return TActorId(node, TStringBuf(x, 12));
}



////////////////////////////////////////////

const ui32 WAKEUP_TIMEOUT_MS = 100;
const ui32 TIMEOUT_SECONDS = 10;


///////////////////////////////////////////

class TChooseProxyActorImpl : public TActorBootstrapped<TChooseProxyActorImpl> {

    using TBase = TActorBootstrapped<TChooseProxyActorImpl>;
    TActorId Sender;
    ui32 NodesRequested;
    ui32 NodesReceived;
    THolder<NMsgBusProxy::TBusChooseProxy> Request;
    TVector<ui32> Nodes;
    THashMap<ui32, TString> NodeNames;
    THashMap<ui32, TString> NodeDataCenter;
    THashMap<ui32, std::shared_ptr<TEvGRpcProxyStatus::TEvGetStatusResponse>> PerNodeResponse;


public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::FRONT_CHOOSE_RROXY;
    }

    //
    TChooseProxyActorImpl(const TActorId& sender)
        : Sender(sender)
        , NodesRequested(0)
        , NodesReceived(0)
    {
    }

    virtual ~TChooseProxyActorImpl()
    {}


    void SendRequest(ui32 nodeId, const TActorContext &ctx) {
        TActorId proxyNodeServiceId = MakeGRpcProxyStatusID(nodeId);
        ctx.Send(proxyNodeServiceId, new TEvGRpcProxyStatus::TEvGetStatusRequest(),
            IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, nodeId);
        Nodes.emplace_back(nodeId);
    }

    void Die(const TActorContext& ctx) override {
        for (const ui32 node : Nodes) {
            ctx.Send(TActivationContext::InterconnectProxy(node), new TEvents::TEvUnsubscribe());
        }
        TBase::Die(ctx);
    }

    void Bootstrap(const TActorContext& ctx) {
        const TActorId nameserviceId = GetNameserviceActorId();
        ctx.Send(nameserviceId, new TEvInterconnect::TEvListNodes());
        TBase::Become(&TThis::StateRequestedBrowse);
        ctx.Schedule(TDuration::Seconds(TIMEOUT_SECONDS), new TEvents::TEvWakeup());
    }

    STFUNC(StateRequestedBrowse) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvInterconnect::TEvNodesInfo, HandleBrowse);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    STFUNC(StateRequested) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvGRpcProxyStatus::TEvGetStatusResponse, HandleResponse);
            HFunc(TEvents::TEvUndelivered, Undelivered);
            HFunc(TEvInterconnect::TEvNodeDisconnected, Disconnected);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void HandleBrowse(TEvInterconnect::TEvNodesInfo::TPtr &ev, const TActorContext &ctx) {
        const TEvInterconnect::TEvNodesInfo* nodesInfo = ev->Get();
        Y_ABORT_UNLESS(!nodesInfo->Nodes.empty());
        Nodes.reserve(nodesInfo->Nodes.size());
        for (const auto& ni : nodesInfo->Nodes) {
            if (ni.Port > AppData(ctx)->PQConfig.GetMaxStorageNodePort()) continue;
            NodeNames[ni.NodeId] = ni.Host;
            NodeDataCenter[ni.NodeId] = ni.Location.GetDataCenterId();
            SendRequest(ni.NodeId, ctx);
            ++NodesRequested;
        }
        if (NodesRequested > 0) {
            TBase::Become(&TThis::StateRequested);
        } else {
            ReplyAndDie(ctx);
        }
    }

    void Undelivered(TEvents::TEvUndelivered::TPtr &ev, const TActorContext &ctx) {
        ui32 nodeId = ev->Cookie;
        if (PerNodeResponse.emplace(nodeId, nullptr).second) {
            NodeResponseReceived(ctx);
        }
    }

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr &ev, const TActorContext &ctx) {
        ui32 nodeId = ev->Get()->NodeId;
        if (PerNodeResponse.emplace(nodeId, nullptr).second) {
            NodeResponseReceived(ctx);
        }
    }

    void HandleResponse(TEvGRpcProxyStatus::TEvGetStatusResponse::TPtr &ev, const TActorContext &ctx) {
        ui64 nodeId = ev->Cookie;
        PerNodeResponse[nodeId].reset(ev->Release().Release());
        NodeResponseReceived(ctx);
    }

    void NodeResponseReceived(const TActorContext &ctx) {
        ++NodesReceived;
        if (NodesReceived >= NodesRequested) {
            ReplyAndDie(ctx);
        }
    }

    void HandleTimeout(const TActorContext &ctx) {
        ReplyAndDie(ctx);
    }


    void ReplyAndDie(const TActorContext& ctx) {
        THolder<TEvGRpcProxyStatus::TEvResponse> response = MakeHolder<TEvGRpcProxyStatus::TEvResponse>();

        response->PerNodeResponse = PerNodeResponse;
        response->NodeNames = NodeNames;
        response->NodeDataCenter = NodeDataCenter;

        ctx.Send(Sender, response.Release());
        Die(ctx);
    }
};




////////////////////////////////////////////
/// The TGRpcProxyStatusActor class
////////////////////////////////////////////
class TGRpcProxyStatusActor : public TActorBootstrapped<TGRpcProxyStatusActor> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::FRONT_GRPC_PROXY_STATUS;
    }

    //
    TGRpcProxyStatusActor();
    virtual ~TGRpcProxyStatusActor();

    //
    void Bootstrap(const TActorContext &ctx);

    //
    STFUNC(StateFunc);

private:
    //
    void Handle(TEvGRpcProxyStatus::TEvSetup::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvGRpcProxyStatus::TEvUpdateStatus::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvGRpcProxyStatus::TEvGetStatusRequest::TPtr &ev, const TActorContext &ctx);
    void HandleWakeup(const TActorContext &ctx);

    void Handle(TEvGRpcProxyStatus::TEvRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvGRpcProxyStatus::TEvResponse::TPtr &ev, const TActorContext &ctx);


    bool Allowed;
    ui32 MaxWriteSessions;
    ui32 MaxReadSessions;

    ui32 WriteSessions;
    ui32 ReadSessions;

    TActorId Worker;
    std::deque<TActorId> Requests;

};

TGRpcProxyStatusActor::TGRpcProxyStatusActor()
    : Allowed(false)
    , MaxWriteSessions(1)
    , MaxReadSessions(1)
    , WriteSessions(0)
    , ReadSessions(0)
    , Worker()
{}

////////////////////////////////////////////
TGRpcProxyStatusActor::~TGRpcProxyStatusActor()
{}

////////////////////////////////////////////
void
TGRpcProxyStatusActor::Bootstrap(const TActorContext &ctx) {
    Become(&TThis::StateFunc);
    ctx.Schedule(TDuration::MilliSeconds(WAKEUP_TIMEOUT_MS), new TEvents::TEvWakeup());
}

////////////////////////////////////////////
void
TGRpcProxyStatusActor::Handle(TEvGRpcProxyStatus::TEvRequest::TPtr &ev, const TActorContext& ctx) {
    Requests.push_back(ev->Sender);
    if (Worker == TActorId()) {
        Worker = ctx.Register(new TChooseProxyActorImpl(ctx.SelfID));
    }
}

////////////////////////////////////////////
void
TGRpcProxyStatusActor::Handle(TEvGRpcProxyStatus::TEvResponse::TPtr &ev, const TActorContext& ctx) {

    for (auto & sender : Requests) {
        THolder<TEvGRpcProxyStatus::TEvResponse> response = MakeHolder<TEvGRpcProxyStatus::TEvResponse>();
        response->PerNodeResponse = ev->Get()->PerNodeResponse;
        response->NodeNames = ev->Get()->NodeNames;
        response->NodeDataCenter = ev->Get()->NodeDataCenter;
        ctx.Send(sender, response.Release());
    }
    Requests.clear();
    Worker = TActorId();
}


////////////////////////////////////////////
void
TGRpcProxyStatusActor::Handle(TEvGRpcProxyStatus::TEvSetup::TPtr &ev, const TActorContext&) {
    Allowed = ev->Get()->Allowed;
    MaxReadSessions = ev->Get()->MaxReadSessions;
    MaxWriteSessions = ev->Get()->MaxWriteSessions;
}

////////////////////////////////////////////
void
TGRpcProxyStatusActor::Handle(TEvGRpcProxyStatus::TEvUpdateStatus::TPtr &ev, const TActorContext&) {

    Y_ABORT_UNLESS((i32)WriteSessions + ev->Get()->WriteSessions >= 0);
    Y_ABORT_UNLESS((i32)ReadSessions + ev->Get()->ReadSessions >= 0);
    WriteSessions += ev->Get()->WriteSessions;
    ReadSessions += ev->Get()->ReadSessions;
    //TODO: count here write/read speed
}

////////////////////////////////////////////
void
TGRpcProxyStatusActor::Handle(TEvGRpcProxyStatus::TEvGetStatusRequest::TPtr &ev, const TActorContext &ctx) {

    THolder<TEvGRpcProxyStatus::TEvGetStatusResponse> resp(new TEvGRpcProxyStatus::TEvGetStatusResponse);
    ui64 weight = Allowed * 1000000;

    if (MaxWriteSessions <= WriteSessions)
        weight = 0;
    else
        weight = Min<ui64>(weight, ui64(MaxWriteSessions - WriteSessions) * 1000000 / MaxWriteSessions);

    if (MaxReadSessions <= ReadSessions)
        weight = 0;
    else
        weight = Min<ui64>(weight, ui64(MaxReadSessions - ReadSessions) * 1000000 / MaxReadSessions);

    resp->Record.SetWeight(weight);
    ctx.Send(ev->Sender, resp.Release(), 0, ev->Cookie);
}


////////////////////////////////////////////
void
TGRpcProxyStatusActor::HandleWakeup(const TActorContext &ctx) {

    ctx.Schedule(TDuration::Seconds(WAKEUP_TIMEOUT_MS), new TEvents::TEvWakeup());
    //TODO: update statistics here
}


////////////////////////////////////////////
/// public state functions
////////////////////////////////////////////
STFUNC(TGRpcProxyStatusActor::StateFunc) {
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvGRpcProxyStatus::TEvSetup, Handle);
        HFunc(TEvGRpcProxyStatus::TEvGetStatusRequest, Handle);
        HFunc(TEvGRpcProxyStatus::TEvUpdateStatus, Handle);
        HFunc(TEvGRpcProxyStatus::TEvRequest, Handle);
        HFunc(TEvGRpcProxyStatus::TEvResponse, Handle);
        CFunc(TEvents::TSystem::Wakeup, HandleWakeup);

        // HFunc(TEvents::TEvPoisonPill, Handle); // we do not need PoisonPill for the actor
    }
}


///////////////////////////////////////////

class TChooseProxyActor : public TActorBootstrapped<TChooseProxyActor>, public NMsgBusProxy::TMessageBusSessionIdentHolder {

    using TBase = TActorBootstrapped<TChooseProxyActor>;
    THolder<NMsgBusProxy::TBusChooseProxy> Request;
    THashMap<ui32, TString> NodeNames;
    THashMap<ui32, TString> NodeDataCenter;
    THashMap<ui32, std::shared_ptr<TEvGRpcProxyStatus::TEvGetStatusResponse>> PerNodeResponse;


public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::FRONT_CHOOSE_RROXY;
    }

    //
    TChooseProxyActor(NMsgBusProxy::TBusMessageContext &msg)
        : TMessageBusSessionIdentHolder(msg)
        , Request(static_cast<NMsgBusProxy::TBusChooseProxy*>(msg.ReleaseMessage()))
    {
    }

    virtual ~TChooseProxyActor()
    {}

    void Bootstrap(const TActorContext& ctx) {
        ctx.Send(MakeGRpcProxyStatusID(ctx.SelfID.NodeId()), new TEvGRpcProxyStatus::TEvRequest());
        TBase::Become(&TThis::StateWork);
        ctx.Schedule(TDuration::Seconds(TIMEOUT_SECONDS), new TEvents::TEvWakeup());
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvGRpcProxyStatus::TEvResponse, HandleResponse);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void HandleResponse(TEvGRpcProxyStatus::TEvResponse::TPtr &ev, const TActorContext &ctx) {
        NodeNames = ev->Get()->NodeNames;
        PerNodeResponse = ev->Get()->PerNodeResponse;
        NodeDataCenter = ev->Get()->NodeDataCenter;
        ReplyAndDie(ctx);
    }

    void HandleTimeout(const TActorContext &ctx) {
        ReplyAndDie(ctx);
    }


    void ReplyAndDie(const TActorContext& ctx) {
        auto response = MakeHolder<NMsgBusProxy::TBusResponse>();
        TString name;
        ui64 totalWeight = 0;
        ui64 cookie = 0;
        const auto& record = Request->Record;
        const std::optional<TString> filterDataCenter = record.HasDataCenter() ? std::make_optional(record.GetDataCenter()) :
            record.HasDataCenterNum() ? std::make_optional(DataCenterToString(record.GetDataCenterNum())) : std::nullopt;
        const bool preferLocalProxy = Request->Record.GetPreferLocalProxy();
        const ui32 localNodeId = ctx.SelfID.NodeId();
        //choose random proxy
        TStringBuilder s;
        s << "ChooseProxyResponses [preferLocal " << preferLocalProxy << " localId " << localNodeId << "]:";
        for (auto& resp : PerNodeResponse) {
            if (!resp.second)
                continue;
            s << " " << NodeNames[resp.first] << "[" << resp.first << "], " << resp.second->Record.GetWeight() << " ";
            if (filterDataCenter && filterDataCenter != NodeDataCenter[resp.first])
                continue;

            ui64 weight = resp.second->Record.GetWeight();
            ui64 rand = TAppData::RandomProvider->GenRand64();
            if ((weight > 0 && rand % (totalWeight + weight) >= totalWeight || preferLocalProxy && resp.first == localNodeId) && //random choosed this node or it is prefered local node
                (!preferLocalProxy || cookie != localNodeId)) { //and curent node will not kick prefered local
                name = NodeNames[resp.first];
                cookie = resp.first;
            }
            totalWeight += weight;
        }

        s << " : result " << cookie << " " << name;
        LOG_DEBUG(ctx, NKikimrServices::CHOOSE_PROXY, "%s", s.c_str());

        if (name.empty()) {
            response->Record.SetStatus(NMsgBusProxy::MSTATUS_ERROR);
        } else {
            response->Record.SetProxyName(name);
            response->Record.SetProxyCookie(cookie); //TODO: encode here name and current time
            response->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);
        }

        SendReplyMove(response.Release());
        Die(ctx);
    }
};




IActor* CreateGRpcProxyStatus() {
    return new TGRpcProxyStatusActor();
}
namespace NMsgBusProxy {

    IActor* CreateMessageBusChooseProxy(NMsgBusProxy::TBusMessageContext &msg) {
        return new TChooseProxyActor(msg);
    }

}

}
