#include "tablet_info.h"

#include <ydb/library/actors/core/log.h>
#include <ydb/core/mon/mon.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/base/tracing.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include "http.h"

namespace NKikimr {
namespace NTabletInfo {

using namespace NActors;
using namespace NNodeWhiteboard;

namespace {

template<typename TStatus>
struct TWBReplyStatus {
    enum EStatus {
        OK = 0,
        UNDELIVERED,
        DISCONNECTED,
        TIMEOUT
    };

    static TString StatusDescription(EStatus status) {
        switch (status) {
        case OK:
            return "Reply received";
            break;
        case UNDELIVERED:
            return "Request undelivered";
            break;
        case DISCONNECTED:
            return "Node disconnected";
            break;
        case TIMEOUT:
            return "Request timeout";
        default:
            return "Unknown status";
            break;
        }
    }

    EStatus Status;
    THolder<TStatus> Response;
};

void AddTabletLookupForm(TStringStream& str) {
    str << "<br><form method=\"GET\" id=\"tabletMonFrm\" name=\"tabletMonFrm\">" << Endl;
    str << "<h3>Tablet lookup</h3>" << Endl;
    str << "TabletID: <input type=\"text\" id=\"iTabletID\" name=\"iTabletID\"/>" << Endl;
    str << "<input class=\"btn btn-primary\" type=\"submit\" value=\"Watch\"/>" << Endl;
    str << "</form>" << Endl;
}

void AddTraceLookupForm(TStringStream& str) {
    str << "<br><form method=\"GET\" id=\"tblIntMonFrm\" name=\"tblIntMonFrm\">" << Endl;
    str << "<h3>Trace lookup</h3>" << Endl;
    str << "<a class='collapse-ref' data-toggle='collapse' data-target='#LookupInfo'>Help</a>";
    str << "<div id='LookupInfo' class='collapse'>";
    str << "<div class=\"tab-left\">"
        << "NodeID - Get all tablets with traces(NodeID=0 - all nodes)</div>" << Endl;
    str << "<div class=\"tab-left\">"
        << "TabletID - Get all traces for given Tablet on all Nodes </div>" << Endl;
    str << "<div class=\"tab-left\">"
        << "NodeID && TabletID - Get all traces for given Tablet on given Node </div>" << Endl;
    str << "<div class=\"tab-left\">"
        << "NodeID && TabletID && RandomID && CreationTime - show exact trace </div>" << Endl;
    str << "</div><br>" << Endl;
    str << "NodeID: <input type=\"text\" id=\"NodeID\" name=\"NodeID\"/>" << Endl;
    str << "TabletID: <input type=\"text\" id=\"TabletID\" name=\"TabletID\"/>" << Endl;
    str << "RandomID: <input type=\"text\" id=\"RandomID\" name=\"RandomID\"/>" << Endl;
    str << "CreationTime: <input type=\"text\" id=\"CreationTime\" name=\"CreationTime\"/>" << Endl;
    str << "<input class=\"btn btn-primary\" type=\"submit\" value=\"Watch\"/>" << Endl;
    str << "</form>" << Endl;
}

class TTabletLookupActor : public TActorBootstrapped<TTabletLookupActor> {
public:
    using TReplyStatus = TWBReplyStatus<TEvWhiteboard::TEvTabletLookupResponse>;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TABLET_LOOKUP_ACTOR;
    }

    TTabletLookupActor(ui32 nodeId, const TActorId& sender, ui32 timeout)
        : NodeId(nodeId)
        , Sender(sender)
        , Timeout(timeout)
    {}

    void Bootstrap(const TActorContext& ctx) {
        if (NodeId) {
            // Direct request
            SendRequest(NodeId, ctx);
            Become(&TThis::StateTabletLookupRequested);
        } else {
            // Broadcast request
            const TActorId nameserviceId = NActors::GetNameserviceActorId();
            ctx.Send(nameserviceId, new TEvInterconnect::TEvListNodes());
            Become(&TThis::StateNodeListRequested);
        }
        ctx.Schedule(TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
    }

    STFUNC(StateNodeListRequested) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvInterconnect::TEvNodesInfo, HandleNodeList);
            CFunc(TEvents::TSystem::Wakeup, HandleNodeListTimeout);
        }
    }

    void HandleNodeList(TEvInterconnect::TEvNodesInfo::TPtr& ev, const TActorContext& ctx) {
        for (const auto& ni : ev->Get()->Nodes) {
            SendRequest(ni.NodeId, ctx);
        }
        if (Requested > 0) {
            Become(&TThis::StateTabletLookupRequested);
        } else {
            ReplyAndDie(ctx);
        }
    }

    void HandleNodeListTimeout(const TActorContext &ctx) {
        Notify(ctx, "Timeout getting node list");
        Die(ctx);
    }

    void SendRequest(ui32 nodeId, const TActorContext& ctx) {
        TActorId whiteboardServiceId = MakeNodeWhiteboardServiceId(nodeId);
        ctx.Send(whiteboardServiceId
            , new TEvWhiteboard::TEvTabletLookupRequest()
            , IEventHandle::FlagTrackDelivery, nodeId);
        ++Requested;
        TracedTablets[nodeId] = nullptr;
    }

    STFUNC(StateTabletLookupRequested) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvWhiteboard::TEvTabletLookupResponse, Handle);
            HFunc(TEvents::TEvUndelivered, Undelivered);
            HFunc(TEvInterconnect::TEvNodeDisconnected, Disconnected);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void Handle(TEvWhiteboard::TEvTabletLookupResponse::TPtr& ev, const TActorContext& ctx) {
        ui64 nodeId = ev.Get()->Cookie;
        TracedTablets[nodeId] = THolder<TReplyStatus>(new TReplyStatus({ TReplyStatus::OK, THolder<TEvWhiteboard::TEvTabletLookupResponse>(ev->Release()) }));
        NodeTabletInfoReceived(ctx);
    }

    void NodeTabletInfoReceived(const TActorContext& ctx) {
        ++Received;
        if (Received >= Requested) {
            ReplyAndDie(ctx);
        }
    }

    void BadResponseReceived(ui32 nodeId, const TActorContext& ctx, TReplyStatus::EStatus status) {
        if (TracedTablets[nodeId] == nullptr) {
            TracedTablets[nodeId] = THolder<TReplyStatus>(new TReplyStatus({ status, nullptr }));
            NodeTabletInfoReceived(ctx);
        }
    }

    void Undelivered(TEvents::TEvUndelivered::TPtr &ev, const TActorContext &ctx) {
        BadResponseReceived(ev.Get()->Cookie, ctx, TReplyStatus::UNDELIVERED);
    }

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr &ev, const TActorContext &ctx) {
        BadResponseReceived(ev.Get()->Cookie, ctx, TReplyStatus::DISCONNECTED);
    }

    void HandleTimeout(const TActorContext &ctx) {
        for (auto& nodeTabletTracesPair : TracedTablets) {
            if (!nodeTabletTracesPair.second) {
                nodeTabletTracesPair.second = THolder<TReplyStatus>(new TReplyStatus({ TReplyStatus::TIMEOUT, nullptr }));
            }
        }
        ReplyAndDie(ctx);
    }

    void ReplyAndDie(const TActorContext& ctx) {
        TStringStream str;

        HTML(str) {
            NTracing::NHttp::OutputStaticPart(str);
            AddTabletLookupForm(str);
            str << "<br>";

            TAG(TH3) {
                str << "All tablets that have traces";
                if (NodeId) {
                    str << " on Node " << NodeId << ":";
                } else {
                    str << ":";
                }
            }
            for (auto& nodeTabletsPair : TracedTablets) {
                ui32 nodeId = nodeTabletsPair.first;
                if (nodeTabletsPair.second->Status == TReplyStatus::OK) {
                    auto& nodeTablets = nodeTabletsPair.second->Response.Get()->Record;
                    size_t tabletIDsSize = nodeTablets.TabletIDsSize();
                    if (!NodeId) {
                        TAG(TH4) {
                            str << "Node " << nodeId;
                            if (!tabletIDsSize) {
                                str << " - no tablets with traces found";
                            }
                        }
                    }
                    if (tabletIDsSize) {
                        for (ui32 i = 0; i < tabletIDsSize; ++i) {
                            ui64 tabletId = nodeTablets.GetTabletIDs(i);
                            DIV_CLASS("tab-left") {
                                str << "<a href=\"tablet?NodeID=" << nodeId << "&TabletID=" << tabletId << "\">"
                                    << tabletId << "</a>";
                            }
                        }
                    } else {
                        if (NodeId) {
                            DIV_CLASS("tab-left") {
                                str << "No tablets with traces found";
                            }
                        }
                    }
                } else {
                    // Bad response
                    if (NodeId) {
                        DIV_CLASS("tab-left") {
                            str << "<font color=\"red\">"
                                << TReplyStatus::StatusDescription(nodeTabletsPair.second->Status)
                                << "</font>";
                        }
                    } else {
                        TAG(TH4) {
                            str << "<font color=\"red\">Node " << nodeId << ": "
                                << TReplyStatus::StatusDescription(nodeTabletsPair.second->Status)
                                << "</font>";
                        }
                    }
                }
            }
            AddTraceLookupForm(str);
        }
        Notify(ctx, str.Str());
        Die(ctx);
    }

    void Notify(const TActorContext &ctx, const TString& html) {
        ctx.Send(Sender, new NMon::TEvHttpInfoRes(html));
    }

private:
    const ui32 NodeId;
    const TActorId Sender;
    ui32 Timeout;
    ui32 Requested = 0;
    ui32 Received = 0;
    TMap<ui32, THolder<TReplyStatus>> TracedTablets;
};

class TTraceLookupActor : public TActorBootstrapped<TTraceLookupActor> {
public:
    using TReplyStatus = TWBReplyStatus<TEvWhiteboard::TEvTraceLookupResponse>;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TRACE_LOOKUP_ACTOR;
    }

    TTraceLookupActor(ui32 nodeId, ui64 tabletId, const TActorId& sender, ui32 timeout)
        : NodeId(nodeId)
        , TabletId(tabletId)
        , Sender(sender)
        , Timeout(timeout)
    {}

    void Bootstrap(const TActorContext& ctx) {
        if (NodeId) {
            // Direct request
            SendRequest(NodeId, ctx);
            Become(&TThis::StateTraceLookupRequested);
        } else {
            // Broadcast request
            const TActorId nameserviceId = NActors::GetNameserviceActorId();
            ctx.Send(nameserviceId, new TEvInterconnect::TEvListNodes());
            Become(&TThis::StateNodeListRequested);
            ctx.Schedule(TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
        }
    }

    STFUNC(StateNodeListRequested) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvInterconnect::TEvNodesInfo, HandleNodeList);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void HandleNodeList(TEvInterconnect::TEvNodesInfo::TPtr& ev, const TActorContext& ctx) {
        for (const auto& ni : ev->Get()->Nodes) {
            SendRequest(ni.NodeId, ctx);
        }
        if (Requested > 0) {
            Become(&TThis::StateTraceLookupRequested);
        } else {
            ReplyAndDie(ctx);
        }
    }

    void SendRequest(ui32 nodeId, const TActorContext& ctx) {
        TActorId whiteboardServiceId = MakeNodeWhiteboardServiceId(nodeId);
        THolder<TEvWhiteboard::TEvTraceLookupRequest> request = MakeHolder<TEvWhiteboard::TEvTraceLookupRequest>();
        auto& record = request->Record;
        record.SetTabletID(TabletId);
        ctx.Send(whiteboardServiceId, request.Release(), IEventHandle::FlagTrackDelivery, nodeId);
        ++Requested;
        TabletTraces[nodeId] = nullptr;
    }

    STFUNC(StateTraceLookupRequested) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvWhiteboard::TEvTraceLookupResponse, Handle);
            HFunc(TEvents::TEvUndelivered, Undelivered);
            HFunc(TEvInterconnect::TEvNodeDisconnected, Disconnected);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void Handle(TEvWhiteboard::TEvTraceLookupResponse::TPtr& ev, const TActorContext& ctx) {
        ui64 nodeId = ev.Get()->Cookie;
        TabletTraces[nodeId] = THolder<TReplyStatus>(new TReplyStatus({ TReplyStatus::OK, THolder<TEvWhiteboard::TEvTraceLookupResponse>(ev->Release()) }));
        NodeTabletInfoReceived(ctx);
    }

    void NodeTabletInfoReceived(const TActorContext& ctx) {
        ++Received;
        if (Received >= Requested) {
            ReplyAndDie(ctx);
        }
    }

    void BadResponseReceived(ui32 nodeId, const TActorContext& ctx, TReplyStatus::EStatus status) {
        if (TabletTraces[nodeId] == nullptr) {
            TabletTraces[nodeId] = THolder<TReplyStatus>(new TReplyStatus({ status, nullptr }));
            NodeTabletInfoReceived(ctx);
        }
    }

    void Undelivered(TEvents::TEvUndelivered::TPtr &ev, const TActorContext &ctx) {
        BadResponseReceived(ev.Get()->Cookie, ctx, TReplyStatus::UNDELIVERED);
    }

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr &ev, const TActorContext &ctx) {
        BadResponseReceived(ev.Get()->Cookie, ctx, TReplyStatus::DISCONNECTED);
    }

    void ReplyAndDie(const TActorContext& ctx) {
        TStringStream str;
        TList<ui32> nodesWithNoTraces;
        THashMap<TReplyStatus::EStatus, TList<ui32>> failNodes;

        HTML(str) {
            NTracing::NHttp::OutputStaticPart(str);
            str << "<br>";
            TAG(TH3) {
                str << "Traces for Tablet " << "<a href=\"tablet?iTabletID=" << TabletId << "\">"
                    << TabletId << "</a>" << ":";
            }
            for (auto& nodeTabletTracesPair : TabletTraces) {
                ui32 nodeId = nodeTabletTracesPair.first;
                if (nodeTabletTracesPair.second->Status == TReplyStatus::OK) {
                    auto& nodeTabletTraces = nodeTabletTracesPair.second->Response.Get()->Record;
                    size_t traceIDsSize = nodeTabletTraces.TraceIDsSize();
                    if (traceIDsSize) {
                        TAG(TH4) {
                            str << "Node " << nodeId << ":";
                        }
                        // Sorting traces by Id
                        TVector<NTracing::TTraceID> traceIDs;
                        traceIDs.reserve(traceIDsSize);
                        for (ui32 i = 0; i < traceIDsSize; ++i) {
                            traceIDs.push_back(NTracing::TraceIDFromTraceID(nodeTabletTraces.GetTraceIDs(i)));
                        }
                        Sort(traceIDs.begin(), traceIDs.end());
                        for (auto& traceID : traceIDs) {
                            DIV_CLASS("row") {
                                DIV_CLASS("col-md-12") {
                                    str << "<a href=\"tablet?NodeID=" << nodeId
                                        << "&TabletID=" << TabletId
                                        << "&RandomID=" << traceID.RandomID
                                        << "&CreationTime=" << traceID.CreationTime
                                        << "\">" << traceID.RandomID << ":" << traceID.CreationTime << "</a>"
                                        << " - created at "
                                        << TInstant::MicroSeconds(traceID.CreationTime).ToRfc822StringLocal();
                                }
                            }
                        }
                    } else {
                        nodesWithNoTraces.push_back(nodeId);
                    }
                } else {
                    failNodes[nodeTabletTracesPair.second->Status].push_back(nodeId);
                }
            }
            size_t emptyNodes = nodesWithNoTraces.size();
            if (emptyNodes) {
                str << "<br>";
                TAG(TH4) {
                    str << "No traces found on " << emptyNodes << " node" << (emptyNodes == 1 ? ":" : "s:");
                }
                DIV_CLASS("row") {
                    DIV_CLASS("col-md-12") {
                        for (ui32 nodeId : nodesWithNoTraces) {
                            if (nodeId != *nodesWithNoTraces.begin()) {
                                str << ", ";
                            }
                            str << nodeId;
                        }
                    }
                }
            }

            for (auto& failNodesPair : failNodes) {
                size_t failedNodes = failNodesPair.second.size();
                if (failedNodes) {
                    str << "<br>";
                    TAG(TH4) {
                        str << "<font color=\"red\">"
                            << TReplyStatus::StatusDescription(failNodesPair.first)
                            << "</font> on " << failedNodes
                            << " node" << (failedNodes == 1 ? ":" : "s:");
                    }
                    DIV_CLASS("row") {
                        DIV_CLASS("col-md-12") {
                            for (ui32 nodeId : failNodesPair.second) {
                                if (nodeId != *failNodesPair.second.begin()) {
                                    str << ", ";
                                }
                                str << nodeId;
                            }
                        }
                    }
                }
            }
            AddTraceLookupForm(str);
        }
        Notify(ctx, str.Str());
        Die(ctx);
    }

    void Notify(const TActorContext &ctx, const TString& html) {
        ctx.Send(Sender, new NMon::TEvHttpInfoRes(html));
    }

    void HandleTimeout(const TActorContext &ctx) {
        for (auto& nodeTabletTracesPair : TabletTraces) {
            if (!nodeTabletTracesPair.second) {
                nodeTabletTracesPair.second = THolder<TReplyStatus>(new TReplyStatus({ TReplyStatus::TIMEOUT, nullptr }));
            }
        }
        ReplyAndDie(ctx);
    }

private:
    const ui32 NodeId;
    const ui64 TabletId;
    const TActorId Sender;
    ui32 Timeout;
    ui32 Requested = 0;
    ui32 Received = 0;
    THashMap<ui32, THolder<TReplyStatus>> TabletTraces;
};

class TTraceRequestActor : public TActorBootstrapped<TTraceRequestActor> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TRACE_REQUEST_ACTOR;
    }

    TTraceRequestActor(const NTracing::TTraceInfo& traceInfo, const TActorId& sender)
        : TraceInfo(traceInfo)
        , Sender(sender)
    {}

    void Bootstrap(const TActorContext& ctx) {
        TActorId whiteboardServiceId = MakeNodeWhiteboardServiceId(TraceInfo.NodeId);
        THolder<TEvWhiteboard::TEvTraceRequest> request = MakeHolder<TEvWhiteboard::TEvTraceRequest>();
        auto& record = request->Record;
        record.SetTabletID(TraceInfo.TabletId);
        NTracing::TraceIDFromTraceID(TraceInfo.TraceId, record.MutableTraceID());
        record.SetMode(TraceInfo.TimestampInfo.Mode);
        record.SetPrecision(TraceInfo.TimestampInfo.Precision);
        record.SetTabletID(TraceInfo.TabletId);
        ctx.Send(whiteboardServiceId, request.Release(), IEventHandle::FlagTrackDelivery, TraceInfo.NodeId);
        Become(&TThis::StateTraceRequested);
    }

    STFUNC(StateTraceRequested) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvWhiteboard::TEvTraceResponse, Handle);
            CFunc(TEvents::TEvUndelivered::EventType, Undelivered);
            CFunc(TEvInterconnect::TEvNodeDisconnected::EventType, Disconnected);
        }
    }

    void Handle(TEvWhiteboard::TEvTraceResponse::TPtr& ev, const TActorContext& ctx) {
        TStringStream str;
        try {
            HTML(str) {
                NTracing::NHttp::OutputStaticPart(str);
                TAG(TH4) {
                    str << "Trace " << TraceInfo.TraceId << "<br>at Tablet "
                        << "<a href=\"tablet?iTabletID=" << TraceInfo.TabletId << "\">" << TraceInfo.TabletId << "</a>"
                        << " on Node " << TraceInfo.NodeId << ":";
                }
                auto& record = ev->Get()->Record;
                auto& tabletTrace = record.GetTrace();
                str << tabletTrace;
                AddTraceLookupForm(str);
            }
        }
        catch (yexception&) {
            Notify(ctx, "Error rendering trace");
            Die(ctx);
            return;
        }
        Notify(ctx, str.Str());
        Die(ctx);
    }

    void Undelivered(const TActorContext &ctx) {
        Notify(ctx, "Request is undelivered");
        Die(ctx);
    }

    void Disconnected(const TActorContext &ctx) {
        Notify(ctx, "Interconnect: Node Disconnected");
        Die(ctx);
    }

    void Notify(const TActorContext &ctx, const TString& html) {
        ctx.Send(Sender, new NMon::TEvHttpInfoRes(html));
    }

private:
    const NTracing::TTraceInfo TraceInfo;
    const TActorId Sender;
};

class TSignalBodyRequestActor : public TActorBootstrapped<TSignalBodyRequestActor> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SIGNAL_BODY_REQUEST_ACTOR;
    }

    TSignalBodyRequestActor(const NTracing::TTraceInfo& traceInfo, const TString& signalId, const TActorId& sender)
        : TraceInfo(traceInfo)
        , SignalId(signalId)
        , Sender(sender)
    {}

    void Bootstrap(const TActorContext& ctx) {
        TActorId whiteboardServiceId = MakeNodeWhiteboardServiceId(TraceInfo.NodeId);
        THolder<TEvWhiteboard::TEvSignalBodyRequest> request = MakeHolder<TEvWhiteboard::TEvSignalBodyRequest>();
        auto& record = request->Record;
        record.SetTabletID(TraceInfo.TabletId);
        NTracing::TraceIDFromTraceID(TraceInfo.TraceId, record.MutableTraceID());
        record.SetMode(TraceInfo.TimestampInfo.Mode);
        record.SetPrecision(TraceInfo.TimestampInfo.Precision);
        record.SetSignalID(SignalId);
        ctx.Send(whiteboardServiceId, request.Release(), IEventHandle::FlagTrackDelivery, TraceInfo.NodeId);
        Become(&TThis::StateTraceRequested);
    }

    STFUNC(StateTraceRequested) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvWhiteboard::TEvSignalBodyResponse, Handle);
            CFunc(TEvents::TEvUndelivered::EventType, Undelivered);
            CFunc(TEvInterconnect::TEvNodeDisconnected::EventType, Disconnected);
        }
    }

    void Handle(TEvWhiteboard::TEvSignalBodyResponse::TPtr& ev, const TActorContext& ctx) {
        auto& record = ev->Get()->Record;
        auto& signalBody = record.GetSignalBody();
        TStringStream str;
        str << NMonitoring::HTTPOKTEXT;
        HTML(str) {
            DIV_CLASS("tab-left") {
                str << signalBody;
            }
        }
        Notify(ctx, str.Str());
        Die(ctx);
    }

    void Undelivered(const TActorContext &ctx) {
        Notify(ctx, "Request is undelivered");
        Die(ctx);
    }

    void Disconnected(const TActorContext &ctx) {
        Notify(ctx, "Interconnect: Node Disconnected");
        Die(ctx);
    }

    void Notify(const TActorContext &ctx, const TString& html) {
        ctx.Send(Sender, new NMon::TEvHttpInfoRes(html, 0, NMon::IEvHttpInfoRes::Custom));
    }

private:
    const NTracing::TTraceInfo TraceInfo;
    const TString SignalId;
    const TActorId Sender;
};

ui64 TryParseTabletId(TStringBuf tabletIdParam) {
    if (tabletIdParam.StartsWith("0x")) {
        return IntFromString<ui64, 16>(tabletIdParam.substr(2));
    } else {
        return FromString<ui64>(tabletIdParam);
    }
}

void RenderTabletPage(NMon::TEvHttpInfo::TPtr ev, const TActorContext &ctx, ui64 tabletId) {
    TStringStream str;
    HTML(str) {
        NTracing::NHttp::OutputStaticPart(str);
        TAG(TH3) {
            str << "TabletID: " << tabletId;
        }
        DIV_CLASS("row") {
            DIV_CLASS("col-md-12") { str << "<a href=\"tablets?TabletID=" << tabletId << "\">Tablet details</a>"; }
        }
        DIV_CLASS("row") {
            DIV_CLASS("col-md-12") { str << "<a href=\"tablets?SsId=" << tabletId << "\">State Storage</a>"; }
        }
        DIV_CLASS("row") {
            DIV_CLASS("col-md-12") {
                str << "<a href=\"tablet?TabletID=" << tabletId << "\">Introspection traces</a>";
            }
        }
        AddTraceLookupForm(str);
    }
    ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
}

}

class TTabletInfoActor : public TActorBootstrapped<TTabletInfoActor> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TABLET_INFO;
    }
    void Bootstrap(const TActorContext &ctx);
    STFUNC(StateWork);

private:
    void Handle(NMon::TEvHttpInfo::TPtr &ev, const TActorContext &ctx);
};

void TTabletInfoActor::Bootstrap(const TActorContext &ctx) {
    Become(&TThis::StateWork);

    TMon* mon = AppData(ctx)->Mon;

    if (mon) {
        mon->RegisterActorPage(nullptr, "tablet", "Tablet boot tracing", false, ctx.ExecutorThread.ActorSystem, ctx.SelfID);
    }
}

bool HasProperty(const TCgiParameters& cgi, const TString& name) {
    return cgi.Has(name) && cgi.Get(name).length();
}

void TTabletInfoActor::Handle(NMon::TEvHttpInfo::TPtr &ev, const TActorContext &ctx) {
    if (!ev) {
        return;
    }
    NMon::TEvHttpInfo* msg = ev->Get();
    const TCgiParameters& cgi = msg->Request.GetParams();
    ui32 timeout = FromStringWithDefault<ui32>(cgi.Get("timeout"), 10000);

    bool hasNodeIdParam = HasProperty(cgi, "NodeID");
    bool hasTabletIdParam = HasProperty(cgi, "TabletID");
    bool hasTraceIdParam = HasProperty(cgi, "RandomID") && HasProperty(cgi, "CreationTime");

    if (cgi.Has("SignalID")) {
        // SignalID => Signal body direct request
        const TString& signalID = cgi.Get("SignalID");
        if (!hasNodeIdParam || !hasTabletIdParam || !hasTraceIdParam || !signalID.length()) {
            ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(
                "Need NodeID, TabletID, TraceID and signal address to search for the signal body."
            ));
            return;
        }
        const TString& nodeIdParam = cgi.Get("NodeID");
        const TString& tabletIdParam = cgi.Get("TabletID");
        const TString& randomIdParam = cgi.Get("RandomID");
        const TString& creationTimeParam = cgi.Get("CreationTime");
        try {
            NTracing::TTraceInfo traceInfo = {
                FromString<ui32>(nodeIdParam),
                TryParseTabletId(tabletIdParam),
                {
                    FromString<ui64>(randomIdParam),
                    FromString<ui64>(creationTimeParam)
                },
                NTracing::TTimestampInfo(
                    HasProperty(cgi, "TMode") ?
                        static_cast<NTracing::TTimestampInfo::EMode>(FromString<ui32>(cgi.Get("TMode")))
                            : NTracing::TTimestampInfo::ModeDefault,
                    HasProperty(cgi, "TPrecision") ?
                        static_cast<NTracing::TTimestampInfo::EPrecision>(FromString<ui32>(cgi.Get("TPrecision")))
                            : NTracing::TTimestampInfo::PrecisionDefault
                )
            };
            ctx.ExecutorThread.RegisterActor(
                new TSignalBodyRequestActor(traceInfo, signalID, ev->Sender)
            );
            return;
        }
        catch (yexception&) {
            ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes("Error parsing IDs"));
            return;
        }
    }

    if (hasTraceIdParam) {
        if (!hasNodeIdParam || !hasTabletIdParam) {
            ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes("Need NodeID and TabletID to search for the trace."));
            return;
        }
        // NodeID && TabletID && TraceID => Direct trace request
        const TString& nodeIdParam = cgi.Get("NodeID");
        const TString& tabletIdParam = cgi.Get("TabletID");
        const TString& randomIdParam = cgi.Get("RandomID");
        const TString& creationTimeParam = cgi.Get("CreationTime");
        try {
            NTracing::TTraceInfo traceInfo = {
                FromString<ui32>(nodeIdParam),
                TryParseTabletId(tabletIdParam),
                {
                    FromString<ui64>(randomIdParam),
                    FromString<ui64>(creationTimeParam)
                },
                NTracing::TTimestampInfo(
                    HasProperty(cgi, "TMode") ?
                        static_cast<NTracing::TTimestampInfo::EMode>(FromString<ui32>(cgi.Get("TMode")))
                            : NTracing::TTimestampInfo::ModeDefault,
                    HasProperty(cgi, "TPrecision") ?
                        static_cast<NTracing::TTimestampInfo::EPrecision>(FromString<ui32>(cgi.Get("TPrecision")))
                            : NTracing::TTimestampInfo::PrecisionDefault
                )
            };
            ctx.ExecutorThread.RegisterActor(
                new TTraceRequestActor(traceInfo, ev->Sender)
            );
            return;
        }
        catch (yexception&) {
            ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes("Error parsing IDs"));
            return;
        }
    }

    if (hasTabletIdParam) {
        // TabletID && !TraceID => TraceLookup
        const TString& tabletIdParam = cgi.Get("TabletID");
        try {
            ui32 nodeId = hasNodeIdParam ? FromString<ui32>(cgi.Get("NodeID")) : 0;
            ui64 tabletId = TryParseTabletId(tabletIdParam);
            ctx.ExecutorThread.RegisterActor(
                new TTraceLookupActor(nodeId, tabletId, ev->Sender, timeout)
            );
            return;
        }
        catch (yexception&) {
            ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes("Error parsing IDs"));
            return;
        }
    }

    if (HasProperty(cgi, "iTabletID")) {
        const TString& tabletIdParam = cgi.Get("iTabletID");
        ui64 tabletId;
        try {
            tabletId = TryParseTabletId(tabletIdParam);
        }
        catch (yexception&) {
            ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes("Error parsing IDs"));
            return;
        }
        RenderTabletPage(ev, ctx, tabletId);
        return;
    }

    // NodeID && !TabletID && !TraceID => TabletLookup
    if (hasNodeIdParam) {
        try {
            ui32 nodeId = FromString<ui32>(cgi.Get("NodeID"));
            ctx.ExecutorThread.RegisterActor(
                new TTabletLookupActor(nodeId, ev->Sender, timeout)
            );
            return;
        }
        catch (yexception&) {
            ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes("Error parsing NodeId"));
            return;
        }
    }
    TStringStream str;

    HTML(str) {
        NTracing::NHttp::OutputStaticPart(str);
        AddTabletLookupForm(str);
        AddTraceLookupForm(str);
    }
    ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
}

STFUNC(TTabletInfoActor::StateWork) {
    switch (ev->GetTypeRewrite()) {
        HFunc(NMon::TEvHttpInfo, Handle);
        // HFunc(TEvents::TEvPoisonPill, Handle); // we do not need PoisonPill for the actor
    }
}

IActor* CreateTabletInfo() {
    return new TTabletInfoActor();
}

} } // end of the NKikimr::NTabletInfo namespace
