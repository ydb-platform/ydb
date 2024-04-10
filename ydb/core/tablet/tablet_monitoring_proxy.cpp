#include "tablet_monitoring_proxy.h"

#include <ydb/library/actors/core/log.h>
#include <ydb/core/mon/mon.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/tx/tx.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <util/string/builder.h>

////////////////////////////////////////////
namespace NKikimr { namespace NTabletMonitoringProxy {

namespace {

bool IsFormUrlencoded(const NMonitoring::IMonHttpRequest& request) {
    auto *header = request.GetHeaders().FindHeader("Content-Type");
    if (!header) {
        return false;
    }
    TStringBuf value = header->Value();
    const TStringBuf contentType = value.NextTok(';');
    return contentType == "application/x-www-form-urlencoded";
}

class TForwardingActor : public TActorBootstrapped<TForwardingActor> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TABLET_FORWARDING_ACTOR;
    }

    TForwardingActor(const TTabletMonitoringProxyConfig& config, ui64 targetTablet, bool forceFollower, const TActorId& sender, const NMonitoring::IMonHttpRequest& request, const TString& userToken)
        : Config(config)
        , TargetTablet(targetTablet)
        , ForceFollower(forceFollower)
        , Sender(sender)
        , Request(ConvertRequestToProtobuf(request, userToken))
    {}

    static NActorsProto::TRemoteHttpInfo ConvertRequestToProtobuf(const NMonitoring::IMonHttpRequest& request, const TString& userToken) {
        NActorsProto::TRemoteHttpInfo pb;
        pb.SetMethod(request.GetMethod());
        pb.SetPath(TString(request.GetPathInfo()));
        for (const auto& [key, value] : request.GetParams()) {
            auto *p = pb.AddQueryParams();
            p->SetKey(key);
            p->SetValue(value);
        }
        if (request.GetMethod() == HTTP_METHOD_POST && IsFormUrlencoded(request)) {
            for (const auto& [key, value] : request.GetPostParams()) {
                auto *p = pb.AddPostParams();
                p->SetKey(key);
                p->SetValue(value);
            }
        }
        if (const auto& content = request.GetPostContent()) {
            pb.SetPostContent(content.data(), content.size());
        }
        for (const auto& header : request.GetHeaders()) {
            auto *p = pb.AddHeaders();
            p->SetName(header.Name());
            p->SetValue(header.Value());
        }
        if (const auto& addr = request.GetRemoteAddr()) {
            pb.SetRemoteAddr(addr.data(), addr.size());
        }
        pb.SetUserToken(userToken);
        return pb;
    }

    void Bootstrap(const TActorContext& ctx) {
        NTabletPipe::TClientConfig config;
        config.AllowFollower = ForceFollower;
        config.ForceFollower = ForceFollower;
        config.PreferLocal = Config.PreferLocal;
        config.RetryPolicy = Config.RetryPolicy;

        PipeClient = ctx.ExecutorThread.RegisterActor(NTabletPipe::CreateClient(ctx.SelfID, TargetTablet, config));
        NTabletPipe::SendData(ctx, PipeClient, new NMon::TEvRemoteHttpInfo(std::move(Request)));

        ctx.Schedule(TDuration::Seconds(60), new TEvents::TEvWakeup());
        Become(&TThis::StateWork);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(NMon::TEvRemoteHttpInfoRes, Handle);
            HFunc(NMon::TEvRemoteBinaryInfoRes, Handle);
            HFunc(NMon::TEvRemoteJsonInfoRes, Handle);
            CFunc(TEvents::TSystem::Wakeup, Wakeup);
        }
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx) {
        if (!ev->Get()->ServerId) {
            auto reply = Sprintf("Tablet pipe with %" PRIu64 " is not connected with status: %s"
                                 " (<a href=\"?SsId=%" PRIu64 "\">see State Storage</a>)",
                                 ev->Get()->TabletId,
                                 NKikimrProto::EReplyStatus_Name(ev->Get()->Status).c_str(),
                                 ev->Get()->TabletId);
            Notify(ctx, reply);
            Die(ctx);
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);
        Notify(ctx, "Tablet pipe is reset");
        Die(ctx);
    }

    void Handle(NMon::TEvRemoteHttpInfoRes::TPtr &ev, const TActorContext &ctx) {
        Notify(ctx, ev->Get()->Html);
        Detach(ctx);
    }

    void Handle(NMon::TEvRemoteBinaryInfoRes::TPtr& ev, const TActorContext& ctx) {
        ctx.Send(Sender, new NMon::TEvHttpInfoRes(ev->Get()->Blob, 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        Detach(ctx);
    }

    TString GetCORS() {
        TStringBuilder res;
        TString origin;
        for (const auto& header : Request.headers()) {
            if (header.name() == "Origin") {
                origin = header.value();
            }
        }
        if (origin.empty()) {
            origin = "*";
        }
        res << "Access-Control-Allow-Origin: " << origin << "\r\n";
        res << "Access-Control-Allow-Credentials: true\r\n";
        res << "Access-Control-Allow-Headers: Content-Type,Authorization,Origin,Accept\r\n";
        res << "Access-Control-Allow-Methods: OPTIONS, GET, POST\r\n";
        return res;
    }

    void Handle(NMon::TEvRemoteJsonInfoRes::TPtr &ev, const TActorContext &ctx) {
        TStringStream str;
        str << "HTTP/1.1 200 Ok\r\n"
            << "Content-Type: application/json\r\n"
            << GetCORS()
            << "\r\n"
            << ev->Get()->Json;

        ctx.Send(Sender, new NMon::TEvHttpInfoRes(str.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        Detach(ctx);
    }

    void Notify(const TActorContext &ctx, const TString& html) {
        ctx.Send(Sender, new NMon::TEvHttpInfoRes(html));
    }

    void Wakeup(const TActorContext &ctx) {
        Notify(ctx, "Timeout");
        Detach(ctx);
    }

    void Detach(const TActorContext &ctx) {
        NTabletPipe::CloseClient(ctx, PipeClient);
        Die(ctx);
    }

private:
    const TTabletMonitoringProxyConfig Config;
    const ui64 TargetTablet;
    const bool ForceFollower;
    const TActorId Sender;
    NActorsProto::TRemoteHttpInfo Request;
    TActorId PipeClient;
};

}

////////////////////////////////////////////
class TTabletMonitoringProxyActor : public TActorBootstrapped<TTabletMonitoringProxyActor> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TABLET_MONITORING_PROXY;
    }

    //
    TTabletMonitoringProxyActor(TTabletMonitoringProxyConfig config);
    virtual ~TTabletMonitoringProxyActor();

    //
    void Bootstrap(const TActorContext &ctx);

    //
    STFUNC(StateWork);

private:
    //
    void Handle(NMon::TEvHttpInfo::TPtr &ev, const TActorContext &ctx);

private:
    TTabletMonitoringProxyConfig Config;
};

////////////////////////////////////////////
/// The TTabletMonitoringProxyActor class
////////////////////////////////////////////
TTabletMonitoringProxyActor::TTabletMonitoringProxyActor(TTabletMonitoringProxyConfig config)
    : Config(std::move(config))
{}

////////////////////////////////////////////
TTabletMonitoringProxyActor::~TTabletMonitoringProxyActor()
{}

////////////////////////////////////////////
void
TTabletMonitoringProxyActor::Bootstrap(const TActorContext &ctx) {
    Become(&TThis::StateWork);

    NActors::TMon* mon = AppData(ctx)->Mon;

    if (mon) {
        mon->RegisterActorPage(nullptr, "tablets", "Tablets", false, ctx.ExecutorThread.ActorSystem, ctx.SelfID);
    }
}

static ui64 TryParseTabletId(TStringBuf tabletIdParam) {
    if (tabletIdParam.StartsWith("0x")) {
        ui64 result = 0;
        TryIntFromString<16, ui64>(tabletIdParam.substr(2), result);
        return result;
    } else {
        return FromStringWithDefault<ui64>(tabletIdParam);
    }
}

////////////////////////////////////////////
void
TTabletMonitoringProxyActor::Handle(NMon::TEvHttpInfo::TPtr &ev, const TActorContext &ctx) {
    //
    NMon::TEvHttpInfo* msg = ev->Get();
    const TCgiParameters* cgi;

    if (msg->Request.GetMethod() == HTTP_METHOD_POST && IsFormUrlencoded(msg->Request)) {
        cgi = &msg->Request.GetPostParams();
    } else {
        cgi = &msg->Request.GetParams();
    }

    // remove later
    if (cgi->Has("KillTabletID")) {
        const ui64 tabletId = TryParseTabletId(cgi->Get("KillTabletID"));
        if (tabletId) {
            ctx.Register(CreateTabletKiller(tabletId));
            ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes("<meta http-equiv=\"refresh\" content=\"0; tablets\" />"));
            return;
        }
    }
    //

    // temporary copy-paste
    if (cgi->Has("RestartTabletID")) {
        const ui64 tabletId = TryParseTabletId(cgi->Get("RestartTabletID"));
        if (tabletId) {
            ctx.Register(CreateTabletKiller(tabletId));
            ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes("<meta http-equiv=\"refresh\" content=\"0; tablets\" />"));
            return;
        }
    }
    //

    bool hasFollowerParam = cgi->Has("FollowerID");
    if (hasFollowerParam) {
        const TString &tabletIdParam = cgi->Get("FollowerID");
        const ui64 tabletId = TryParseTabletId(tabletIdParam);
        if (tabletId) {
            ctx.ExecutorThread.RegisterActor(new TForwardingActor(Config, tabletId, true, ev->Sender, msg->Request, msg->UserToken));
            return;
        }
    }

    bool hasIdParam = cgi->Has("TabletID");
    if (hasIdParam) {
        const TString &tabletIdParam = cgi->Get("TabletID");
        const ui64 tabletId = TryParseTabletId(tabletIdParam);
        if (tabletId) {
            ctx.ExecutorThread.RegisterActor(new TForwardingActor(Config, tabletId, false, ev->Sender, msg->Request, msg->UserToken));
            return;
        }
    }

    if (cgi->Has("SsId")) {
        const TString &ssIdParam = cgi->Get("SsId");
        const ui64 tabletId = TryParseTabletId(ssIdParam);
        if (tabletId) {
            TString url = TStringBuilder() << msg->Request.GetPathInfo() << "?" << cgi->Print();
            ctx.ExecutorThread.RegisterActor(CreateStateStorageMonitoringActor(tabletId, ev->Sender, std::move(url)));
            return;
        }
    }


    TStringStream str;

    const auto& domainsInfo = AppData(ctx)->DomainsInfo;
    HTML(str) {
        if (const auto& domain = domainsInfo->Domain) { // actually we MUST have it
            TAG(TH3) {
                str << "Domain \"" << domain->Name << "\" (id: " << domain->DomainUid << ")";
            }
            TABLE_SORTABLE_CLASS("table") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() {str << "Tablet";}
                        TABLEH() {str << "ID";}
                        TABLEH_CLASS("sorter-false") {}
                        TABLEH_CLASS("sorter-false") {}
                    }
                }
                TABLEBODY() {
                    if (const ui64 schemeRootTabletId = domain->SchemeRoot) {
                        TABLER() {
                            TABLED() {str << "<a href=\"tablets?TabletID=" << schemeRootTabletId << "\">SCHEMESHARD</a>";}
                            TABLED() {str << schemeRootTabletId;}
                            TABLED() {str << " <a href=\"tablets?SsId="
                                        << schemeRootTabletId << "\">"
                                        << "<span class=\"glyphicon glyphicon-tasks\""
                                        << " title=\"State Storage\"/>"
                                        << "</a>";}
                            TABLED() {str << "<a href='tablets?RestartTabletID=" << schemeRootTabletId << "'><span class='glyphicon glyphicon-remove' title='Restart Tablet'/></a>";}
                        }
                    }
                    ui64 tabletId = domainsInfo->GetHive();
                    TABLER() {
                        TABLED() {str << "<a href=\"tablets?TabletID=" << tabletId << "\">HIVE</a>";}
                        TABLED() {str << tabletId;}
                        TABLED() {str << " <a href=\"tablets?SsId="
                                    << tabletId << "\">"
                                    << "<span class=\"glyphicon glyphicon-tasks\""
                                    << " title=\"State Storage\" />"
                                    << "</a>";}
                        TABLED() {str << "<a href='tablets?RestartTabletID=" << tabletId << "'><span class='glyphicon glyphicon-remove' title='Restart Tablet'/></a>";}
                    }
                    tabletId = NKikimr::MakeBSControllerID();
                    TABLER() {
                        TABLED() {str << "<a href=\"tablets?TabletID=" << tabletId << "\">BS_CONTROLLER</a>";}
                        TABLED() {str << tabletId;}
                        TABLED() {str << " <a href=\"tablets?SsId="
                                    << tabletId << "\">"
                                    << "<span class=\"glyphicon glyphicon-tasks\""
                                    << " title=\"State Storage\" />"
                                    << "</a>";}
                        TABLED() {str << "<a href='tablets?RestartTabletID=" << tabletId << "'><span class='glyphicon glyphicon-remove' title='Restart Tablet'/></a>";}
                    }
                    tabletId = NKikimr::MakeCmsID();
                    TABLER() {
                        TABLED() {str << "<a href=\"tablets?TabletID=" << tabletId << "\">CMS</a>";}
                        TABLED() {str << tabletId;}
                        TABLED() {str << " <a href=\"tablets?SsId="
                                    << tabletId << "\">"
                                    << "<span class=\"glyphicon glyphicon-tasks\""
                                    << " title=\"State Storage\" />"
                                    << "</a>";}
                        TABLED() {str << "<a href='tablets?RestartTabletID=" << tabletId << "'><span class='glyphicon glyphicon-remove' title='Restart Tablet'/></a>";}
                    }
                    tabletId = NKikimr::MakeNodeBrokerID();
                    TABLER() {
                        TABLED() {str << "<a href=\"tablets?TabletID=" << tabletId << "\">NODE_BROKER</a>";}
                        TABLED() {str << tabletId;}
                        TABLED() {str << " <a href=\"tablets?SsId="
                                    << tabletId << "\">"
                                    << "<span class=\"glyphicon glyphicon-tasks\""
                                    << " title=\"State Storage\" />"
                                    << "</a>";}
                        TABLED() {str << "<a href='tablets?RestartTabletID=" << tabletId << "'><span class='glyphicon glyphicon-remove' title='Restart Tablet'/></a>";}
                    }
                    tabletId = NKikimr::MakeTenantSlotBrokerID();
                    TABLER() {
                        TABLED() {str << "<a href=\"tablets?TabletID=" << tabletId << "\">TENANT_SLOT_BROKER</a>";}
                        TABLED() {str << tabletId;}
                        TABLED() {str << " <a href=\"tablets?SsId="
                                    << tabletId << "\">"
                                    << "<span class=\"glyphicon glyphicon-tasks\""
                                    << " title=\"State Storage\" />"
                                    << "</a>";}
                        TABLED() {str << "<a href='tablets?RestartTabletID=" << tabletId << "'><span class='glyphicon glyphicon-remove' title='Restart Tablet'/></a>";}
                    }
                    tabletId = NKikimr::MakeConsoleID();
                    TABLER() {
                        TABLED() {str << "<a href=\"tablets?TabletID=" << tabletId << "\">CONSOLE</a>";}
                        TABLED() {str << tabletId;}
                        TABLED() {str << " <a href=\"tablets?SsId="
                                    << tabletId << "\">"
                                    << "<span class=\"glyphicon glyphicon-tasks\""
                                    << " title=\"State Storage\" />"
                                    << "</a>";}
                        TABLED() {str << "<a href='tablets?RestartTabletID=" << tabletId << "'><span class='glyphicon glyphicon-remove' title='Restart Tablet'/></a>";}
                    }
                    for (auto tabletId : domain->Coordinators) {
                        TABLER() {
                            TABLED() {str << "<a href=\"tablets?TabletID=" << tabletId << "\">TX_COORDINATOR</a>";}
                            TABLED() {str << tabletId;}
                            TABLED() {str << " <a href=\"tablets?SsId="
                                        << tabletId << "\">"
                                        << "<span class=\"glyphicon glyphicon-tasks\""
                                        << " title=\"State Storage\" />"
                                        << "</a>";}
                            TABLED() {str << "<a href='tablets?RestartTabletID=" << tabletId << "'><span class='glyphicon glyphicon-remove' title='Restart Tablet'/></a>";}
                        }
                    }
                    for (auto tabletId : domain->Mediators) {
                        TABLER() {
                            TABLED() {str << "<a href=\"tablets?TabletID=" << tabletId << "\">TX_MEDIATOR</a>";}
                            TABLED() {str << tabletId;}
                            TABLED() {str << " <a href=\"tablets?SsId="
                                        << tabletId << "\">"
                                        << "<span class=\"glyphicon glyphicon-tasks\""
                                        << " title=\"State Storage\" />"
                                        << "</a>";}
                            TABLED() {str << "<a href='tablets?RestartTabletID=" << tabletId << "'><span class='glyphicon glyphicon-remove' title='Restart Tablet'/></a>";}
                        }
                    }
                    for (auto tabletId : domain->TxAllocators) {
                        TABLER() {
                            TABLED() {str << "<a href=\"tablets?TabletID=" << tabletId << "\">TX_ALLOCATOR</a>";}
                            TABLED() {str << tabletId;}
                            TABLED() {str << " <a href=\"tablets?SsId="
                                        << tabletId << "\">"
                                        << "<span class=\"glyphicon glyphicon-tasks\""
                                        << " title=\"State Storage\" />"
                                        << "</a>";}
                            TABLED() {str << "<a href='tablets?RestartTabletID=" << tabletId << "'><span class='glyphicon glyphicon-remove' title='Restart Tablet'/></a>";}
                        }
                    }
                }
            }
        }
        str << "<form method=\"GET\" id=\"tblMonPrxFrm\" name=\"tblMonPrxFrm\">" << Endl;
        str << "<h2>Lookup</h2>" << Endl;
        str << "TabletID: <input type=\"text\" id=\"TabletID\" name=\"TabletID\"/>" << Endl;
        str << "<input class=\"btn btn-primary\" type=\"submit\" value=\"Watch\"/>" << Endl;
        str << "</form>" << Endl;
    }
    ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
}

////////////////////////////////////////////
/// public state functions
////////////////////////////////////////////
STFUNC(TTabletMonitoringProxyActor::StateWork) {
    switch (ev->GetTypeRewrite()) {
        HFunc(NMon::TEvHttpInfo, Handle);

        // HFunc(TEvents::TEvPoisonPill, Handle); // we do not need PoisonPill for the actor
    }
}

////////////////////////////////////////////
/// actor make function
////////////////////////////////////////////
IActor* CreateTabletMonitoringProxy(TTabletMonitoringProxyConfig config) {
    return new TTabletMonitoringProxyActor(std::move(config));
}

} } // end of the NKikimr::NCompactionService namespace
