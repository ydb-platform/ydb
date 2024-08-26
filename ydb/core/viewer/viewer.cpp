#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/core/mon/mon.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/core/base/appdata.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <ydb/library/actors/core/interconnect.h>
#include <util/generic/algorithm.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_types.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/base/statestorage.h>
#include <library/cpp/mime/types/mime.h>
#include <library/cpp/lwtrace/all.h>
#include <library/cpp/lwtrace/mon/mon_lwtrace.h>
#include <util/system/fstat.h>
#include <util/stream/file.h>
#include "viewer.h"
#include "viewer_request.h"
#include "viewer_probes.h"
#include <ydb/core/viewer/json/json.h>
#include <ydb/core/util/wildcard.h>
#include "browse_pq.h"
#include "browse_db.h"
#include "counters_hosts.h"
#include "json_healthcheck.h"

#include "json_handlers.h"

#include "json_bsgroupinfo.h"
#include "json_nodeinfo.h"
#include "json_vdiskinfo.h"


namespace NKikimr {
namespace NViewer {

using namespace NNodeWhiteboard;

void SetupPQVirtualHandlers(IViewer* viewer) {
    viewer->RegisterVirtualHandler(
        NKikimrViewer::EObjectType::Root,
        [] (const TActorId& owner, const IViewer::TBrowseContext& browseContext) -> IActor* {
            return new NViewerPQ::TBrowseRoot(owner, browseContext);
        });
    viewer->RegisterVirtualHandler(
        NKikimrViewer::EObjectType::Consumers,
        [] (const TActorId& owner, const IViewer::TBrowseContext& browseContext) -> IActor* {
            return new NViewerPQ::TBrowseConsumers(owner, browseContext);
        });
    viewer->RegisterVirtualHandler(
        NKikimrViewer::EObjectType::Consumer,
        [] (const TActorId& owner, const IViewer::TBrowseContext& browseContext) -> IActor* {
            return new NViewerPQ::TBrowseConsumer(owner, browseContext);
        });
    viewer->RegisterVirtualHandler(
        NKikimrViewer::EObjectType::Topic,
        [] (const TActorId& owner, const IViewer::TBrowseContext& browseContext) -> IActor* {
            return new NViewerPQ::TBrowseTopic(owner, browseContext);
        });
}

void SetupDBVirtualHandlers(IViewer* viewer) {
    viewer->RegisterVirtualHandler(
        NKikimrViewer::EObjectType::Table,
        [] (const TActorId& owner, const IViewer::TBrowseContext& browseContext) -> IActor* {
            return new NViewerDB::TBrowseTable(owner, browseContext);
        });
}

class TViewer : public TActorBootstrapped<TViewer>, public IViewer {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TABLET_MONITORING_PROXY;
    }

    TViewer(const TKikimrRunConfig& kikimrRunConfig)
        : KikimrRunConfig(kikimrRunConfig)
    {
        CurrentMonitoringPort = KikimrRunConfig.AppConfig.GetMonitoringConfig().GetMonitoringPort();
        CurrentWorkerName = TStringBuilder() << FQDNHostName() << ":" << CurrentMonitoringPort;
    }

    void Bootstrap(const TActorContext& ctx) {
        Become(&TThis::StateWork);
        NActors::TMon* mon = AppData(ctx)->Mon;
        if (mon) {
            NLwTraceMonPage::ProbeRegistry().AddProbesList(LWTRACE_GET_PROBES(VIEWER_PROVIDER));
            const auto& protoAllowedSIDs = KikimrRunConfig.AppConfig.GetDomainsConfig().GetSecurityConfig().GetViewerAllowedSIDs();
            TVector<TString> allowedSIDs;
            for (const auto& sid : protoAllowedSIDs) {
                allowedSIDs.emplace_back(sid);
            }
            mon->RegisterActorPage({
                .Title = "Viewer (classic)",
                .RelPath = "viewer",
                .ActorSystem = ctx.ExecutorThread.ActorSystem,
                .ActorId = ctx.SelfID,
                .UseAuth = true,
                .AllowedSIDs = allowedSIDs,
            });
            mon->RegisterActorPage({
                .Title = "Viewer",
                .RelPath = "viewer/v2",
                .ActorSystem = ctx.ExecutorThread.ActorSystem,
                .ActorId = ctx.SelfID,
            });
            mon->RegisterActorPage({
                .Title = "Monitoring",
                .RelPath = "monitoring",
                .ActorSystem = ctx.ExecutorThread.ActorSystem,
                .ActorId = ctx.SelfID,
                .UseAuth = false,
            });
            mon->RegisterActorPage({
                .RelPath = "counters/hosts",
                .ActorSystem = ctx.ExecutorThread.ActorSystem,
                .ActorId = ctx.SelfID,
                .UseAuth = false,
            });
            mon->RegisterActorPage({
                .RelPath = "healthcheck",
                .ActorSystem = ctx.ExecutorThread.ActorSystem,
                .ActorId = ctx.SelfID,
                .UseAuth = false,
            });
            mon->RegisterActorPage({
                .Title = "VDisk",
                .RelPath = "vdisk",
                .ActorSystem = ctx.ExecutorThread.ActorSystem,
                .ActorId = ctx.SelfID,
                .UseAuth = true,
                .AllowedSIDs = allowedSIDs,
            });
            auto whiteboardServiceId = NNodeWhiteboard::MakeNodeWhiteboardServiceId(ctx.SelfID.NodeId());
            ctx.Send(whiteboardServiceId, new NNodeWhiteboard::TEvWhiteboard::TEvSystemStateAddEndpoint(
                "http-mon", Sprintf(":%d", KikimrRunConfig.AppConfig.GetMonitoringConfig().GetMonitoringPort())));

            AllowOrigin = KikimrRunConfig.AppConfig.GetMonitoringConfig().GetAllowOrigin();

            ViewerJsonHandlers.Init();
            VDiskJsonHandlers.Init();

            TWhiteboardInfo<NKikimrWhiteboard::TEvNodeStateResponse>::InitMerger();
            TWhiteboardInfo<NKikimrWhiteboard::TEvBSGroupStateResponse>::InitMerger();
        }
    }

    const TKikimrRunConfig& GetKikimrRunConfig() const override {
        return KikimrRunConfig;
    }

    TString GetCORS(const NMon::TEvHttpInfo* request) override;
    TString GetHTTPOKJSON(const NMon::TEvHttpInfo* request, TString response) override;
    TString GetHTTPOKTEXT(const NMon::TEvHttpInfo* request, TString response) override;
    TString GetHTTPOK(const NMon::TEvHttpInfo* request, TString type, TString response) override;
    TString GetHTTPGATEWAYTIMEOUT(const NMon::TEvHttpInfo* request) override;
    TString GetHTTPBADREQUEST(const NMon::TEvHttpInfo* request, TString type, TString response) override;
    TString GetHTTPFORBIDDEN(const NMon::TEvHttpInfo* request) override;

    void RegisterVirtualHandler(
            NKikimrViewer::EObjectType parentObjectType,
            TVirtualHandlerType handler) override {
        VirtualHandlersByParentType.insert(std::make_pair(parentObjectType, TVirtualHandler(handler)));
    }

    TVector<const TVirtualHandler*> GetVirtualHandlers(NKikimrViewer::EObjectType type, const TString&/* path*/) const override {
        TVector<const TVirtualHandler*> handlers;
        auto its = VirtualHandlersByParentType.equal_range(type);
        for (auto it = its.first; it != its.second; ++it) {
            handlers.push_back(&it->second);
        }
        return handlers;
    }

    TContentHandler GetContentHandler(NKikimrViewer::EObjectType objectType) const override {
        auto rec = ContentHandlers.find(objectType);
        return (rec != ContentHandlers.end()) ? rec->second : (TContentHandler)nullptr;
    }

    void RegisterContentHandler(
        NKikimrViewer::EObjectType objectType,
        const TContentHandler& handler) override {
        if (handler) {
            ContentHandlers.emplace(objectType, handler);
        }
    }

private:
    TViewerJsonHandlers ViewerJsonHandlers;
    TVDiskJsonHandlers VDiskJsonHandlers;
    THashMap<TString, TAutoPtr<TJsonHandlerBase>> JsonHandlers;
    const TKikimrRunConfig KikimrRunConfig;
    std::unordered_multimap<NKikimrViewer::EObjectType, TVirtualHandler> VirtualHandlersByParentType;
    std::unordered_map<NKikimrViewer::EObjectType, TContentHandler> ContentHandlers;
    TString AllowOrigin;
    ui32 CurrentMonitoringPort;
    TString CurrentWorkerName;

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NMon::TEvHttpInfo, Handle);
            hFunc(TEvViewer::TEvViewerRequest, Handle);
        }
    }

    TString GetSwaggerJson(NMon::TEvHttpInfo::TPtr &ev) {
        TStringStream json;
        TString basepath = ev->Get()->Request.GetParams().Get("basepath");
        if (basepath.empty()) {
            basepath = ev->Get()->Request.GetPage()->Path;
        } else {
            if (basepath.EndsWith("/api/")) {
                basepath = basepath.substr(0, basepath.size() - 5);
            }
        }
        TString protocol = ev->Get()->Request.GetParams().Get("protocol");
        if (protocol.empty()) {
            protocol = "http";
        }

        json << '{';
        json << "\"swagger\":\"2.0\",";
        json << "\"info\": {";
            json << "\"version\": \"1.0.0\",";
            json << "\"title\": \"YDB Viewer\",";
            json << "\"description\": \"YDB API for external introspection\"";
        json << "},";
        json << "\"basePath\": \"/\",";
        json << "\"schemes\": [\"" << protocol << "\"],";
        json << R"___("consumes": ["application/json"],
                  "produces": ["application/json"],
                  "paths": {)___";

        ViewerJsonHandlers.PrintForSwagger(json);
        json << ',';
        VDiskJsonHandlers.PrintForSwagger(json);

        json << R"___(},"definitions":{)___";
        json << R"___(}})___";

        return json.Str();
    }

    bool ReplyWithSwaggerJson(NMon::TEvHttpInfo::TPtr &ev, const TActorContext &ctx) {
        TString json(GetSwaggerJson(ev));
        TStringStream response;
        response << "HTTP/1.1 200 Ok\r\n";
        response << "Content-Type: application/json\r\n";
        response << "Content-Length: " << json.size() << "\r\n";
        response << "\r\n";
        response.Write(json.data(), json.size());
        ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(response.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        return true;
    }

    bool ReplyWithFile(NMon::TEvHttpInfo::TPtr &ev, const TActorContext &ctx, const TString& name) {
        if (name == "/api/viewer.json") {
            return ReplyWithSwaggerJson(ev, ctx);
        }
        TString filename("content" + name);
        TString blob;
        TString type;
        TFileStat fstat(filename);
        if (fstat.IsFile()) {
            blob = TUnbufferedFileInput(filename).ReadAll();
            if (!blob.empty()) {
                type = mimetypeByExt(filename.c_str());
            }
        }
        if (blob.empty()) {
            filename = TString("viewer") + name;
            if (NResource::FindExact(filename, &blob)) {
                type = mimetypeByExt(filename.c_str());
            } else {
                filename = name;
                if (NResource::FindExact(filename, &blob)) {
                    type = mimetypeByExt(filename.c_str());
                }
            }
        }
        if (!blob.empty()) {
            if (name == "/index.html" || name == "/v2/index.html") { // we send root's index in such format that it could be embedded into existing web interface
                ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(TString(static_cast<const char*>(blob.data()), blob.size())));
            } else {
                TStringStream response;
                response << "HTTP/1.1 200 Ok\r\n";
                response << "Content-Type: " << type << "\r\n";
                response << "Content-Length: " << blob.size() << "\r\n";
                response << "\r\n";
                response.Write(blob.data(), blob.size());
                ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(response.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
            }
            return true;
        }
        return false;
    }

    void Handle(TEvViewer::TEvViewerRequest::TPtr& ev) {
        IActor* actor = CreateViewerRequestHandler(ev);
        if (actor) {
            Register(actor);
        } else {
            BLOG_ERROR("Unable to process EvViewerRequest");
            Send(ev->Sender, new TEvViewer::TEvViewerResponse(), 0, ev->Cookie);
        }
    }

    void Handle(NMon::TEvHttpInfo::TPtr &ev, const TActorContext &ctx) {
        NMon::TEvHttpInfo* msg = ev->Get();
        if (msg->Request.GetMethod() == HTTP_METHOD_OPTIONS) {
            TString url(msg->Request.GetPathInfo());
            TString type = mimetypeByExt(url.c_str());
            if (type.empty()) {
                type = "application/json";
            }
            TStringBuilder response;
            response << "HTTP/1.1 204 No Content\r\n";
            response << GetCORS(msg);
            response << "Content-Type: " + type + "\r\n"
                        "Connection: Keep-Alive\r\n\r\n";
            Send(ev->Sender, new NMon::TEvHttpInfoRes(response, 0, NMon::IEvHttpInfoRes::EContentType::Custom));
            return;
        }
        TString filename(msg->Request.GetPage()->Path + msg->Request.GetPathInfo());
        if (msg->Request.GetPathInfo().StartsWith("/json/")) {
            if (filename.StartsWith("viewer")) {
                ViewerJsonHandlers.Handle(this, ev, ctx);
                return;
            }
            if (filename.StartsWith("vdisk")) {
                VDiskJsonHandlers.Handle(this, ev, ctx);
                return;
            }
        }
        if (filename.StartsWith("counters/hosts")) {
            ctx.ExecutorThread.RegisterActor(new TCountersHostsList(this, ev));
            return;
        }
        if (filename.StartsWith("healthcheck")) {
            ctx.ExecutorThread.RegisterActor(new TJsonHealthCheck(this, ev));
            return;
        }
        // TODO: check path validity
        // TODO: cache
        if (msg->Request.GetPathInfo().StartsWith('/')) {
            if (filename.StartsWith("viewer")) {
                filename.erase(0, 6);
            } else if (filename.StartsWith("vdisk")) {
                filename.erase(0, 5);
            }
            if (IsMatchesWildcard(filename, "monitoring*/static/js/*")
            || IsMatchesWildcard(filename, "monitoring*/static/css/*")
            || IsMatchesWildcard(filename, "monitoring*/static/media/*")
            || IsMatchesWildcard(filename, "monitoring*/static/assets/fonts/*")
            || IsMatchesWildcard(filename, "monitoring*/static/favicon.png")) {
                auto resPos = filename.find("/static/");
                if (resPos != TString::npos) {
                    filename = "monitoring" + filename.substr(resPos);
                }
            } else if (filename.StartsWith("monitoring") && filename != "monitoring/index.html") {
                filename = "monitoring/index.html";
            }
            if (filename.EndsWith('/')) {
                filename += "index.html";
            }
            if (ReplyWithFile(ev, ctx, filename)) {
                return;
            }
        }
        if (msg->Request.GetPathInfo() == "/tablet") {
            ui64 id = FromStringWithDefault<ui64>(ev->Get()->Request.GetParams().Get("id"), 0);
            ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes("HTTP/1.1 302 Found\r\nLocation: ../tablets?TabletID=" + ToString(id) + "\r\n\r\n", 0, NMon::IEvHttpInfoRes::EContentType::Custom));
            return;
        }
        if (msg->Request.GetPathInfo().empty()) {
            ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes("HTTP/1.1 302 Found\r\nLocation: " + SplitPath(msg->Request.GetPage()->Path).back() + "/\r\n\r\n", 0, NMon::IEvHttpInfoRes::EContentType::Custom));
            return;
        }
        ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(NMonitoring::HTTPNOTFOUND));
    }
};

TString IViewer::TContentRequestContext::Dump() const
{
    auto typeToString = [] (int type) -> TString {
        using namespace NKikimrViewer;
        if (EObjectType_IsValid(type)) {
            return EObjectType_Name((EObjectType)type);
        } else {
            return TStringBuilder() << "unknown (" << type << ")";
        }
    };

    TStringBuilder result;
    result << "Path = " << Path << Endl;
    result << "Name = " << ObjectName << Endl;
    result << "Type = " << typeToString(Type) << Endl;

    result << "Limit = " << Limit << Endl;
    result << "Offset = " << Offset << Endl;
    result << "Key = " << Key << Endl;

    result << "JsonSettings.EnumAsNumbers = " << JsonSettings.EnumAsNumbers << Endl;
    result << "JsonSettings.UI64AsString = " << JsonSettings.UI64AsString << Endl;
    result << "Timeout = " << Timeout.MilliSeconds() << " ms" << Endl;

    return result;
}

IActor* CreateViewer(const TKikimrRunConfig& kikimrRunConfig) {
    return new TViewer(kikimrRunConfig);
}

TString TViewer::GetCORS(const NMon::TEvHttpInfo* request) {
    TStringBuilder res;
    TString origin;
    if (AllowOrigin) {
        origin = AllowOrigin;
    } else if (request && request->Request.GetHeaders().HasHeader("Origin")) {
        origin = request->Request.GetHeader("Origin");
    }
    if (origin.empty()) {
        origin = "*";
    }
    if (origin) {
        res << "Access-Control-Allow-Origin: " << origin << "\r\n"
            << "Access-Control-Allow-Credentials: true\r\n"
            << "Access-Control-Allow-Headers: Content-Type,Authorization,Origin,Accept,X-Trace-Verbosity,X-Want-Trace\r\n"
            << "Access-Control-Allow-Methods: OPTIONS, GET, POST, DELETE\r\n"
            << "Allow: OPTIONS, GET, POST, DELETE\r\n";
    }
    return res;
}

TString TViewer::GetHTTPOKJSON(const NMon::TEvHttpInfo* request, TString response) {
    TStringBuilder res;
    res << "HTTP/1.1 200 Ok\r\n"
        << "Content-Type: application/json; charset=utf-8\r\n"
        << "X-Worker-Name: " << CurrentWorkerName << "\r\n";
    res << GetCORS(request);
    if (response) {
        res << "Content-Length: " << response.size() << "\r\n";
    }
    res << "\r\n";
    if (response) {
        res << response;
    }
    return res;
}

TString TViewer::GetHTTPOKTEXT(const NMon::TEvHttpInfo* request, TString response) {
    TStringBuilder res;
    res << "HTTP/1.1 200 Ok\r\n"
        << "Content-Type: text/plain; charset=utf-8\r\n"
        << "X-Worker-Name: " << CurrentWorkerName << "\r\n";
    res << GetCORS(request);
    if (response) {
        res << "Content-Length: " << response.size() << "\r\n";
    }
    res << "\r\n";
    if (response) {
        res << response;
    }
    return res;
}

TString TViewer::GetHTTPGATEWAYTIMEOUT(const NMon::TEvHttpInfo* request) {
    TStringBuilder res;
    res << "HTTP/1.1 504 Gateway Time-out\r\n"
        << "Connection: Close\r\n"
        << "X-Worker-Name: " << FQDNHostName() << ":" << CurrentWorkerName << "\r\n";
    res << GetCORS(request);
    res << "\r\nGateway Time-out\r\n";
    return res;
}

TString TViewer::GetHTTPBADREQUEST(const NMon::TEvHttpInfo* request, TString contentType, TString response) {
    TStringBuilder res;
    res << "HTTP/1.1 400 Bad Request\r\n"
        << "Connection: Close\r\n";
    if (contentType) {
        res << "Content-Type: " << contentType << "\r\n";
    }
    res << GetCORS(request);
    res << "\r\n";
    if (response) {
        res << response;
    }
    return res;
}

TString TViewer::GetHTTPFORBIDDEN(const NMon::TEvHttpInfo* request) {
    TStringBuilder res;
    res << "HTTP/1.1 403 Forbidden\r\n"
        << "Content-Type: application/json; charset=utf-8\r\n"
        << "Connection: Close\r\n";
    res << GetCORS(request);
    res << "\r\n";
    return res;
}

TString TViewer::GetHTTPOK(const NMon::TEvHttpInfo* request, TString contentType = {}, TString response = {}) {
    TStringBuilder res;
    res << "HTTP/1.1 200 Ok\r\n"
        << "Content-Type: " << contentType << "\r\n"
        << "X-Worker-Name: " << CurrentWorkerName << "\r\n";
    res << GetCORS(request);
    if (response) {
        res << "Content-Length: " << response.size() << "\r\n";
    }
    res << "\r\n";
    if (response) {
        res << response;
    }
    return res;
}

NKikimrViewer::EFlag GetFlagFromTabletState(NKikimrWhiteboard::TTabletStateInfo::ETabletState state) {
    NKikimrViewer::EFlag flag = NKikimrViewer::EFlag::Grey;
    switch (state) {
    case NKikimrWhiteboard::TTabletStateInfo::Dead:
        flag = NKikimrViewer::EFlag::Red;
        break;
    case NKikimrWhiteboard::TTabletStateInfo::Created:
    case NKikimrWhiteboard::TTabletStateInfo::ResolveStateStorage:
    case NKikimrWhiteboard::TTabletStateInfo::Candidate:
    case NKikimrWhiteboard::TTabletStateInfo::BlockBlobStorage:
    case NKikimrWhiteboard::TTabletStateInfo::WriteZeroEntry:
    case NKikimrWhiteboard::TTabletStateInfo::Restored:
    case NKikimrWhiteboard::TTabletStateInfo::Discover:
    case NKikimrWhiteboard::TTabletStateInfo::Lock:
    case NKikimrWhiteboard::TTabletStateInfo::RebuildGraph:
    case NKikimrWhiteboard::TTabletStateInfo::ResolveLeader:
        flag = NKikimrViewer::EFlag::Yellow;
        break;
    case NKikimrWhiteboard::TTabletStateInfo::Deleted:
    case NKikimrWhiteboard::TTabletStateInfo::Active:
        flag = NKikimrViewer::EFlag::Green;
        break;
    default:
        break;
    }
    return flag;
}

NKikimrViewer::EFlag GetFlagFromTabletState(NKikimrHive::ETabletVolatileState state) {
    NKikimrViewer::EFlag flag = NKikimrViewer::EFlag::Grey;
    switch (state) {
        case NKikimrHive::ETabletVolatileState::TABLET_VOLATILE_STATE_STOPPED:
            flag = NKikimrViewer::EFlag::Red;
            break;
        case NKikimrHive::ETabletVolatileState::TABLET_VOLATILE_STATE_BOOTING:
            flag = NKikimrViewer::EFlag::Orange;
            break;
        case NKikimrHive::ETabletVolatileState::TABLET_VOLATILE_STATE_STARTING:
            flag = NKikimrViewer::EFlag::Yellow;
            break;
        case NKikimrHive::ETabletVolatileState::TABLET_VOLATILE_STATE_RUNNING:
            flag = NKikimrViewer::EFlag::Green;
            break;
        default:
            flag = NKikimrViewer::EFlag::Red;
            break;
    }
    return flag;
}

NKikimrViewer::EFlag GetFlagFromUsage(double usage) {
    NKikimrViewer::EFlag flag = NKikimrViewer::EFlag::Grey;
    if (usage >= 0.94) {
        flag = NKikimrViewer::EFlag::Red;
    } else if (usage >= 0.92) {
        flag = NKikimrViewer::EFlag::Orange;
    } else if (usage >= 0.85) {
        flag = NKikimrViewer::EFlag::Yellow;
    } else  {
        flag = NKikimrViewer::EFlag::Green;
    }
    return flag;
}

NKikimrViewer::EFlag GetPDiskStateFlag(const NKikimrWhiteboard::TPDiskStateInfo& info) {
    NKikimrViewer::EFlag flag = NKikimrViewer::EFlag::Grey;
    switch (info.GetState()) {
        case NKikimrBlobStorage::TPDiskState::Normal:
            flag = NKikimrViewer::EFlag::Green;
            break;
        case NKikimrBlobStorage::TPDiskState::Initial:
        case NKikimrBlobStorage::TPDiskState::InitialFormatRead:
        case NKikimrBlobStorage::TPDiskState::InitialSysLogRead:
        case NKikimrBlobStorage::TPDiskState::InitialCommonLogRead:
            flag = NKikimrViewer::EFlag::Yellow;
            break;
        case NKikimrBlobStorage::TPDiskState::InitialFormatReadError:
        case NKikimrBlobStorage::TPDiskState::InitialSysLogReadError:
        case NKikimrBlobStorage::TPDiskState::InitialSysLogParseError:
        case NKikimrBlobStorage::TPDiskState::InitialCommonLogReadError:
        case NKikimrBlobStorage::TPDiskState::InitialCommonLogParseError:
        case NKikimrBlobStorage::TPDiskState::CommonLoggerInitError:
        case NKikimrBlobStorage::TPDiskState::OpenFileError:
            flag = NKikimrViewer::EFlag::Red;
            break;
        default:
            flag = NKikimrViewer::EFlag::Red;
            break;
    }
    return flag;
}

NKikimrViewer::EFlag GetPDiskOverallFlag(const NKikimrWhiteboard::TPDiskStateInfo& info) {
    NKikimrViewer::EFlag flag = NKikimrViewer::EFlag::Grey;
    flag = Max(flag, GetPDiskStateFlag(info));
    if (info.HasDevice()) {
        flag = Max(flag, Min(NKikimrViewer::EFlag::Orange, GetViewerFlag(info.GetDevice())));
    }
    if (info.HasRealtime()) {
        flag = Max(flag, Min(NKikimrViewer::EFlag::Orange, GetViewerFlag(info.GetRealtime())));
    }
    if (info.HasAvailableSize() && info.GetTotalSize() != 0) {
        double avail = (double)info.GetAvailableSize() / info.GetTotalSize();
        if (avail <= 0.06) {
            flag = Max(flag, NKikimrViewer::EFlag::Red);
        } else if (avail <= 0.08) {
            flag = Max(flag, NKikimrViewer::EFlag::Orange);
        } else if (avail <= 0.15) {
            flag = Max(flag, NKikimrViewer::EFlag::Yellow);
        }
    }
    return flag;
}

NKikimrViewer::EFlag GetVDiskOverallFlag(const NKikimrWhiteboard::TVDiskStateInfo& info) {
    NKikimrViewer::EFlag flag = NKikimrViewer::EFlag::Grey;
    switch (info.GetVDiskState()) {
        case NKikimrWhiteboard::EVDiskState::Initial:
        case NKikimrWhiteboard::EVDiskState::SyncGuidRecovery:
            flag = NKikimrViewer::EFlag::Yellow;
            break;
        case NKikimrWhiteboard::EVDiskState::LocalRecoveryError:
        case NKikimrWhiteboard::EVDiskState::SyncGuidRecoveryError:
        case NKikimrWhiteboard::EVDiskState::PDiskError:
            flag = NKikimrViewer::EFlag::Red;
            break;
        case NKikimrWhiteboard::EVDiskState::OK:
            flag = NKikimrViewer::EFlag::Green;
            break;
        default:
            flag = NKikimrViewer::EFlag::Red;
            break;
    }
    if (info.HasDiskSpace()) {
        flag = Max(flag, GetViewerFlag(info.GetDiskSpace()));
    }
    if (info.HasSatisfactionRank()
            && info.GetSatisfactionRank().HasFreshRank()
            && info.GetSatisfactionRank().GetFreshRank().HasFlag()) {
        flag = Max(flag, Min(NKikimrViewer::EFlag::Orange, GetViewerFlag(info.GetSatisfactionRank().GetFreshRank().GetFlag())));
    }
    if (info.HasSatisfactionRank()
            && info.GetSatisfactionRank().HasLevelRank()
            && info.GetSatisfactionRank().GetLevelRank().HasFlag()) {
        flag = Max(flag, Min(NKikimrViewer::EFlag::Orange, GetViewerFlag(info.GetSatisfactionRank().GetLevelRank().GetFlag())));
    }
    if (info.HasFrontQueues()) {
        flag = Max(flag, Min(NKikimrViewer::EFlag::Orange, GetViewerFlag(info.GetFrontQueues())));
    }
    if (info.HasReplicated() && !info.GetReplicated()) {
        flag = Max(flag, NKikimrViewer::EFlag::Blue);
    }
    return flag;
}

TBSGroupState GetBSGroupOverallStateWithoutLatency(
        const NKikimrWhiteboard::TBSGroupStateInfo& info,
        const TMap<NKikimrBlobStorage::TVDiskID, const NKikimrWhiteboard::TVDiskStateInfo&>& vDisksIndex,
        const TMap<std::pair<ui32, ui32>, const NKikimrWhiteboard::TPDiskStateInfo&>& pDisksIndex) {

    TBSGroupState groupState;
    groupState.Overall = NKikimrViewer::EFlag::Grey;

    const auto& vDiskIds = info.GetVDiskIds();
    std::unordered_map<ui32, ui32> failedRings;
    std::unordered_map<ui32, ui32> failedDomains;
    TVector<NKikimrViewer::EFlag> vDiskFlags;
    vDiskFlags.reserve(vDiskIds.size());
    for (auto iv = vDiskIds.begin(); iv != vDiskIds.end(); ++iv) {
        const NKikimrBlobStorage::TVDiskID& vDiskId = *iv;
        NKikimrViewer::EFlag flag = NKikimrViewer::EFlag::Grey;
        auto ie = vDisksIndex.find(vDiskId);
        if (ie != vDisksIndex.end()) {
            auto pDiskId = std::make_pair(ie->second.GetNodeId(), ie->second.GetPDiskId());
            auto ip = pDisksIndex.find(pDiskId);
            if (ip != pDisksIndex.end()) {
                const NKikimrWhiteboard::TPDiskStateInfo& pDiskInfo(ip->second);
                flag = Max(flag, GetPDiskOverallFlag(pDiskInfo));
            } else {
                flag = NKikimrViewer::EFlag::Red;
            }
            const NKikimrWhiteboard::TVDiskStateInfo& vDiskInfo(ie->second);
            flag = Max(flag, GetVDiskOverallFlag(vDiskInfo));
            if (vDiskInfo.GetDiskSpace() > NKikimrWhiteboard::EFlag::Green) {
                groupState.SpaceProblems++;
            }
        } else {
            flag = NKikimrViewer::EFlag::Red;
        }
        vDiskFlags.push_back(flag);
        if (flag == NKikimrViewer::EFlag::Red || flag == NKikimrViewer::EFlag::Blue) {
            groupState.MissingDisks++;
            ++failedRings[vDiskId.GetRing()];
            ++failedDomains[vDiskId.GetDomain()];
        }
        groupState.Overall = Max(groupState.Overall, flag);
    }

    groupState.Overall = Min(groupState.Overall, NKikimrViewer::EFlag::Yellow); // without failed rings we only allow to raise group status up to Blue/Yellow
    TString erasure = info.GetErasureSpecies();
    if (erasure == TErasureType::ErasureSpeciesName(TErasureType::ErasureNone)) {
        if (!failedDomains.empty()) {
            groupState.Overall = NKikimrViewer::EFlag::Red;
        }
    } else if (erasure == TErasureType::ErasureSpeciesName(TErasureType::ErasureMirror3dc)) {
        if (failedRings.size() > 2) {
            groupState.Overall = NKikimrViewer::EFlag::Red;
        } else if (failedRings.size() == 2) { // TODO: check for 1 ring - 1 domain rule
            groupState.Overall = NKikimrViewer::EFlag::Orange;
        } else if (failedRings.size() > 0) {
            groupState.Overall = Min(groupState.Overall, NKikimrViewer::EFlag::Yellow);
        }
    } else if (erasure == TErasureType::ErasureSpeciesName(TErasureType::Erasure4Plus2Block)) {
        if (failedDomains.size() > 2) {
            groupState.Overall = NKikimrViewer::EFlag::Red;
        } else if (failedDomains.size() > 1) {
            groupState.Overall = NKikimrViewer::EFlag::Orange;
        } else if (failedDomains.size() > 0) {
            groupState.Overall = Min(groupState.Overall, NKikimrViewer::EFlag::Yellow);
        }
    }
    return groupState;
}

NKikimrViewer::EFlag GetBSGroupOverallFlagWithoutLatency(
        const NKikimrWhiteboard::TBSGroupStateInfo& info,
        const TMap<NKikimrBlobStorage::TVDiskID, const NKikimrWhiteboard::TVDiskStateInfo&>& vDisksIndex,
        const TMap<std::pair<ui32, ui32>, const NKikimrWhiteboard::TPDiskStateInfo&>& pDisksIndex) {
    return GetBSGroupOverallStateWithoutLatency(info, vDisksIndex, pDisksIndex).Overall;
}

TBSGroupState GetBSGroupOverallState(
        const NKikimrWhiteboard::TBSGroupStateInfo& info,
        const TMap<NKikimrBlobStorage::TVDiskID, const NKikimrWhiteboard::TVDiskStateInfo&>& vDisksIndex,
        const TMap<std::pair<ui32, ui32>, const NKikimrWhiteboard::TPDiskStateInfo&>& pDisksIndex) {
    TBSGroupState state = GetBSGroupOverallStateWithoutLatency(info, vDisksIndex, pDisksIndex);
    if (info.HasLatency()) {
        state.Overall = Max(state.Overall, Min(NKikimrViewer::EFlag::Yellow, GetViewerFlag(info.GetLatency())));
    }
    return state;
}

NKikimrViewer::EFlag GetBSGroupOverallFlag(
        const NKikimrWhiteboard::TBSGroupStateInfo& info,
        const TMap<NKikimrBlobStorage::TVDiskID, const NKikimrWhiteboard::TVDiskStateInfo&>& vDisksIndex,
        const TMap<std::pair<ui32, ui32>, const NKikimrWhiteboard::TPDiskStateInfo&>& pDisksIndex) {
    return GetBSGroupOverallState(info, vDisksIndex, pDisksIndex).Overall;
}

NKikimrWhiteboard::EFlag GetWhiteboardFlag(NKikimrViewer::EFlag flag) {
    switch (flag) {
    case NKikimrViewer::EFlag::Grey:
    case NKikimrViewer::EFlag::EFlag_INT_MIN_SENTINEL_DO_NOT_USE_:
    case NKikimrViewer::EFlag::EFlag_INT_MAX_SENTINEL_DO_NOT_USE_:
        return NKikimrWhiteboard::EFlag::Grey;
    case NKikimrViewer::EFlag::Green:
        return NKikimrWhiteboard::EFlag::Green;
    case NKikimrViewer::EFlag::Blue:
        return NKikimrWhiteboard::EFlag::Green;
    case NKikimrViewer::EFlag::Yellow:
        return NKikimrWhiteboard::EFlag::Yellow;
    case NKikimrViewer::EFlag::Orange:
        return NKikimrWhiteboard::EFlag::Orange;
    case NKikimrViewer::EFlag::Red:
        return NKikimrWhiteboard::EFlag::Red;
    }
}

NKikimrViewer::EFlag GetViewerFlag(NKikimrWhiteboard::EFlag flag) {
    switch (flag) {
    case NKikimrWhiteboard::EFlag::Grey:
        return NKikimrViewer::EFlag::Grey;
    case NKikimrWhiteboard::EFlag::Green:
        return NKikimrViewer::EFlag::Green;
    case NKikimrWhiteboard::EFlag::Yellow:
        return NKikimrViewer::EFlag::Yellow;
    case NKikimrWhiteboard::EFlag::Orange:
        return NKikimrViewer::EFlag::Orange;
    case NKikimrWhiteboard::EFlag::Red:
        return NKikimrViewer::EFlag::Red;
    }
    return static_cast<NKikimrViewer::EFlag>((int)flag);
}


} // NNodeTabletMonitor
} // NKikimr
