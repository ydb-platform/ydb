
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <ydb/core/mon/mon.h>
#include <library/cpp/actors/core/mon.h>
#include <ydb/core/base/appdata.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/actors/core/interconnect.h>
#include <util/generic/algorithm.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_types.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/base/statestorage.h>
#include <library/cpp/mime/types/mime.h>
#include <util/system/fstat.h>
#include <util/stream/file.h>
#include "viewer.h"
#include <ydb/core/viewer/json/json.h>
#include <ydb/core/util/wildcard.h>
#include "json_nodelist.h"
#include "json_nodeinfo.h"
#include "json_vdiskinfo.h"
#include "json_pdiskinfo.h"
#include "json_describe.h"
#include "json_hotkeys.h"
#include "json_sysinfo.h"
#include "json_tabletinfo.h"
#include "json_hiveinfo.h"
#include "json_bsgroupinfo.h"
#include "json_bscontrollerinfo.h"
#include "json_config.h"
#include "json_counters.h"
#include "json_topicinfo.h"
#include "json_pqconsumerinfo.h"
#include "json_tabletcounters.h"
#include "json_storage.h"
#include "json_metainfo.h"
#include "json_browse.h"
#include "json_cluster.h"
#include "json_content.h"
#include "json_labeledcounters.h"
#include "json_tenants.h"
#include "json_hivestats.h"
#include "json_tenantinfo.h"
#include "json_whoami.h"
#include "json_query.h"
#include "json_netinfo.h"
#include "json_compute.h"
#include "counters_hosts.h"
#include "json_healthcheck.h"
#include "json_nodes.h"
#include "json_acl.h"

namespace NKikimr {
namespace NViewer {

using namespace NNodeWhiteboard;

class TJsonHandlerBase {
public:
    virtual ~TJsonHandlerBase() = default;
    virtual IActor* CreateRequestActor(IViewer* viewer, NMon::TEvHttpInfo::TPtr& event) = 0;
    virtual TString GetResponseJsonSchema() = 0;
    virtual TString GetRequestSummary() { return TString(); }
    virtual TString GetRequestDescription() { return TString(); }
    virtual TString GetRequestParameters() { return TString(); }
};

template <typename ActorRequestType>
class TJsonHandler : public TJsonHandlerBase {
public:
    IActor* CreateRequestActor(IViewer* viewer, NMon::TEvHttpInfo::TPtr& event) override {
        return new ActorRequestType(viewer, event);
    }

    TString GetResponseJsonSchema() override {
        static TString jsonSchema = TJsonRequestSchema<ActorRequestType>::GetSchema();
        return jsonSchema;
    }

    TString GetRequestSummary() override {
        static TString jsonSummary = TJsonRequestSummary<ActorRequestType>::GetSummary();
        return jsonSummary;
    }

    TString GetRequestDescription() override {
        static TString jsonDescription = TJsonRequestDescription<ActorRequestType>::GetDescription();
        return jsonDescription;
    }

    TString GetRequestParameters() override {
        static TString jsonParameters = TJsonRequestParameters<ActorRequestType>::GetParameters();
        return jsonParameters;
    }
};

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

    TViewer(const TKikimrRunConfig &kikimrRunConfig)
        : KikimrRunConfig(kikimrRunConfig)
    {}

    void Bootstrap(const TActorContext &ctx) {
        Become(&TThis::StateWork);
        NActors::TMon* mon = AppData(ctx)->Mon;
        if (mon) {
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
            auto whiteboardServiceId = NNodeWhiteboard::MakeNodeWhiteboardServiceId(ctx.SelfID.NodeId());
            ctx.Send(whiteboardServiceId, new NNodeWhiteboard::TEvWhiteboard::TEvSystemStateAddEndpoint(
                "http-mon", Sprintf(":%d", KikimrRunConfig.AppConfig.GetMonitoringConfig().GetMonitoringPort())));

            AllowOrigin = KikimrRunConfig.AppConfig.GetMonitoringConfig().GetAllowOrigin();

            TWhiteboardInfo<TEvWhiteboard::TEvNodeStateResponse>::InitMerger();
            TWhiteboardInfo<TEvWhiteboard::TEvBSGroupStateResponse>::InitMerger();

            JsonHandlers["/json/nodelist"] = new TJsonHandler<TJsonNodeList>;
            JsonHandlers["/json/nodeinfo"] = new TJsonHandler<TJsonNodeInfo>;
            JsonHandlers["/json/vdiskinfo"] = new TJsonHandler<TJsonVDiskInfo>;
            JsonHandlers["/json/pdiskinfo"] = new TJsonHandler<TJsonPDiskInfo>;
            JsonHandlers["/json/describe"] = new TJsonHandler<TJsonDescribe>;
            JsonHandlers["/json/hotkeys"] = new TJsonHandler<TJsonHotkeys>;
            JsonHandlers["/json/sysinfo"] = new TJsonHandler<TJsonSysInfo>;
            JsonHandlers["/json/tabletinfo"] = new TJsonHandler<TJsonTabletInfo>;
            JsonHandlers["/json/hiveinfo"] = new TJsonHandler<TJsonHiveInfo>;
            JsonHandlers["/json/bsgroupinfo"] = new TJsonHandler<TJsonBSGroupInfo>;
            JsonHandlers["/json/bscontrollerinfo"] = new TJsonHandler<TJsonBSControllerInfo>;
            JsonHandlers["/json/config"] = new TJsonHandler<TJsonConfig>;
            JsonHandlers["/json/counters"] = new TJsonHandler<TJsonCounters>;
            JsonHandlers["/json/topicinfo"] = new TJsonHandler<TJsonTopicInfo>;
            JsonHandlers["/json/pqconsumerinfo"] = new TJsonHandler<TJsonPQConsumerInfo>();
            JsonHandlers["/json/tabletcounters"] = new TJsonHandler<TJsonTabletCounters>;
            JsonHandlers["/json/storage"] = new TJsonHandler<TJsonStorage>;
            JsonHandlers["/json/metainfo"] = new TJsonHandler<TJsonMetaInfo>;
            JsonHandlers["/json/browse"] = new TJsonHandler<TJsonBrowse>;
            JsonHandlers["/json/cluster"] = new TJsonHandler<TJsonCluster>;
            JsonHandlers["/json/content"] = new TJsonHandler<TJsonContent>;
            JsonHandlers["/json/labeledcounters"] = new TJsonHandler<TJsonLabeledCounters>;
            JsonHandlers["/json/tenants"] = new TJsonHandler<TJsonTenants>;
            JsonHandlers["/json/hivestats"] = new TJsonHandler<TJsonHiveStats>;
            JsonHandlers["/json/tenantinfo"] = new TJsonHandler<TJsonTenantInfo>;
            JsonHandlers["/json/whoami"] = new TJsonHandler<TJsonWhoAmI>;
            JsonHandlers["/json/query"] = new TJsonHandler<TJsonQuery>;
            JsonHandlers["/json/netinfo"] = new TJsonHandler<TJsonNetInfo>;
            JsonHandlers["/json/compute"] = new TJsonHandler<TJsonCompute>;
            JsonHandlers["/json/healthcheck"] = new TJsonHandler<TJsonHealthCheck>;
            JsonHandlers["/json/nodes"] = new TJsonHandler<TJsonNodes>;
            JsonHandlers["/json/acl"] = new TJsonHandler<TJsonACL>;
        }
    }

    const TKikimrRunConfig& GetKikimrRunConfig() const override {
        return KikimrRunConfig;
    }

    TString GetHTTPOKJSON(const NMon::TEvHttpInfo* request, TString response) override;
    TString GetHTTPGATEWAYTIMEOUT() override;

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
    THashMap<TString, TAutoPtr<TJsonHandlerBase>> JsonHandlers;
    const TKikimrRunConfig KikimrRunConfig;
    std::unordered_multimap<NKikimrViewer::EObjectType, TVirtualHandler> VirtualHandlersByParentType;
    std::unordered_map<NKikimrViewer::EObjectType, TContentHandler> ContentHandlers;
    TString AllowOrigin;

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NMon::TEvHttpInfo, Handle);
        }
    }

    TString GetSwaggerJson(NMon::TEvHttpInfo::TPtr &ev) {
        TStringStream json;
        TString basepath = ev->Get()->Request.GetParams().Get("basepath");
        if (basepath.empty()) {
            basepath = "/viewer";
        } else {
            if (basepath.EndsWith("/api/")) {
                basepath = basepath.substr(0, basepath.size() - 5);
            }
        }
        TString protocol = ev->Get()->Request.GetParams().Get("protocol");
        if (protocol.empty()) {
            protocol = "http";
        }

        json << R"___({"swagger":"2.0",
                  "info": {
                    "version": "1.0.0",
                    "title": "YDB Viewer",
                    "description": "YDB API for external introspection"
                  },)___";
        json << "\"basePath\": \"" << basepath << "\",";
        json << "\"schemes\": [\"" << protocol << "\"],";
        json << R"___("consumes": ["application/json"],
                  "produces": ["application/json"],
                  "paths": {)___";

        for (auto itJson = JsonHandlers.begin(); itJson != JsonHandlers.end(); ++itJson) {
            if (itJson != JsonHandlers.begin()) {
                json << ',';
            }
            TString name = itJson->first;
            json << '"' << name << '"' << ":{";
                json << "\"get\":{";
                    json << "\"tags\":[\"viewer\"],";
                    json << "\"produces\":[\"application/json\"],";
                    TString summary = itJson->second->GetRequestSummary();
                    if (!summary.empty()) {
                        json << "\"summary\":" << summary << ',';
                    }
                    TString description = itJson->second->GetRequestDescription();
                    if (!description.empty()) {
                        json << "\"description\":" << description << ',';
                    }
                    TString parameters = itJson->second->GetRequestParameters();
                    if (!parameters.empty()) {
                        json << "\"parameters\":" << parameters << ',';
                    }
                    json << "\"responses\":{";
                        json << "\"200\":{";
                            TString schema = itJson->second->GetResponseJsonSchema();
                            if (!schema.empty()) {
                                json << "\"schema\":" << schema;
                            }
                        json << "}";
                    json << "}";
                json << "}";
            json << "}";
        }

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

    void Handle(NMon::TEvHttpInfo::TPtr &ev, const TActorContext &ctx) {
        NMon::TEvHttpInfo* msg = ev->Get();
        if (msg->Request.GetMethod() == HTTP_METHOD_OPTIONS) {
            TString url(msg->Request.GetPathInfo());
            TString type = mimetypeByExt(url.c_str());
            if (type.empty()) {
                type = "application/json";
            }
            if (AllowOrigin) {
                ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(
                    "HTTP/1.1 204 No Content\r\n"
                    "Access-Control-Allow-Origin: " + AllowOrigin + "\r\n"
                    "Access-Control-Allow-Credentials: true\r\n"
                    "Access-Control-Allow-Headers: Content-Type,Authorization,Origin,Accept\r\n"
                    "Access-Control-Allow-Methods: OPTIONS, GET, POST\r\n"
                    "Allow: OPTIONS, GET, POST\r\n"
                    "Content-Type: " + type + "\r\n"
                    "Connection: Keep-Alive\r\n\r\n", 0, NMon::IEvHttpInfoRes::EContentType::Custom));
            } else {
                TString origin = TString(msg->Request.GetHeader("Origin"));
                if (!origin.empty()) {
                    ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(
                        "HTTP/1.1 204 No Content\r\n"
                        "Access-Control-Allow-Origin: " + origin + "\r\n"
                        "Access-Control-Allow-Credentials: true\r\n"
                        "Access-Control-Allow-Headers: Content-Type,Authorization,Origin,Accept\r\n"
                        "Access-Control-Allow-Methods: OPTIONS, GET, POST\r\n"
                        "Allow: OPTIONS, GET, POST\r\n"
                        "Content-Type: " + type + "\r\n"
                        "Connection: Keep-Alive\r\n\r\n", 0, NMon::IEvHttpInfoRes::EContentType::Custom));
                } else {
                    ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(
                        "HTTP/1.1 204 No Content\r\n"
                        "Allow: OPTIONS, GET, POST\r\n"
                        "Content-Type: " + type + "\r\n"
                        "Connection: Keep-Alive\r\n\r\n", 0, NMon::IEvHttpInfoRes::EContentType::Custom));
                }
            }
            return;
        }
        if (msg->Request.GetPathInfo().StartsWith("/json/")) {
            auto itJson = JsonHandlers.find(msg->Request.GetPathInfo());
            if (itJson != JsonHandlers.end()) {
                try {
                    ctx.ExecutorThread.RegisterActor(itJson->second->CreateRequestActor(this, ev));
                }
                catch (const std::exception& e) {
                    ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(TString("HTTP/1.1 400 Bad Request\r\n\r\n") + e.what(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
                    return;
                }
            } else {
                ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(NMonitoring::HTTPNOTFOUND));
            }
            return;
        }
        TString filename(msg->Request.GetPage()->Path + msg->Request.GetPathInfo());
        if (filename.StartsWith("counters/hosts")) {
            ctx.ExecutorThread.RegisterActor(new TCountersHostsList(this, ev));
            return;
        }
        // TODO: check path validity
        // TODO: cache
        if (msg->Request.GetPathInfo().StartsWith('/')) {
            if (filename.StartsWith("viewer")) {
                filename.erase(0, 6);
            }
            if (IsMatchesWildcard(filename, "monitoring*/resources/js/*")
            || IsMatchesWildcard(filename, "monitoring*/resources/css/*")
            || IsMatchesWildcard(filename, "monitoring*/resources/assets/fonts/*")
            || IsMatchesWildcard(filename, "monitoring*/resources/favicon.png")) {
                auto resPos = filename.find("/resources/");
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

ui32 CurrentMonitoringPort = 8765;

IActor* CreateViewer(
    const TKikimrRunConfig &kikimrRunConfig
) {
    CurrentMonitoringPort = kikimrRunConfig.AppConfig.GetMonitoringConfig().GetMonitoringPort();
    return new TViewer(kikimrRunConfig);
}

TString TViewer::GetHTTPOKJSON(const NMon::TEvHttpInfo* request, TString response) {
    TStringBuilder res;
    TString origin;
    res << "HTTP/1.1 200 Ok\r\n"
        << "Content-Type: application/json; charset=utf-8\r\n"
        << "X-Worker-Name: " << FQDNHostName() << ":" << CurrentMonitoringPort << "\r\n";
    if (AllowOrigin) {
        origin = AllowOrigin;
    } else if (request && request->Request.GetHeaders().HasHeader("Origin")) {
        origin = request->Request.GetHeader("Origin");
    }
    if (origin) {
        res << "Access-Control-Allow-Origin: " << origin << "\r\n"
            << "Access-Control-Allow-Credentials: true\r\n"
            << "Access-Control-Allow-Headers: Content-Type,Authorization,Origin,Accept\r\n"
            << "Access-Control-Allow-Methods: OPTIONS, GET, POST\r\n";
    }
    if (response) {
        res << "Content-Length: " << response.size() << "\r\n";
    }
    res << "\r\n";
    if (response) {
        res << response;
    }
    return res;
}

TString TViewer::GetHTTPGATEWAYTIMEOUT() {
    return TStringBuilder()
            << "HTTP/1.1 504 Gateway Time-out\r\nConnection: Close\r\n"
            << "X-Worker-Name: " << FQDNHostName() << ":" << CurrentMonitoringPort << "\r\n"
            << "\r\nGateway Time-out\r\n";
}

NKikimrViewer::EFlag GetFlagFromTabletState(NKikimrWhiteboard::TTabletStateInfo::ETabletState state) {
    NKikimrViewer::EFlag flag = NKikimrViewer::EFlag::Grey;
    switch (state) {
    case NKikimrWhiteboard::TTabletStateInfo::Created:
    case NKikimrWhiteboard::TTabletStateInfo::ResolveStateStorage:
    case NKikimrWhiteboard::TTabletStateInfo::Candidate:
    case NKikimrWhiteboard::TTabletStateInfo::BlockBlobStorage:
    case NKikimrWhiteboard::TTabletStateInfo::WriteZeroEntry:
    case NKikimrWhiteboard::TTabletStateInfo::Restored:
    case NKikimrWhiteboard::TTabletStateInfo::Discover:
    case NKikimrWhiteboard::TTabletStateInfo::Lock:
    case NKikimrWhiteboard::TTabletStateInfo::Dead:
        flag = NKikimrViewer::EFlag::Red;
        break;
    case NKikimrWhiteboard::TTabletStateInfo::RebuildGraph:
        flag = NKikimrViewer::EFlag::Orange;
        break;
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
