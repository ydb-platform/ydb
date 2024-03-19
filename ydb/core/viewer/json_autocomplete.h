#pragma once
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/viewer/json/json.h>
#include "viewer.h"
#include "query_autocomplete_helper.h"

namespace NKikimr {
namespace NViewer {

using namespace NActors;
using TNavigate = NSchemeCache::TSchemeCacheNavigate;

class TJsonAutocomplete : public TViewerPipeClient<TJsonAutocomplete> {
    using TBase = TViewerPipeClient<TJsonAutocomplete>;
    IViewer* Viewer;
    NMon::TEvHttpInfo::TPtr Event;
    TEvViewer::TEvViewerRequest::TPtr ViewerRequest;
    TJsonSettings JsonSettings;
    ui32 Timeout = 0;

    TAutoPtr<NConsole::TEvConsole::TEvListTenantsResponse> ConsoleResult;
    TAutoPtr<TEvTxProxySchemeCache::TEvNavigateKeySetResult> CacheResult;

    struct SchemaWordData {
        TString Name;
        TString Type;
        TString Table;
    };
    THashMap<TString, SchemaWordData> Dictionary;
    TString Database;
    TVector<TString> Paths;
    TString Prefix;
    TString PrefixPath;
    ui32 Limit = 10;
    NKikimrViewer::TQueryAutocomplete Result;

    std::optional<TNodeId> SubscribedNodeId;
    std::vector<TNodeId> TenantDynamicNodes;
    bool Direct = false;
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TJsonAutocomplete(IViewer* viewer, NMon::TEvHttpInfo::TPtr &ev)
        : Viewer(viewer)
        , Event(ev)
    {
        Cerr << "iiiiii TJsonAutocomplete 1 " << Endl;
        const auto& params(Event->Get()->Request.GetParams());
        InitConfig(params);
        ParseCgiParameters(params);
        if (IsPostContent()) {
            TStringBuf content = Event->Get()->Request.GetPostContent();
            ParsePostContent(content);
        }
    }

    // proxied request
    TJsonAutocomplete(TEvViewer::TEvViewerRequest::TPtr& ev)
        : ViewerRequest(ev)
    {
        Cerr << "iiiiii TJsonAutocomplete 2 " << Endl;
        auto& request = ViewerRequest->Get()->Record.GetAutocompleteRequest();

        Database = request.GetDatabase();
        for (auto& path: request.GetTables()) {
            Paths.emplace_back(path);
        }
        Prefix = request.GetPrefix();

        Timeout = ViewerRequest->Get()->Record.GetTimeout();
        Direct = true;
    }

    void PreparePaths() {
        if (Paths.size() == 0) {
            Paths.emplace_back("");
        }
        TString prefixPath = "";
        auto splitPos = Prefix.find_last_of('/');
        if (splitPos != std::string::npos) {
            prefixPath += "/" + Prefix.substr(0, splitPos);
            Prefix = Prefix.substr(splitPos + 1);
        }
        for (TString& path: Paths) {
            if (!path.StartsWith(Database)) {
                path = Database + "/" + path;
            }
            path += prefixPath;
            auto splitPos = Prefix.find_last_of('/');
            if (splitPos != std::string::npos) {
                path += "/" + Prefix.substr(0, splitPos);
                Prefix = Prefix.substr(splitPos + 1);
            }
        }
    }

    void ParseCgiParameters(const TCgiParameters& params) {
        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), true);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        Database = params.Get("database");
        StringSplitter(params.Get("table")).Split(',').SkipEmpty().Collect(&Paths);
        Prefix = params.Get("prefix");
        Limit = FromStringWithDefault<ui32>(params.Get("limit"), Limit);
        Direct = FromStringWithDefault<bool>(params.Get("direct"), Direct);
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
    }

    void ParsePostContent(const TStringBuf& content) {
        static NJson::TJsonReaderConfig JsonConfig;
        NJson::TJsonValue requestData;
        bool success = NJson::ReadJsonTree(content, &JsonConfig, &requestData);
        if (success) {
            Database = Database.empty() ? requestData["database"].GetStringSafe({}) : Database;
            for (auto& table: requestData["tables"].GetArraySafe()) {
                Paths.emplace_back(table.GetStringSafe());
            }
            Prefix = Prefix.empty() ? requestData["prefix"].GetStringSafe({}) : Prefix;
        }
    }

    bool IsPostContent() {
        if (Event->Get()->Request.GetMethod() == HTTP_METHOD_POST) {
            const THttpHeaders& headers = Event->Get()->Request.GetHeaders();
            auto itContentType = FindIf(headers, [](const auto& header) { return header.Name() == "Content-Type"; });
            if (itContentType != headers.end()) {
                TStringBuf contentTypeHeader = itContentType->Value();
                TStringBuf contentType = contentTypeHeader.NextTok(';');
                return contentType == "application/json";
            }
        }
        return false;
    }

    TAutoPtr<NSchemeCache::TSchemeCacheNavigate> MakeSchemeCacheRequest() {
        TAutoPtr<NSchemeCache::TSchemeCacheNavigate> request(new NSchemeCache::TSchemeCacheNavigate());

        for (TString& path: Paths) {
            Cerr << "iiiiiiii path " << path << Endl;
            Cerr << "iiiiiiii Prefix " << Prefix << Endl;

            NSchemeCache::TSchemeCacheNavigate::TEntry entry;
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpList;
            entry.SyncVersion = false;
            entry.Path = SplitPath(path);
            request->ResultSet.emplace_back(entry);
        }

        return request;
    }

    void Bootstrap() {
        Cerr << "iiiiii Bootstrap " << Endl;
        if (ViewerRequest) {
            // proxied request
            PreparePaths();
            SendSchemeCacheRequest();
        } else if (!Database) {
            // autocomplete databases via console request
            RequestConsoleListTenants();
        } else {
            if (!Direct) {
                // autocomplete with proxy
                RequestStateStorageEndpointsLookup(Database); // to find some dynamic node and redirect there
            }
            if (Requests == 0) {
                // autocomplete without proxy
                PreparePaths();
                SendSchemeCacheRequest();
            }
        }


        Become(&TThis::StateRequestedDescribe, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
    }

    void Connected(TEvInterconnect::TEvNodeConnected::TPtr &) {}

    void Undelivered(TEvents::TEvUndelivered::TPtr &ev) {
        if (ev->Get()->SourceType == NViewer::TEvViewer::EvViewerRequest) {
            SendSchemeCacheRequest();
        }
    }

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr &) {
        SendSchemeCacheRequest();
    }

    void Handle(TEvStateStorage::TEvBoardInfo::TPtr& ev) {
        BLOG_TRACE("Received TEvBoardInfo");
        if (ev->Get()->Status == TEvStateStorage::TEvBoardInfo::EStatus::Ok) {
            for (const auto& [actorId, infoEntry] : ev->Get()->InfoEntries) {
                TenantDynamicNodes.emplace_back(actorId.NodeId());
            }
        }
        if (TenantDynamicNodes.empty()) {
            SendSchemeCacheRequest();
        } else {
            SendDynamicNodeAutocompleteRequest();
        }
    }

    void SendSchemeCacheRequest() {
        Cerr << "iiiiii SendSchemeCacheRequest " << Endl;
        SendRequest(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(MakeSchemeCacheRequest()));
    }

    void SendDynamicNodeAutocompleteRequest() {
        Cerr << "iiiiii SendDynamicNodeAutocompleteRequest " << Endl;
        ui64 hash = std::hash<TString>()(Event->Get()->Request.GetRemoteAddr());

        auto itPos = std::next(TenantDynamicNodes.begin(), hash % TenantDynamicNodes.size());
        std::nth_element(TenantDynamicNodes.begin(), itPos, TenantDynamicNodes.end());

        TNodeId nodeId = *itPos;
        SubscribedNodeId = nodeId;
        TActorId viewerServiceId = MakeViewerID(nodeId);

        Cerr << "iiiiii SendDynamicNodeAutocompleteRequest nodeId " << nodeId << Endl;
        THolder<TEvViewer::TEvViewerRequest> request = MakeHolder<TEvViewer::TEvViewerRequest>();
        request->Record.SetTimeout(Timeout);
        auto autocompleteRequest = request->Record.MutableAutocompleteRequest();
        autocompleteRequest->SetDatabase(Database);
        for (TString& path: Paths) {
            autocompleteRequest->AddTables(path);
        }
        autocompleteRequest->SetPrefix(Prefix);

        ViewerWhiteboardCookie cookie(NKikimrViewer::TEvViewerRequest::kAutocompleteRequest, nodeId);
        SendRequest(viewerServiceId, request.Release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, cookie.ToUi64());
    }

    void PassAway() override {
        if (SubscribedNodeId.has_value()) {
            Send(TActivationContext::InterconnectProxy(SubscribedNodeId.value()), new TEvents::TEvUnsubscribe());
        }
        TBase::PassAway();
        BLOG_TRACE("PassAway()");
    }

    STATEFN(StateRequestedDescribe) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvBoardInfo, Handle);
            hFunc(NConsole::TEvConsole::TEvListTenantsResponse, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvents::TEvUndelivered, Undelivered);
            hFunc(TEvInterconnect::TEvNodeConnected, Connected);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Disconnected);
            hFunc(TEvViewer::TEvViewerResponse, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    TString ConvertType(TNavigate::EKind navigate) {
        switch (navigate) {
            case TNavigate::KindSubdomain:
                return "subdomain";
            case TNavigate::KindPath:
                return "directory";
            case TNavigate::KindExtSubdomain:
                return "database";
            case TNavigate::KindTable:
                return "table";
            case TNavigate::KindOlapStore:
                return "columnStore";
            case TNavigate::KindColumnTable:
                return "columnTable";
            case TNavigate::KindRtmr:
                return "rtmrVolume";
            case TNavigate::KindKesus:
                return "kesus";
            case TNavigate::KindSolomon:
                return "solomonVolume";
            case TNavigate::KindTopic:
                return "persQueueGroup";
            case TNavigate::KindCdcStream:
                return "cdcStream";
            case TNavigate::KindSequence:
                return "sequence";
            case TNavigate::KindReplication:
                return "replication";
            case TNavigate::KindBlobDepot:
                return "blobDepot";
            case TNavigate::KindExternalTable:
                return "externalTable";
            case TNavigate::KindExternalDataSource:
                return "externalDataSource";
            case TNavigate::KindBlockStoreVolume:
                return "blockStoreVolume";
            case TNavigate::KindFileStore:
                return "fileStore";
            case TNavigate::KindView:
                return "view";
            default:
                return "directory";
        }
    }

    void ParseConsoleResult() {
        if (ConsoleResult == nullptr) {
            Result.add_error("Failed to collect information");
            return;
        }

        Ydb::Cms::ListDatabasesResult listTenantsResult;
        ConsoleResult->Record.GetResponse().operation().result().UnpackTo(&listTenantsResult);
        for (const TString& path : listTenantsResult.paths()) {
            Dictionary[path] = SchemaWordData(path, "database", "");
        }
        RequestDone();
    }

    void ParseCacheResult() {
        if (CacheResult == nullptr) {
            Result.add_error("Failed to collect information");
            return;
        }
        NSchemeCache::TSchemeCacheNavigate *navigate = CacheResult->Request.Get();
        if (navigate->ErrorCount > 0) {
            Result.add_error("Inner errors while collected information");
            return;
        }
        for (auto& entry: CacheResult->Request.Get()->ResultSet) {
            TString path = CanonizePath(entry.Path);
            if (entry.ListNodeEntry) {
                for (const auto& child : entry.ListNodeEntry->Children) {
                    Dictionary[child.Name] = SchemaWordData(child.Name, ConvertType(child.Kind), path);
                }
            };
            for (const auto& [id, column] : entry.Columns) {
                Dictionary[column.Name] = SchemaWordData(column.Name, path, "column");
            }
            for (const auto& index : entry.Indexes) {
                Dictionary[index.GetName()] = SchemaWordData(index.GetName(), path, "index");
            }
            for (const auto& cdcStream : entry.CdcStreams) {
                Dictionary[cdcStream.GetName()] = SchemaWordData(cdcStream.GetName(), path, "cdcstream");
            }
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr &ev) {
        CacheResult = ev->Release();
        RequestDone();
    }

    void Handle(NConsole::TEvConsole::TEvListTenantsResponse::TPtr& ev) {
        ConsoleResult = ev->Release();
        RequestDone();
    }

    void SendAutocompleteResponse() {
        if (ViewerRequest) {
            TEvViewer::TEvViewerResponse* viewerResponse = new TEvViewer::TEvViewerResponse();
            viewerResponse->Record.MutableAutocompleteResponse()->CopyFrom(Result);
            Send(ViewerRequest->Sender, viewerResponse);
        } else {
            TStringStream json;
            TProtoToJson::ProtoToJson(json, Result, JsonSettings);
            Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get()) + json.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        }
    }

    void ReplyAndPassAway() {
        if (!Database) {
            ParseConsoleResult();
        } else {
            ParseCacheResult();
        }

        Result.set_success(Result.error_size() == 0);
        if (Result.error_size() == 0) {
            auto fuzzy = FuzzySearcher(Dictionary);
            auto autocomplete = fuzzy.Search(Prefix, Limit);
            Result.MutableResult()->SetTotal(autocomplete.size());
            for (SchemaWordData& wordData: autocomplete) {
                auto entity = Result.MutableResult()->AddEntities();
                entity->SetName(wordData.Name);
                entity->SetType(wordData.Type);
                if (wordData.Table) {
                    entity->SetParent(wordData.Table);
                }
            }
        }

        SendAutocompleteResponse();
        PassAway();
    }

    void Handle(TEvViewer::TEvViewerResponse::TPtr& ev) {
        Result = ev.Get()->Get()->Record.GetAutocompleteResponse();
        SendAutocompleteResponse();
    }

    void HandleTimeout() {
        if (ViewerRequest) {
            Result.add_error("Request timed out");
            ReplyAndPassAway();
        } else {
            Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPGATEWAYTIMEOUT(Event->Get()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
            PassAway();
        }

    }
};

template <>
struct TJsonRequestSchema<TJsonAutocomplete> {
    static TString GetSchema() {
        TStringStream stream;
        TProtoToJson::ProtoToJsonSchema<NKikimrViewer::TQueryAutocomplete>(stream);
        return stream.Str();
    }
};

template <>
struct TJsonRequestParameters<TJsonAutocomplete> {
    static TString GetParameters() {
        return R"___([{"name":"enums","in":"query","description":"convert enums to strings","required":false,"type":"boolean"},
                      {"name":"ui64","in":"query","description":"return ui64 as number","required":false,"type":"boolean"},
                      {"name":"direct","in":"query","description":"force execution on current node","required":false,"type":"boolean"},
                      {"name":"table","in":"query","description":"table list","required":false,"type":"string"},
                      {"name":"prefix","in":"query","description":"known part of the word","required":false,"type":"string"},
                      {"name":"limit","in":"query","description":"limit of entities","required":false,"type":"integer"},
                      {"name":"timeout","in":"query","description":"timeout in ms","required":false,"type":"integer"}])___";
    }
};

template <>
struct TJsonRequestSummary<TJsonAutocomplete> {
    static TString GetSummary() {
        return "\"Tenant info (brief)\"";
    }
};

template <>
struct TJsonRequestDescription<TJsonAutocomplete> {
    static TString GetDescription() {
        return "\"Returns list of tenants\"";
    }
};

}
}
