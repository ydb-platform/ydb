#pragma once
#include "json_handlers.h"
#include "json_pipe_req.h"
#include "log.h"
#include "query_autocomplete_helper.h"
#include "viewer_request.h"

namespace NKikimr::NViewer {

using namespace NActors;
using TNavigate = NSchemeCache::TSchemeCacheNavigate;

class TJsonAutocomplete : public TViewerPipeClient {
    using TThis = TJsonAutocomplete;
    using TBase = TViewerPipeClient;
    IViewer* Viewer;
    NMon::TEvHttpInfo::TPtr Event;
    TEvViewer::TEvViewerRequest::TPtr ViewerRequest;
    TJsonSettings JsonSettings;
    ui32 Timeout = 0;

    TAutoPtr<TEvViewer::TEvViewerResponse> ProxyResult;
    TAutoPtr<NConsole::TEvConsole::TEvListTenantsResponse> ConsoleResult;
    TAutoPtr<TEvTxProxySchemeCache::TEvNavigateKeySetResult> CacheResult;

    struct TSchemaWordData {
        TString Name;
        NKikimrViewer::EAutocompleteType Type;
        TString Table;
        TSchemaWordData() {}
        TSchemaWordData(const TString& name, const NKikimrViewer::EAutocompleteType type, const TString& table = "")
            : Name(name)
            , Type(type)
            , Table(table)
        {}
    };
    THashMap<TString, TSchemaWordData> Dictionary;
    TString Database;
    TVector<TString> Tables;
    TVector<TString> Paths;
    TString Prefix;
    TString SearchWord;
    ui32 Limit = 10;
    NKikimrViewer::TQueryAutocomplete Result;

    std::optional<TNodeId> SubscribedNodeId;
    std::vector<TNodeId> TenantDynamicNodes;
    bool Direct = false;
public:
    TJsonAutocomplete(IViewer* viewer, NMon::TEvHttpInfo::TPtr &ev)
        : Viewer(viewer)
        , Event(ev)
    {
        const auto& params(Event->Get()->Request.GetParams());
        InitConfig(params);
        ParseCgiParameters(params);
        if (IsPostContent()) {
            TStringBuf content = Event->Get()->Request.GetPostContent();
            ParsePostContent(content);
        }
        PrepareParameters();
    }

    // proxied request
    TJsonAutocomplete(TEvViewer::TEvViewerRequest::TPtr& ev)
        : ViewerRequest(ev)
    {
        auto& request = ViewerRequest->Get()->Record.GetAutocompleteRequest();

        Database = request.GetDatabase();
        for (auto& table: request.GetTables()) {
            Tables.emplace_back(table);
        }
        Prefix = request.GetPrefix();
        Limit = request.GetLimit();

        Timeout = ViewerRequest->Get()->Record.GetTimeout();
        Direct = true;
        PrepareParameters();
    }

    void PrepareParameters() {
        if (Database) {
            TString prefixUpToLastSlash = "";
            auto splitPos = Prefix.find_last_of('/');
            if (splitPos != std::string::npos) {
                prefixUpToLastSlash += Prefix.substr(0, splitPos);
                SearchWord = Prefix.substr(splitPos + 1);
            } else {
                SearchWord = Prefix;
            }

            if (Tables.size() == 0) {
                Paths.emplace_back(Database);
            } else {
                for (TString& table: Tables) {
                    TString path = table;
                    if (!table.StartsWith(Database)) {
                        path = Database + "/" + path;
                    }
                    path += "/" + prefixUpToLastSlash;
                    Paths.emplace_back(path);
                }
            }
        } else {
            SearchWord = Prefix;
        }
        if (Limit == 0) {
            Limit = std::numeric_limits<ui32>::max();
        }
    }

    void ParseCgiParameters(const TCgiParameters& params) {
        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), true);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        Database = params.Get("database");
        StringSplitter(params.Get("table")).Split(',').SkipEmpty().Collect(&Tables);
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
            if (requestData["table"].IsArray()) {
                for (auto& table: requestData["table"].GetArraySafe()) {
                    Tables.emplace_back(table.GetStringSafe());
                }
            }
            Prefix = Prefix.empty() ? requestData["prefix"].GetStringSafe({}) : Prefix;
            if (requestData["limit"].IsDefined()) {
                Limit = requestData["limit"].GetInteger();
            }
        }
    }

    bool IsPostContent() const {
        return NViewer::IsPostContent(Event);
    }

    TAutoPtr<NSchemeCache::TSchemeCacheNavigate> MakeSchemeCacheRequest() {
        TAutoPtr<NSchemeCache::TSchemeCacheNavigate> request(new NSchemeCache::TSchemeCacheNavigate());

        for (TString& path: Paths) {
            NSchemeCache::TSchemeCacheNavigate::TEntry entry;
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpList;
            entry.SyncVersion = false;
            entry.Path = SplitPath(path);
            request->ResultSet.emplace_back(entry);
        }

        return request;
    }

    void Bootstrap() override {
        if (ViewerRequest) {
            // handle proxied request
            SendSchemeCacheRequest();
        } else if (!Database) {
            // autocomplete database list via console request
            RequestConsoleListTenants();
        } else {
            Direct |= !Event->Get()->Request.GetHeader("X-Forwarded-From-Node").empty(); // we're already forwarding
            Direct |= (Database == AppData()->TenantName) || Database.empty(); // we're already on the right node or don't use database filter
            if (Database && !Direct) {
                // proxy request to a dynamic node of the specified database
                return RedirectToDatabase(Database);
            }
            if (Requests == 0) {
                // perform autocomplete without proxying
                SendSchemeCacheRequest();
            }
        }

        Become(&TThis::StateRequestedDescribe, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
    }

    void Connected(TEvInterconnect::TEvNodeConnected::TPtr &) {}

    void Undelivered(TEvents::TEvUndelivered::TPtr &ev) {
        if (!Direct && ev->Get()->SourceType == NViewer::TEvViewer::EvViewerRequest) {
            Direct = true;
            SendSchemeCacheRequest(); // fallback
            RequestDone();
        }
    }

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr &) {
        if (!Direct) {
            Direct = true;
            SendSchemeCacheRequest(); // fallback
            RequestDone();
        }
    }

    void SendSchemeCacheRequest() {
        SendRequest(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(MakeSchemeCacheRequest()));
    }

    void SendDynamicNodeAutocompleteRequest() {
        ui64 hash = std::hash<TString>()(Event->Get()->Request.GetRemoteAddr());

        auto itPos = std::next(TenantDynamicNodes.begin(), hash % TenantDynamicNodes.size());
        std::nth_element(TenantDynamicNodes.begin(), itPos, TenantDynamicNodes.end());

        TNodeId nodeId = *itPos;
        SubscribedNodeId = nodeId;
        TActorId viewerServiceId = MakeViewerID(nodeId);

        THolder<TEvViewer::TEvViewerRequest> request = MakeHolder<TEvViewer::TEvViewerRequest>();
        request->Record.SetTimeout(Timeout);
        auto autocompleteRequest = request->Record.MutableAutocompleteRequest();
        autocompleteRequest->SetDatabase(Database);
        for (TString& path: Paths) {
            autocompleteRequest->AddTables(path);
        }
        autocompleteRequest->SetPrefix(Prefix);
        autocompleteRequest->SetLimit(Limit);

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
            hFunc(NConsole::TEvConsole::TEvListTenantsResponse, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvents::TEvUndelivered, Undelivered);
            hFunc(TEvInterconnect::TEvNodeConnected, Connected);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Disconnected);
            hFunc(TEvViewer::TEvViewerResponse, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void ParseProxyResult() {
        if (ProxyResult == nullptr) {
            Result.add_error("Failed to collect information from ProxyResult");
            return;
        }
        if (ProxyResult->Record.HasAutocompleteResponse()) {
            Result = ProxyResult->Record.GetAutocompleteResponse();
        } else {
            Result.add_error("Proxying return empty response");
        }

    }

    void ParseConsoleResult() {
        if (ConsoleResult == nullptr) {
            Result.add_error("Failed to collect information from ConsoleResult");
            return;
        }

        Ydb::Cms::ListDatabasesResult listTenantsResult;
        ConsoleResult->Record.GetResponse().operation().result().UnpackTo(&listTenantsResult);
        for (const TString& path : listTenantsResult.paths()) {
            Dictionary[path] = TSchemaWordData(path, NKikimrViewer::ext_sub_domain);
        }
    }

    NKikimrViewer::EAutocompleteType ConvertType(TNavigate::EKind navigate) {
        switch (navigate) {
            case TNavigate::KindSubdomain:
                return NKikimrViewer::sub_domain;
            case TNavigate::KindPath:
                return NKikimrViewer::dir;
            case TNavigate::KindExtSubdomain:
                return NKikimrViewer::ext_sub_domain;
            case TNavigate::KindTable:
                return NKikimrViewer::table;
            case TNavigate::KindOlapStore:
                return NKikimrViewer::column_store;
            case TNavigate::KindColumnTable:
                return NKikimrViewer::column_table;
            case TNavigate::KindRtmr:
                return NKikimrViewer::rtmr_volume;
            case TNavigate::KindKesus:
                return NKikimrViewer::kesus;
            case TNavigate::KindSolomon:
                return NKikimrViewer::solomon_volume;
            case TNavigate::KindTopic:
                return NKikimrViewer::pers_queue_group;
            case TNavigate::KindCdcStream:
                return NKikimrViewer::cdc_stream;
            case TNavigate::KindSequence:
                return NKikimrViewer::sequence;
            case TNavigate::KindReplication:
                return NKikimrViewer::replication;
            case TNavigate::KindBlobDepot:
                return NKikimrViewer::blob_depot;
            case TNavigate::KindExternalTable:
                return NKikimrViewer::external_table;
            case TNavigate::KindExternalDataSource:
                return NKikimrViewer::external_data_source;
            case TNavigate::KindBlockStoreVolume:
                return NKikimrViewer::block_store_volume;
            case TNavigate::KindFileStore:
                return NKikimrViewer::file_store;
            case TNavigate::KindView:
                return NKikimrViewer::view;
            default:
                return NKikimrViewer::dir;
        }
    }

    void ParseCacheResult() {
        if (CacheResult == nullptr) {
            Result.add_error("Failed to collect information from CacheResult");
            return;
        }
        NSchemeCache::TSchemeCacheNavigate *navigate = CacheResult->Request.Get();
        if (navigate->ErrorCount > 0) {
            for (auto& entry: CacheResult->Request.Get()->ResultSet) {
                if (entry.Status != TSchemeCacheNavigate::EStatus::Ok) {
                    Result.add_error(TStringBuilder() << "Error receiving Navigate response: `" << CanonizePath(entry.Path) << "` has <" << ToString(entry.Status) << "> status");
                }
            }
            return;
        }
        for (auto& entry: CacheResult->Request.Get()->ResultSet) {
            TString path = CanonizePath(entry.Path);
            if (entry.ListNodeEntry) {
                for (const auto& child : entry.ListNodeEntry->Children) {
                    Dictionary[child.Name] = TSchemaWordData(child.Name, ConvertType(child.Kind), path);
                }
            };
            for (const auto& [id, column] : entry.Columns) {
                Dictionary[column.Name] = TSchemaWordData(column.Name, NKikimrViewer::column, path);
            }
            for (const auto& index : entry.Indexes) {
                Dictionary[index.GetName()] = TSchemaWordData(index.GetName(), NKikimrViewer::index, path);
            }
            for (const auto& cdcStream : entry.CdcStreams) {
                Dictionary[cdcStream.GetName()] = TSchemaWordData(cdcStream.GetName(), NKikimrViewer::cdc_stream, path);
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
            Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get(), json.Str()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        }
    }

    void ReplyAndPassAway() override {
        if (ProxyResult) {
            ParseProxyResult();
        } else if (Database) {
            ParseCacheResult();
        } else {
            ParseConsoleResult();
        }

        if (!ProxyResult) {
            Result.set_success(Result.error_size() == 0);
            if (Result.error_size() == 0) {
                auto fuzzy = FuzzySearcher<TSchemaWordData>(Dictionary);
                auto autocomplete = fuzzy.Search(SearchWord, Limit);
                Result.MutableResult()->SetTotal(autocomplete.size());
                for (TSchemaWordData& wordData: autocomplete) {
                    auto entity = Result.MutableResult()->AddEntities();
                    entity->SetName(wordData.Name);
                    entity->SetType(wordData.Type);
                    if (wordData.Table) {
                        entity->SetParent(wordData.Table);
                    }
                }
            }
        }

        SendAutocompleteResponse();
        PassAway();
    }

    void Handle(TEvViewer::TEvViewerResponse::TPtr& ev) {
        if (ev.Get()->Get()->Record.HasAutocompleteResponse()) {
            ProxyResult = ev.Release()->Release();
        } else {
            Direct = true;
            SendSchemeCacheRequest(); // fallback
        }
        RequestDone();
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

    static YAML::Node GetSwagger() {
        TSimpleYamlBuilder yaml({
            .Method = "get",
            .Tag = "viewer",
            .Summary = "Autocomplete information",
            .Description = "Returns autocomplete information about objects in the database"
        });
        yaml.AddParameter({
            .Name = "database",
            .Description = "database name",
            .Type = "string",
        });
        yaml.AddParameter({
            .Name = "table",
            .Description = "table list",
            .Type = "string",
        });
        yaml.AddParameter({
            .Name = "prefix",
            .Description = "known part of the word",
            .Type = "string",
        });
        yaml.AddParameter({
            .Name = "limit",
            .Description = "limit of entities",
            .Type = "integer",
        });
        yaml.AddParameter({
            .Name = "timeout",
            .Description = "timeout in ms",
            .Type = "integer",
        });
        yaml.AddParameter({
            .Name = "enums",
            .Description = "convert enums to strings",
            .Type = "boolean",
        });
        yaml.AddParameter({
            .Name = "ui64",
            .Description = "return ui64 as number",
            .Type = "boolean",
        });
        yaml.AddParameter({
            .Name = "direct",
            .Description = "force execution on current node",
            .Type = "boolean",
        });
        yaml.SetResponseSchema(TProtoToYaml::ProtoToYamlSchema<NKikimrViewer::TQueryAutocomplete>());
        return yaml;
    }
};

}
