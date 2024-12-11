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
    TEvViewer::TEvViewerRequest::TPtr ViewerRequest;
    TJsonSettings JsonSettings;
    ui32 Timeout = 0;

    std::optional<TRequestResponse<NConsole::TEvConsole::TEvListTenantsResponse>> ConsoleResult;
    std::optional<TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult>> CacheResult;

    struct TSchemaWordData {
        TString Name;
        NKikimrViewer::EAutocompleteType Type;
        TString Parent;
        std::optional<ui32> PKIndex;
        bool NotNull = false;
        TSysTables::TTableColumnInfo::EDefaultKind Default = TSysTables::TTableColumnInfo::EDefaultKind::DEFAULT_UNDEFINED;

        TSchemaWordData(const TString& name, const NKikimrViewer::EAutocompleteType type, const TString& parent = {})
            : Name(name)
            , Type(type)
            , Parent(parent)
        {}

        operator TString() const {
            return Name;
        }
    };

    TVector<TString> DatabasePath;
    std::vector<TSchemaWordData> Dictionary;
    TVector<TString> Tables;
    TVector<TString> Paths;
    TString Prefix;
    TString SearchWord;
    ui32 Limit = 10;
    NKikimrViewer::TQueryAutocomplete Result;

public:
    TJsonAutocomplete(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TBase(viewer, ev)
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
            DatabasePath = SplitPath(Database);
            auto prefixPaths = SplitPath(Prefix);
            if (Prefix.EndsWith('/')) {
                prefixPaths.emplace_back();
            }
            if (!prefixPaths.empty()) {
                SearchWord = prefixPaths.back();
                prefixPaths.pop_back();
            }
            if (!prefixPaths.empty()) {
                Paths.emplace_back(JoinPath(prefixPaths));
            }
            for (const TString& table : Tables) {
                Paths.emplace_back(table);
            }
            if (Paths.empty()) {
                Paths.emplace_back();
            }
        } else {
            SearchWord = Prefix;
        }
        if (Limit == 0) {
            Limit = 1000;
        }
    }

    void ParseCgiParameters(const TCgiParameters& params) {
        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), true);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        StringSplitter(params.Get("table")).Split(',').SkipEmpty().Collect(&Tables);
        Prefix = params.Get("prefix");
        Limit = FromStringWithDefault<ui32>(params.Get("limit"), Limit);
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
    }

    void ParsePostContent(const TStringBuf& content) {
        NJson::TJsonValue requestData;
        bool success = NJson::ReadJsonTree(content, &requestData);
        if (success) {
            Database = Database.empty() ? requestData["database"].GetStringSafe({}) : Database;
            if (requestData["table"].IsArray()) {
                for (const auto& table : requestData["table"].GetArraySafe()) {
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

    TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult> MakeRequestSchemeCacheNavigate() {
        auto request = std::make_unique<NSchemeCache::TSchemeCacheNavigate>();
        for (const TString& path : Paths) {
            NSchemeCache::TSchemeCacheNavigate::TEntry entry;
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpList;
            entry.SyncVersion = false;
            auto splittedPath = SplitPath(path);
            entry.Path = DatabasePath;
            entry.Path.insert(entry.Path.end(), splittedPath.begin(), splittedPath.end());
            request->ResultSet.emplace_back(entry);
        }
        return MakeRequest<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(MakeSchemeCacheID(),
            new TEvTxProxySchemeCache::TEvNavigateKeySet(request.release()));
    }

    void Bootstrap() override {
        if (ViewerRequest) {
            // handle proxied request
            CacheResult = MakeRequestSchemeCacheNavigate();
        } else {
            if (NeedToRedirect()) {
                return;
            }
            if (Database) {
                CacheResult = MakeRequestSchemeCacheNavigate();
            } else {
                // autocomplete database list via console request
                ConsoleResult = MakeRequestConsoleListTenants();
            }
        }

        Become(&TThis::StateRequestedDescribe, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
    }

    STATEFN(StateRequestedDescribe) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NConsole::TEvConsole::TEvListTenantsResponse, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void ParseConsoleResult() {
        Ydb::Cms::ListDatabasesResult listTenantsResult;
        ConsoleResult->Get()->Record.GetResponse().operation().result().UnpackTo(&listTenantsResult);
        for (const TString& path : listTenantsResult.paths()) {
            Dictionary.emplace_back(path, NKikimrViewer::ext_sub_domain);
        }
    }

    static NKikimrViewer::EAutocompleteType ConvertType(TNavigate::EKind navigate) {
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
        NSchemeCache::TSchemeCacheNavigate& navigate = *CacheResult->Get()->Request;
        for (auto& entry : navigate.ResultSet) {
            if (entry.Status == TSchemeCacheNavigate::EStatus::Ok) {
                if (entry.Path.size() >= DatabasePath.size()) {
                    entry.Path.erase(entry.Path.begin(), entry.Path.begin() + DatabasePath.size());
                }
                TString path = JoinPath(entry.Path);
                for (const auto& [id, column] : entry.Columns) {
                    auto& dicColumn = Dictionary.emplace_back(column.Name, NKikimrViewer::column, path);
                    if (column.KeyOrder >= 0) {
                        dicColumn.PKIndex = column.KeyOrder;
                    }
                    if (column.IsNotNullColumn) {
                        dicColumn.NotNull = true;
                    }
                    if (column.DefaultKind != TSysTables::TTableColumnInfo::DEFAULT_UNDEFINED) {
                        dicColumn.Default = column.DefaultKind;
                    }
                }
                for (const auto& index : entry.Indexes) {
                    Dictionary.emplace_back(index.GetName(), NKikimrViewer::index, path);
                }
                for (const auto& cdcStream : entry.CdcStreams) {
                    Dictionary.emplace_back(cdcStream.GetName(), NKikimrViewer::cdc_stream, path);
                }
                if (entry.ListNodeEntry) {
                    for (const auto& child : entry.ListNodeEntry->Children) {
                        Dictionary.emplace_back(child.Name, ConvertType(child.Kind), path);
                    }
                };
            } else {
                Result.add_error(TStringBuilder() << "Error receiving Navigate response: `" << CanonizePath(entry.Path) << "` has <" << ToString(entry.Status) << "> status");
            }
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        CacheResult->Set(std::move(ev));
        RequestDone();
    }

    void Handle(NConsole::TEvConsole::TEvListTenantsResponse::TPtr& ev) {
        ConsoleResult->Set(std::move(ev));
        RequestDone();
    }

    void ReplyAndPassAway() override {
        if (Viewer) {
            Result.SetVersion(Viewer->GetCapabilityVersion("/viewer/autocomplete"));
        }

        if (CacheResult) {
            if (CacheResult->IsOk()) {
                ParseCacheResult();
            } else {
                Result.add_error("Failed to collect information from CacheResult");
            }
        }

        if (ConsoleResult) {
            if (ConsoleResult->IsOk()) {
                ParseConsoleResult();
            } else {
                Result.add_error("Failed to collect information from ConsoleResult");
            }
        }

        Result.set_success(Result.error_size() == 0);
        if (Result.error_size() == 0) {
            auto autocomplete = FuzzySearcher::Search(Dictionary, SearchWord, Limit);
            Result.MutableResult()->SetTotal(autocomplete.size());
            for (const TSchemaWordData* wordData : autocomplete) {
                auto entity = Result.MutableResult()->AddEntities();
                entity->SetName(wordData->Name);
                entity->SetType(wordData->Type);
                if (wordData->Parent) {
                    entity->SetParent(wordData->Parent);
                }
                if (wordData->PKIndex) {
                    entity->SetPKIndex(*wordData->PKIndex);
                }
                if (wordData->NotNull) {
                    entity->SetNotNull(wordData->NotNull);
                }
                if (wordData->Default != TSysTables::TTableColumnInfo::DEFAULT_UNDEFINED) {
                    entity->SetDefault(static_cast<NKikimrViewer::TQueryAutocomplete_EDefaultKind>(wordData->Default));
                }
            }
        }

        if (ViewerRequest) {
            TEvViewer::TEvViewerResponse* viewerResponse = new TEvViewer::TEvViewerResponse();
            viewerResponse->Record.MutableAutocompleteResponse()->CopyFrom(Result);
            Send(ViewerRequest->Sender, viewerResponse);
            PassAway();
        } else {
            TStringStream json;
            TProtoToJson::ProtoToJson(json, Result, JsonSettings);
            TBase::ReplyAndPassAway(GetHTTPOKJSON(json.Str()));
        }
    }

    void HandleTimeout() {
        if (ViewerRequest) {
            Result.add_error("Request timed out");
            ReplyAndPassAway();
        } else {
            TBase::ReplyAndPassAway(GetHTTPGATEWAYTIMEOUT());
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
