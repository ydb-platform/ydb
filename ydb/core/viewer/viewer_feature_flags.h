#pragma once
#include "json_pipe_req.h"
#include "viewer.h"
#include <ydb/library/yaml_config/yaml_config.h>

namespace NKikimr::NViewer {

using namespace NActors;

class TJsonFeatureFlags : public TViewerPipeClient {
    using TThis = TJsonFeatureFlags;
    using TBase = TViewerPipeClient;
    TJsonSettings JsonSettings;
    ui32 Timeout = 0;
    THashSet<TString> FilterFeatures;
    bool ChangedOnly = false;
    TRequestResponse<NConsole::TEvConsole::TEvListTenantsResponse> TenantsResponse;
    TRequestResponse<NConsole::TEvConsole::TEvGetAllConfigsResponse> AllConfigsResponse;
    std::unordered_map<TString, TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult>> PathNameNavigateKeySetResults;
    std::unordered_map<TPathId, TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult>> PathIdNavigateKeySetResults;

public:
    TJsonFeatureFlags(IViewer* viewer, NMon::TEvHttpInfo::TPtr &ev)
        : TViewerPipeClient(viewer, ev)
    {}

    void Bootstrap() override {
        if (NeedToRedirect()) {
            return;
        }
        const auto& params(Event->Get()->Request.GetParams());
        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), true);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        StringSplitter(params.Get("features")).Split(',').SkipEmpty().Collect(&FilterFeatures);
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        ChangedOnly = FromStringWithDefault<bool>(params.Get("changed"), ChangedOnly);
        if (Database && DatabaseNavigateResponse) {
            PathNameNavigateKeySetResults[Database] = std::move(*DatabaseNavigateResponse);
        } else {
            TenantsResponse = MakeRequestConsoleListTenants();
        }
        AllConfigsResponse = MakeRequestConsoleGetAllConfigs();

        Become(&TThis::StateWork, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NConsole::TEvConsole::TEvListTenantsResponse, Handle);
            hFunc(NConsole::TEvConsole::TEvGetAllConfigsResponse, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, TBase::Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void Handle(NConsole::TEvConsole::TEvListTenantsResponse::TPtr& ev) {
        if (TenantsResponse.Set(std::move(ev))) {
            if (TenantsResponse.IsOk()) {
                Ydb::Cms::ListDatabasesResult listDatabasesResult;
                TenantsResponse->Record.GetResponse().operation().result().UnpackTo(&listDatabasesResult);
                for (const TString& database : listDatabasesResult.paths()) {
                    if (PathNameNavigateKeySetResults.count(database) == 0) {
                        PathNameNavigateKeySetResults[database] = MakeRequestSchemeCacheNavigate(database);
                    }
                }
            }
            if (PathNameNavigateKeySetResults.empty()) {
                if (AppData()->DomainsInfo && AppData()->DomainsInfo->Domain) {
                    TString domain = "/" + AppData()->DomainsInfo->Domain->Name;
                    PathNameNavigateKeySetResults[domain] = MakeRequestSchemeCacheNavigate(domain);
                }
            }
            RequestDone();
        }
    }

    void Handle(NConsole::TEvConsole::TEvGetAllConfigsResponse::TPtr& ev) {
        if (AllConfigsResponse.Set(std::move(ev))) {
            RequestDone();
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        TString path = GetPath(ev);
        if (path) {
            auto it = PathNameNavigateKeySetResults.find(path);
            if (it != PathNameNavigateKeySetResults.end() && !it->second.IsDone()) {
                if (it->second.Set(std::move(ev))) {
                    if (it->second.IsOk()) {
                        TSchemeCacheNavigate::TEntry& entry(it->second->Request->ResultSet.front());
                        if (entry.DomainInfo) {
                            if (entry.DomainInfo->ResourcesDomainKey && entry.DomainInfo->DomainKey != entry.DomainInfo->ResourcesDomainKey) {
                                TPathId resourceDomainKey(entry.DomainInfo->ResourcesDomainKey);
                                if (PathIdNavigateKeySetResults.count(resourceDomainKey) == 0) {
                                    PathIdNavigateKeySetResults[resourceDomainKey] = MakeRequestSchemeCacheNavigate(resourceDomainKey);
                                }
                            }
                        }
                    }
                    RequestDone();
                }
                return;
            }
        }
        TPathId pathId = GetPathId(ev);
        if (pathId) {
            auto it = PathIdNavigateKeySetResults.find(pathId);
            if (it != PathIdNavigateKeySetResults.end() && !it->second.IsDone()) {
                if (it->second.Set(std::move(ev))) {
                    RequestDone();
                }
                return;
            }
        }
    }

    void ParseFeatureFlags(const NKikimrConfig::TFeatureFlags& featureFlags, NKikimrViewer::TFeatureFlagsConfig::TDatabase& result) {
        const google::protobuf::Reflection* reflection = featureFlags.GetReflection();
        const google::protobuf::Descriptor* descriptor = featureFlags.GetDescriptor();
        for (int i = 0; i < descriptor->field_count(); ++i) {
            const google::protobuf::FieldDescriptor* field = descriptor->field(i);
            if (field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_BOOL) {
                if (FilterFeatures.empty() || FilterFeatures.count(field->name())) {
                    bool hasField = reflection->HasField(featureFlags, field);
                    if (ChangedOnly && !hasField) {
                        continue;
                    }
                    auto flag = result.AddFeatureFlags();
                    flag->SetName(field->name());
                    if (hasField) {
                        flag->SetCurrent(reflection->GetBool(featureFlags, field));
                    }
                    if (field->has_default_value()) {
                        flag->SetDefault(field->default_value_bool());
                    }
                }
            }
        }
    }

    void ParseConfig(const TString& database,
                     const TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult>& navigate,
                     NKikimrViewer::TFeatureFlagsConfig& result) {
        if (AllConfigsResponse.IsOk()) {
            TString realDatabase = database;
            auto databaseProto = result.AddDatabases();
            databaseProto->SetName(database);
            TSchemeCacheNavigate::TEntry& entry(navigate->Request->ResultSet.front());
            if (entry.DomainInfo) {
                if (entry.DomainInfo->ResourcesDomainKey && entry.DomainInfo->DomainKey != entry.DomainInfo->ResourcesDomainKey) {
                    TPathId resourceDomainKey(entry.DomainInfo->ResourcesDomainKey);
                    auto it = PathIdNavigateKeySetResults.find(resourceDomainKey);
                    if (it != PathIdNavigateKeySetResults.end() && it->second.IsOk() && it->second->Request->ResultSet.size() == 1) {
                        realDatabase = CanonizePath(it->second->Request->ResultSet.begin()->Path);
                    }
                }
            }
            NKikimrConfig::TAppConfig appConfig;
            if (AllConfigsResponse->Record.GetResponse().config()) {
                try {
                    NYamlConfig::ResolveAndParseYamlConfig(AllConfigsResponse->Record.GetResponse().config(), {}, {{"tenant", realDatabase}}, appConfig);
                } catch (const std::exception& e) {
                    BLOG_ERROR("Failed to parse config for tenant " << realDatabase << ": " << e.what());
                }
                ParseFeatureFlags(appConfig.GetFeatureFlags(), *databaseProto);
            } else {
                ParseFeatureFlags(AppData()->FeatureFlags, *databaseProto);
            }
        }
    }

    void ReplyAndPassAway() override {
        // prepare response
        NKikimrViewer::TFeatureFlagsConfig Result;
        Result.SetVersion(Viewer->GetCapabilityVersion("/viewer/feature_flags"));
        for (const auto& [database, navigate] : PathNameNavigateKeySetResults) {
            if (navigate.IsOk()) {
                ParseConfig(database, navigate, Result);
            }
        }
        TStringStream json;
        TProtoToJson::ProtoToJson(json, Result, JsonSettings);
        TBase::ReplyAndPassAway(GetHTTPOKJSON(json.Str()));
    }

    static YAML::Node GetSwagger() {
        TSimpleYamlBuilder yaml({
            .Method = "get",
            .Tag = "viewer",
            .Summary = "Feature flags",
            .Description = "Returns feature flags of a database"
        });
        yaml.AddParameter({
            .Name = "database",
            .Description = "database name",
            .Type = "string",
            .Required = true,
        });
        yaml.AddParameter({
            .Name = "features",
            .Description = "comma separated list of features",
            .Type = "string",
        });
        yaml.AddParameter({
            .Name = "direct",
            .Description = "direct request to the node",
            .Type = "boolean",
        });
        yaml.AddParameter({
            .Name = "timeout",
            .Description = "timeout in ms",
            .Type = "integer",
        });
        yaml.SetResponseSchema(TProtoToYaml::ProtoToYamlSchema<NKikimrViewer::TFeatureFlagsConfig>());
        return yaml;
    }
};

}
