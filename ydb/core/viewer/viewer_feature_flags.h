#pragma once
#include "json_pipe_req.h"
#include "viewer.h"

namespace NKikimr::NViewer {

using namespace NActors;

class TJsonFeatureFlags : public TViewerPipeClient {
    using TThis = TJsonFeatureFlags;
    using TBase = TViewerPipeClient;
    TJsonSettings JsonSettings;
    ui32 Timeout = 0;

    TString FilterDatabase;
    THashSet<TString> FilterFeatures;
    THashMap<ui64, TString> DatabaseByCookie;
    ui64 Cookie = 0;
    TString DomainPath;
    bool Direct = false;

    TRequestResponse<NConsole::TEvConsole::TEvListTenantsResponse> TenantsResponse;
    THashMap<TString, TRequestResponse<NConsole::TEvConsole::TEvGetNodeConfigResponse>> NodeConfigResponses;

public:
    TJsonFeatureFlags(IViewer* viewer, NMon::TEvHttpInfo::TPtr &ev)
        : TViewerPipeClient(viewer, ev)
    {}

    void MakeNodeConfigRequest(const TString& database) {
        NodeConfigResponses[database] = MakeRequestConsoleNodeConfigByTenant(database, Cookie);
        DatabaseByCookie[Cookie++] = database;
    }

    void Bootstrap() override {
        const auto& params(Event->Get()->Request.GetParams());

        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), true);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        FilterDatabase = params.Get("database");
        StringSplitter(params.Get("features")).Split(',').SkipEmpty().Collect(&FilterFeatures);
        Direct = FromStringWithDefault<bool>(params.Get("direct"), Direct);
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);

        TIntrusivePtr<TDomainsInfo> domains = AppData()->DomainsInfo;
        auto* domain = domains->GetDomain();
        DomainPath = "/" + domain->Name;

        Direct |= !TBase::Event->Get()->Request.GetHeader("X-Forwarded-From-Node").empty(); // we're already forwarding
        Direct |= (FilterDatabase == AppData()->TenantName); // we're already on the right node
        if (FilterDatabase && !Direct) {
            RequestStateStorageEndpointsLookup(FilterDatabase); // to find some dynamic node and redirect there
        } else if (!FilterDatabase) {
            MakeNodeConfigRequest(DomainPath);
            TenantsResponse = MakeRequestConsoleListTenants();
        } else {
            MakeNodeConfigRequest(FilterDatabase);
        }

        Become(&TThis::StateWork, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
    }

    void HandleReply(TEvStateStorage::TEvBoardInfo::TPtr& ev) {
        TBase::ReplyAndPassAway(MakeForward(GetNodesFromBoardReply(ev)));
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvBoardInfo, HandleReply);
            hFunc(NConsole::TEvConsole::TEvListTenantsResponse, Handle);
            hFunc(NConsole::TEvConsole::TEvGetNodeConfigResponse, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, TBase::Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void Handle(NConsole::TEvConsole::TEvListTenantsResponse::TPtr& ev) {
        TenantsResponse.Set(std::move(ev));
        Ydb::Cms::ListDatabasesResult listDatabasesResult;
        TenantsResponse->Record.GetResponse().operation().result().UnpackTo(&listDatabasesResult);
        for (const TString& path : listDatabasesResult.paths()) {
            MakeNodeConfigRequest(path);
        }
        RequestDone();
    }

    void Handle(NConsole::TEvConsole::TEvGetNodeConfigResponse::TPtr& ev) {
        TString database = DatabaseByCookie[ev.Get()->Cookie];
        NodeConfigResponses[database].Set(std::move(ev));
        RequestDone();
    }

    THashMap<TString, NKikimrViewer::TFeatureFlagsConfig::TFeatureFlag> ParseFeatureFlags(const NKikimrConfig::TFeatureFlags& featureFlags) {
        THashMap<TString, NKikimrViewer::TFeatureFlagsConfig::TFeatureFlag> features;
        const google::protobuf::Reflection* reflection = featureFlags.GetReflection();
        const google::protobuf::Descriptor* descriptor = featureFlags.GetDescriptor();

        for (int i = 0; i < descriptor->field_count(); ++i) {
            const google::protobuf::FieldDescriptor* field = descriptor->field(i);
            if (field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_BOOL) {
                auto& feat = features[field->name()];
                feat.SetName(field->name());
                if (reflection->HasField(featureFlags, field)) {
                    feat.SetCurrent(reflection->GetBool(featureFlags, field));
                }
                if (field->has_default_value()) {
                    feat.SetDefault(field->default_value_bool());
                }
            }
        }
        return features;
    }

    void ReplyAndPassAway() override {
        THashMap<TString, THashMap<TString, NKikimrViewer::TFeatureFlagsConfig::TFeatureFlag>> FeatureFlagsByDatabase;
        for (const auto& [database, response] : NodeConfigResponses) {
            NKikimrConsole::TGetNodeConfigResponse rec = response->Record;
            FeatureFlagsByDatabase[database] = ParseFeatureFlags(rec.GetConfig().GetFeatureFlags());
        }

        auto domainFeaturesIt = FeatureFlagsByDatabase.find(DomainPath);
        if (domainFeaturesIt == FeatureFlagsByDatabase.end()) {
            return TBase::ReplyAndPassAway(GetHTTPINTERNALERROR("text/plain", "No domain info from Console"));
        }

        // prepare response
        NKikimrViewer::TFeatureFlagsConfig Result;
        Result.SetVersion(Viewer->GetCapabilityVersion("/viewer/feature_flags"));
        for (const auto& [database, features] : FeatureFlagsByDatabase) {
            auto databaseProto = Result.AddDatabases();
            databaseProto->SetName(database);
            for (const auto& [name, featProto] : features) {
                if (FilterFeatures.empty() || FilterFeatures.find(name) != FilterFeatures.end()) {
                    auto flag = databaseProto->AddFeatureFlags();
                    flag->CopyFrom(featProto);
                }
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
            .Description = "Returns feature flags of each database"
        });
        yaml.AddParameter({
            .Name = "database",
            .Description = "database name",
            .Type = "string",
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
        yaml.SetResponseSchema(TProtoToYaml::ProtoToYamlSchema<NKikimrViewer::TFeatureFlagsConfig>());
        return yaml;
    }
};

}
