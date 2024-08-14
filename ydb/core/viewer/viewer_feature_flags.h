#pragma once

namespace NKikimr::NViewer {

using namespace NActors;

class TJsonFeatureFlags : public TViewerPipeClient {
    using TThis = TJsonFeatureFlags;
    using TBase = TViewerPipeClient;
    IViewer* Viewer;
    NMon::TEvHttpInfo::TPtr Event;
    TJsonSettings JsonSettings;
    ui32 Timeout = 0;

    TString Database;
    NKikimrViewer::TFeatureFlagsConfig Result;
    THashMap<ui64, TString> TenantsByCookie;
    ui64 Cookie = 0;
    TString DomainPath;
    THashMap<TString, THashMap<TString, bool>> FeatureFlagsByTenant;

public:
    TJsonFeatureFlags(IViewer* viewer, NMon::TEvHttpInfo::TPtr &ev)
        : Viewer(viewer)
        , Event(ev)
    {}

    void MakeNodeConfigRequest(const TString& tenant) {
        RequestConsoleNodeConfigByTenant(tenant, Cookie);
        TenantsByCookie[Cookie++] = tenant;
    }

    void Bootstrap() override {
        const auto& params(Event->Get()->Request.GetParams());

        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), true);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        Database = params.Get("database");
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);

        TIntrusivePtr<TDomainsInfo> domains = AppData()->DomainsInfo;
        auto* domain = domains->GetDomain();
        DomainPath = "/" + domain->Name;

        MakeNodeConfigRequest(DomainPath);
        if (!Database) {
            RequestConsoleListTenants();
        } else {
            MakeNodeConfigRequest(Database);
        }

        Become(&TThis::StateWork, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NConsole::TEvConsole::TEvListTenantsResponse, Handle);
            hFunc(NConsole::TEvConsole::TEvGetNodeConfigResponse, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void Handle(NConsole::TEvConsole::TEvListTenantsResponse::TPtr& ev) {
        auto rec = ev->Release()->Record;

        Ydb::Cms::ListDatabasesResult listTenantsResult;
        rec.GetResponse().operation().result().UnpackTo(&listTenantsResult);
        for (const TString& path : listTenantsResult.paths()) {
            MakeNodeConfigRequest(path);
        }

        RequestDone();
    }

    void Handle(NConsole::TEvConsole::TEvGetNodeConfigResponse::TPtr& ev) {
        TString tenant = TenantsByCookie[ev.Get()->Cookie];
        NKikimrConsole::TGetNodeConfigResponse rec = ev->Release()->Record;
        FeatureFlagsByTenant[tenant] = ParseFeatureFlags(rec.GetConfig().GetFeatureFlags());

        RequestDone();
    }

    THashMap<TString, bool> ParseFeatureFlags(const NKikimrConfig::TFeatureFlags& featureFlags) {
        THashMap<TString, bool> features;
        const google::protobuf::Reflection* reflection = featureFlags.GetReflection();
        const google::protobuf::Descriptor* descriptor = featureFlags.GetDescriptor();

        for (int i = 0; i < descriptor->field_count(); ++i) {
            const google::protobuf::FieldDescriptor* field = descriptor->field(i);
            if (reflection->HasField(featureFlags, field) && field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_BOOL) {
                bool isEnabled = reflection->GetBool(featureFlags, field);
                features[field->name()] = isEnabled;
            }
        }
        return features;
    }

    void ReplyAndPassAway() override {
        auto domainFeaturesIt = FeatureFlagsByTenant.find(DomainPath);
        if (domainFeaturesIt == FeatureFlagsByTenant.end()) {
            Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPINTERNALERROR(Event->Get(), "text/plain", "No domain info from Console"), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
            PassAway();
            return;
        }

        // remove features that are the same as in the domain
        for (auto& [tenant, features] : FeatureFlagsByTenant) {
            if (tenant != DomainPath) {
                for (const auto& [name, enabled] : domainFeaturesIt->second) {
                    auto featureIt = features.find(name);
                    if (featureIt != features.end() && featureIt->second == enabled) {
                        features.erase(name);
                    }
                }
            }
        }

        // prepare response
        for (const auto& [tenant, features] : FeatureFlagsByTenant) {
            if (!Database || Database == tenant) {
                auto tenantProto = Result.AddTenants();
                tenantProto->SetName(tenant);
                for (const auto& [name, enabled] : features) {
                    auto flag = tenantProto->AddFeatureFlags();
                    flag->SetName(name);
                    flag->SetEnabled(enabled);
                }
            }
        }

        TStringStream json;
        TProtoToJson::ProtoToJson(json, Result, JsonSettings);
        Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get(), json.Str()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }

    void HandleTimeout() {
        Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPGATEWAYTIMEOUT(Event->Get()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
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
