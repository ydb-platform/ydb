#pragma once
#include "json_pipe_req.h"
#include "viewer.h"
#include <ydb/library/yaml_config/yaml_config.h>

namespace NKikimr::NViewer {

using namespace NActors;

class TJsonFeatureFlags : public TViewerPipeClient {
    using TThis = TJsonFeatureFlags;
    using TBase = TViewerPipeClient;
    static constexpr bool RunOnDynnode = true;

    THashSet<TString> FilterFeatures;
    bool ChangedOnly = false;

public:
    TJsonFeatureFlags(IViewer* viewer, NMon::TEvHttpInfo::TPtr &ev)
        : TViewerPipeClient(viewer, ev)
    {}

    void BootstrapEx() override {
        StringSplitter(Params.Get("features")).Split(',').SkipEmpty().Collect(&FilterFeatures);
        ChangedOnly = FromStringWithDefault<bool>(Params.Get("changed"), ChangedOnly);
        ReplyAndPassAway();
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

    void ReplyAndPassAway() override {
        // prepare response
        NKikimrViewer::TFeatureFlagsConfig Result;
        Result.SetVersion(Viewer->GetCapabilityVersion("/viewer/feature_flags"));
        auto databaseProto = Result.AddDatabases();
        databaseProto->SetName(AppData()->TenantName);
        ParseFeatureFlags(AppData()->FeatureFlags, *databaseProto);
        TBase::ReplyAndPassAway(GetHTTPOKJSON(Result));
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
