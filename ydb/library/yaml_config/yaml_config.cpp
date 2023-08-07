#include "yaml_config.h"

#include "yaml_config_parser.h"

#include <ydb/core/base/appdata.h>

#include <library/cpp/protobuf/json/json2proto.h>

namespace NYamlConfig {

NKikimrConfig::TAppConfig YamlToProto(const NFyaml::TNodeRef& node, bool allowUnknown, bool preTransform) {
    TStringStream sstr;

    sstr << NFyaml::TJsonEmitter(node);

    TString resolvedJsonConfig = sstr.Str();

    NJson::TJsonValue json;

    NJson::ReadJsonTree(resolvedJsonConfig, &json);

    if (preTransform) {
        NKikimr::NYaml::TransformConfig(json, true);
    }

    NKikimrConfig::TAppConfig yamlProtoConfig;

    NProtobufJson::TJson2ProtoConfig c;
    c.SetFieldNameMode(NProtobufJson::TJson2ProtoConfig::FieldNameSnakeCaseDense);
    c.SetEnumValueMode(NProtobufJson::TJson2ProtoConfig::EnumCaseInsensetive);
    c.CastRobust = true;
    c.MapAsObject = true;
    c.AllowUnknownFields = allowUnknown;

    NProtobufJson::MergeJson2Proto(json, yamlProtoConfig, c);

    return yamlProtoConfig;
}

/**
 * Config used to convert protobuf from/to json
 * changes how names are translated e.g. PDiskInfo -> pdisk_info instead of p_disk_info
 */
NProtobufJson::TJson2ProtoConfig GetJsonToProtoConfig() {
    NProtobufJson::TJson2ProtoConfig config;
    config.SetFieldNameMode(NProtobufJson::TJson2ProtoConfig::FieldNameSnakeCaseDense);
    config.SetEnumValueMode(NProtobufJson::TJson2ProtoConfig::EnumCaseInsensetive);
    config.CastRobust = true;
    config.MapAsObject = true;
    config.AllowUnknownFields = false;
    return config;
}

void ResolveAndParseYamlConfig(
    const TString& yamlConfig,
    const TMap<ui64, TString>& volatileYamlConfigs,
    const TMap<TString, TString>& labels,
    NKikimrConfig::TAppConfig& appConfig,
    TString* resolvedYamlConfig,
    TString* resolvedJsonConfig) {

    auto tree = NFyaml::TDocument::Parse(yamlConfig);

    for (auto& [_, config] : volatileYamlConfigs) {
        auto d = NFyaml::TDocument::Parse(config);
        NYamlConfig::AppendVolatileConfigs(tree, d);
    }

    TSet<NYamlConfig::TNamedLabel> namedLabels;
    for (auto& [name, label] : labels) {
        namedLabels.insert(NYamlConfig::TNamedLabel{name, label});
    }

    auto config = NYamlConfig::Resolve(tree, namedLabels);

    if (resolvedYamlConfig) {
        TStringStream resolvedYamlConfigStream;
        resolvedYamlConfigStream << config.second;
        *resolvedYamlConfig = resolvedYamlConfigStream.Str();
    }

    TStringStream resolvedJsonConfigStream;
    resolvedJsonConfigStream << NFyaml::TJsonEmitter(config.second);

    if (resolvedJsonConfig) {
        *resolvedJsonConfig = resolvedJsonConfigStream.Str();
    }

    NJson::TJsonValue json;
    Y_VERIFY(NJson::ReadJsonTree(resolvedJsonConfigStream.Str(), &json), "Got invalid config from Console");

    NKikimr::NYaml::TransformConfig(json, true);

    NProtobufJson::MergeJson2Proto(json, appConfig, NYamlConfig::GetJsonToProtoConfig());
}

void ReplaceUnmanagedKinds(const NKikimrConfig::TAppConfig& from, NKikimrConfig::TAppConfig& to) {
    if (from.HasNameserviceConfig()) {
        to.MutableNameserviceConfig()->CopyFrom(from.GetNameserviceConfig());
    }

    if (from.HasNetClassifierDistributableConfig()) {
        to.MutableNetClassifierDistributableConfig()->CopyFrom(from.GetNetClassifierDistributableConfig());
    }

    if (from.NamedConfigsSize()) {
        to.MutableNamedConfigs()->CopyFrom(from.GetNamedConfigs());
    }
}

} // namespace NYamlConfig
