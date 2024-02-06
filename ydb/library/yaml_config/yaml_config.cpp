#include "yaml_config.h"

#include "yaml_config_parser.h"

#include <ydb/core/base/appdata.h>

#include <library/cpp/protobuf/json/json2proto.h>

namespace NKikimr::NYamlConfig {

NKikimrConfig::TAppConfig YamlToProto(
    const NFyaml::TNodeRef& node,
    bool allowUnknown,
    bool preTransform,
    TSimpleSharedPtr<NProtobufJson::IUnknownFieldsCollector> unknownFieldsCollector) {

    TStringStream sstr;

    sstr << NFyaml::TJsonEmitter(node);

    TString resolvedJsonConfig = sstr.Str();

    NJson::TJsonValue json;

    NJson::ReadJsonTree(resolvedJsonConfig, &json);

    NProtobufJson::TJson2ProtoConfig c;
    c.SetFieldNameMode(NProtobufJson::TJson2ProtoConfig::FieldNameSnakeCaseDense);
    c.SetEnumValueMode(NProtobufJson::TJson2ProtoConfig::EnumCaseInsensetive);
    c.CastRobust = true;
    c.MapAsObject = true;
    c.AllowUnknownFields = allowUnknown;
    c.UnknownFieldsCollector = std::move(unknownFieldsCollector);

    NYaml::TTransformContext ctx;
    NKikimrConfig::TEphemeralInputFields ephemeralConfig;
    if (preTransform) {
        NYaml::ExtractExtraFields(json, ctx);
        NJson::TJsonValue ephemeralJsonNode = json;
        NYaml::ClearNonEphemeralFields(ephemeralJsonNode);
        NProtobufJson::MergeJson2Proto(ephemeralJsonNode, ephemeralConfig, c);
        NYaml::ClearEphemeralFields(json);
    }

    NKikimrConfig::TAppConfig yamlProtoConfig;
    NProtobufJson::MergeJson2Proto(json, yamlProtoConfig, c);

    if (preTransform) {
        NKikimr::NYaml::TransformProtoConfig(ctx, yamlProtoConfig, ephemeralConfig);
    }

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
    Y_ABORT_UNLESS(NJson::ReadJsonTree(resolvedJsonConfigStream.Str(), &json), "Got invalid config from Console");

    NYaml::TTransformContext ctx;
    NYaml::ExtractExtraFields(json, ctx);

    NJson::TJsonValue ephemeralJsonNode = json;
    NYaml::ClearNonEphemeralFields(ephemeralJsonNode);
    NKikimrConfig::TEphemeralInputFields ephemeralConfig;
    NProtobufJson::MergeJson2Proto(ephemeralJsonNode, ephemeralConfig, NYamlConfig::GetJsonToProtoConfig().SetAllowUnknownFields(true));
    NYaml::ClearEphemeralFields(json);

    NProtobufJson::MergeJson2Proto(json, appConfig, NYamlConfig::GetJsonToProtoConfig().SetAllowUnknownFields(true));
    TransformProtoConfig(ctx, appConfig, ephemeralConfig);
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

} // namespace NKikimr::NYamlConfig
