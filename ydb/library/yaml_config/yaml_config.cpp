#include "yaml_config.h"

#include "yaml_config_parser.h"

#include <ydb/core/base/appdata.h>

#include <library/cpp/protobuf/json/json2proto.h>

#include <ydb/core/protos/netclassifier.pb.h>

namespace NKikimr::NYamlConfig {

NKikimrConfig::TAppConfig YamlToProto(
    const NFyaml::TNodeRef& node,
    bool allowUnknown,
    bool preTransform,
    TSimpleSharedPtr<NProtobufJson::IUnknownFieldsCollector> unknownFieldsCollector)
{
    TStringStream sstr;

    sstr << NFyaml::TJsonEmitter(node);

    TString resolvedJsonConfig = sstr.Str();

    NJson::TJsonValue json;

    NJson::ReadJsonTree(resolvedJsonConfig, &json);

    NKikimrConfig::TAppConfig yamlProtoConfig;
    NYaml::Parse(json, NYaml::GetJsonToProtoConfig(allowUnknown, std::move(unknownFieldsCollector)), yamlProtoConfig, preTransform, true);

    return yamlProtoConfig;
}

void ResolveAndParseYamlConfig(
    const TString& yamlConfig,
    const TMap<ui64, TString>& volatileYamlConfigs,
    const TMap<TString, TString>& labels,
    NKikimrConfig::TAppConfig& appConfig,
    TString* resolvedYamlConfig,
    TString* resolvedJsonConfig)
{
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

    NYaml::Parse(json, NYaml::GetJsonToProtoConfig(true), appConfig, true, true);
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
