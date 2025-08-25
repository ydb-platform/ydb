#include "yaml_config.h"

#include "yaml_config_parser.h"

#include <ydb/core/base/appdata.h>

#include <library/cpp/protobuf/json/json2proto.h>

#include <ydb/core/protos/netclassifier.pb.h>
#include <ydb/core/config/validation/validators.h>

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
    const TString& mainYamlConfig,
    const TMap<ui64, TString>& volatileYamlConfigs,
    const TMap<TString, TString>& labels,
    NKikimrConfig::TAppConfig& appConfig,
    std::optional<TString> databaseYamlConfig,
    TString* resolvedYamlConfig,
    TString* resolvedJsonConfig)
{
    TStringStream resolvedJsonConfigStream;
    bool hasMetadata = false;
    if (mainYamlConfig) {
        auto tree = NFyaml::TDocument::Parse(mainYamlConfig);

        if (tree.Root().Map().Has("metadata")) {
            hasMetadata = true;
        }

        if (databaseYamlConfig) {
            auto d = NFyaml::TDocument::Parse(*databaseYamlConfig);
            NYamlConfig::AppendDatabaseConfig(tree, d);
        }

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

        resolvedJsonConfigStream << NFyaml::TJsonEmitter(config.second);

        if (resolvedJsonConfig) {
            *resolvedJsonConfig = resolvedJsonConfigStream.Str();
        }
    } else {
        resolvedJsonConfigStream << "{}";
    }

    NJson::TJsonValue json;
    Y_ABORT_UNLESS(NJson::ReadJsonTree(resolvedJsonConfigStream.Str(), &json), "Got invalid config from Console");

    if (hasMetadata) {
        appConfig.SetYamlConfigEnabled(true);
    }

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

class TLegacyValidators
    : public IConfigValidator
{
public:
    EValidationResult ValidateConfig(
        const NKikimrConfig::TAppConfig& config,
        std::vector<TString>& msg) const override
    {
        auto res = NKikimr::NConfig::ValidateConfig(config, msg);
        switch (res) {
            case NKikimr::NConfig::EValidationResult::Ok:
                return EValidationResult::Ok;
            case NKikimr::NConfig::EValidationResult::Warn:
                return EValidationResult::Warn;
            case NKikimr::NConfig::EValidationResult::Error:
                return EValidationResult::Error;
        }
    }
};

class TDefaultConfigSwissKnife : public IConfigSwissKnife {
public:
    TDefaultConfigSwissKnife() {
        Validators["LegacyValidators"] = MakeSimpleShared<TLegacyValidators>();
    }

    bool VerifyReplaceRequest(const Ydb::Config::ReplaceConfigRequest&, Ydb::StatusIds::StatusCode&, NYql::TIssues&) const override {
        return true;
    }

    bool VerifyMainConfig(const TString&) const override {
        return true;
    };

    bool VerifyStorageConfig(const TString&) const override {
        return true;
    }
};


std::unique_ptr<IConfigSwissKnife> CreateDefaultConfigSwissKnife() {
    return std::make_unique<TDefaultConfigSwissKnife>();
}

EValidationResult IConfigSwissKnife::ValidateConfig(
    const NKikimrConfig::TAppConfig& config,
    std::vector<TString>& msg) const
{
    for (const auto& [name, validator] : GetValidators()) {
        EValidationResult result = validator->ValidateConfig(config, msg);
        if (result == EValidationResult::Error) {
            return EValidationResult::Error;
        }
    }

    if (msg.size() > 0) {
        return EValidationResult::Warn;
    }

    return EValidationResult::Ok;
}

} // namespace NKikimr::NYamlConfig
