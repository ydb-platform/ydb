#include "yaml_config.h"

#include "yaml_config_parser.h"

#include <ydb/core/base/appdata.h>

#include <library/cpp/protobuf/json/json2proto.h>
#include <library/cpp/protobuf/json/util.h>

#include <ydb/core/config/protos/marker.pb.h>
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
    NYaml::Parse(json, NYaml::GetJsonToProtoConfig(allowUnknown, std::move(unknownFieldsCollector)), yamlProtoConfig, preTransform, /*phase=*/ nullptr, /*relaxed=*/ true);

    return yamlProtoConfig;
}

void ResolveAndParseYamlConfig(
    const TString& mainYamlConfig,
    const TMap<ui64, TString>& volatileYamlConfigs,
    const TMap<TString, TString>& labels,
    NKikimrConfig::TAppConfig& appConfig,
    std::optional<TString> databaseYamlConfig,
    TString* resolvedYamlConfig,
    TString* resolvedJsonConfig,
    TSimpleSharedPtr<NProtobufJson::IUnknownFieldsCollector> unknownFieldsCollector)
{
    TStringStream resolvedJsonConfigStream;
    bool hasMetadata = false;
    if (mainYamlConfig) {
        auto tree = NFyaml::TDocument::Parse(mainYamlConfig);

        if (tree.Root().Map().Has("metadata")) {
            hasMetadata = true;
        }

        TSet<NYamlConfig::TNamedLabel> namedLabels;
        for (auto& [name, label] : labels) {
            namedLabels.insert(NYamlConfig::TNamedLabel{name, label});
        }

        if (databaseYamlConfig) {
            auto d = NFyaml::TDocument::Parse(*databaseYamlConfig);
            NYamlConfig::ResolveDatabaseConfig(d, namedLabels);
            NYamlConfig::AppendDatabaseConfig(tree, d);
        }

        for (auto& [_, config] : volatileYamlConfigs) {
            auto d = NFyaml::TDocument::Parse(config);
            NYamlConfig::AppendVolatileConfigs(tree, d);
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

    NYaml::Parse(json, NYaml::GetJsonToProtoConfig(true, std::move(unknownFieldsCollector)), appConfig, true, /*phase=*/ nullptr, /*relaxed=*/ true);
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
    TVector<TVector<TString>> GetValidationDependencies() const override {
        return {{"/nbs_config", "/grpc_config", "/client_certificate_authorization", "/feature_flags"}};
    }

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

namespace {

// These transform and validator inputs must be resolved with every section group.
const TVector<TString>& SharedValidationInputs() {
    static const TVector<TString> sections = [] {
        TVector<TString> result = {
            "/nameservice_config", "/domains_config", "/blob_storage_config",
            "/channel_profile_config", "/bootstrap_config", "/self_management_config",
        };
        const auto* fields = NKikimrConfig::TEphemeralInputFields::descriptor();
        for (int i = 0; i < fields->field_count(); ++i) {
            TString name = fields->field(i)->name();
            NProtobufJson::ToSnakeCaseDense(&name);
            result.push_back(TString("/") + name);
        }
        return result;
    }();
    return sections;
}

TVector<TVector<TString>> ValidationGroups(
    NFyaml::TDocument& doc, const TVector<TVector<TString>>& dependencies)
{
    auto present = CollectConfigSections(doc);
    TVector<TVector<TString>> groups;
    TVector<TSet<TString>> connected;
    for (const auto& dependency : dependencies) {
        TSet<TString> group(dependency.begin(), dependency.end());
        if (group.contains("/")) {
            return {{"/"}};
        }
        for (size_t i = 0; i < connected.size();) {
            if (AnyOf(connected[i], [&](const TString& section) { return group.contains(section); })) {
                group.insert(connected[i].begin(), connected[i].end());
                connected.erase(connected.begin() + i);
                i = 0;
            } else {
                ++i;
            }
        }
        connected.push_back(std::move(group));
    }
    for (const auto& group : connected) {
        if (AnyOf(group, [&](const TString& section) { return present.contains(section); })) {
            groups.emplace_back(group.begin(), group.end());
            for (const auto& section : group) {
                present.erase(section);
            }
        }
    }
    for (const auto& section : SharedValidationInputs()) {
        present.erase(section);
    }
    for (const auto& section : present) {
        groups.push_back({section});
    }
    if (groups.empty()) {
        groups.emplace_back();
    }
    for (auto& group : groups) {
        group.insert(group.end(), SharedValidationInputs().begin(), SharedValidationInputs().end());
    }
    return groups;
}

} // namespace

void ValidateConfig(
    NFyaml::TDocument& doc,
    const IConfigSwissKnife* validator,
    TSimpleSharedPtr<NProtobufJson::IUnknownFieldsCollector> unknownFieldsCollector)
{
    std::vector<TString> errors;
    auto validate = [&](NFyaml::TNodeRef config) {
        auto proto = YamlToProto(config, true, true, unknownFieldsCollector);
        if (validator && validator->ValidateConfig(proto, errors) == EValidationResult::Error) {
            ythrow yexception() << (errors.empty() ? TString("Config validation failed") : errors.front());
        }
    };
    const auto dependencies = validator ? validator->GetValidationDependencies() : TVector<TVector<TString>>{};
    for (const auto& group : ValidationGroups(doc, dependencies)) {
        EnumerateDistinctProjections(doc, group, validate);
    }
}

} // namespace NKikimr::NYamlConfig
