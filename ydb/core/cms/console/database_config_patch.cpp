#include "database_config_patch.h"

#include <ydb/core/base/database_feature_flags.h>
#include <ydb/library/yaml_config/yaml_config.h>

#include <util/generic/hash_set.h>
#include <util/string/cast.h>

namespace NKikimr::NConsole {
namespace {

TString YamlName(const NKikimrConsole::TFeatureFlagChange& change) {
    const auto* field = FindDatabaseSqlFeatureFlag(change.GetName());
    Y_ENSURE(field, "Feature flag is not available in ALTER DATABASE: " << change.GetName());
    TString name = field->name();
    NProtobufJson::ToSnakeCaseDense(&name);
    return name;
}

} // namespace

NKikimrConsole::TDatabaseFeatureFlagsChange DescribeDatabaseFeatureFlagPatch(const TString& config,
    const NKikimrConsole::TPatchDatabaseConfigRequest& request)
{
    NKikimrConsole::TDatabaseFeatureFlagsChange audit;
    audit.SetDatabase(request.GetDatabase());
    audit.MutableChanges()->CopyFrom(request.GetChanges());
    audit.SetRequestId(request.GetRequestId());

    auto document = NFyaml::TDocument::Parse(config.empty() ? "config: {}\n" : config);
    // Inspect effective database values for auditing, including rejected
    // documents that use aliases or merge keys. Never rewrite stored YAML here.
    document.Resolve();
    const auto configMap = document.Root().Map().at("config").Map();
    NFyaml::TNodeRef flags;
    if (configMap.Has("feature_flags")) {
        flags = configMap.at("feature_flags");
    }
    THashSet<TString> names;
    for (const auto& change : request.GetChanges()) {
        const auto* field = FindDatabaseSqlFeatureFlag(change.GetName());
        if (!field || !names.insert(field->name()).second) {
            continue;
        }
        std::optional<bool> value;
        if (flags && flags.Map().Has(YamlName(change))) {
            bool explicitValue;
            if (!TryFromString(flags.Map().at(YamlName(change)).Scalar(), explicitValue)) {
                continue;
            }
            value = explicitValue;
        }
        auto* oldValue = audit.AddOldOverrides();
        oldValue->SetName(GetDatabaseSqlFeatureFlagName(*field));
        if (value) {
            oldValue->SetValue(*value);
        }
        audit.AddNewOverrides()->CopyFrom(*oldValue);
    }
    return audit;
}

TDatabaseConfigPatch PatchDatabaseFeatureFlags(const TString& config,
    const NKikimrConsole::TPatchDatabaseConfigRequest& request)
{
    Y_ENSURE(request.ChangesSize(), "At least one feature flag is required");
    THashSet<TString> names;
    for (const auto& change : request.GetChanges()) {
        Y_ENSURE(names.insert(YamlName(change)).second,
            "Duplicate feature flag: " << change.GetName());
    }

    auto document = NFyaml::TDocument::Parse(config.empty() ? "config: {}\n" : config);
    auto root = document.Root().Map();
    Y_ENSURE(root.Has("config"), "Database configuration has no config mapping");
    const auto configNode = root.at("config");
    Y_ENSURE(!configNode.IsAlias() && !configNode.HasAnchor(),
        "Database config mapping must not be an alias or an anchor");
    auto configMap = configNode.Map();
    Y_ENSURE(!configMap.Has("<<"),
        "Database config mapping must not use YAML merge keys; change it through config replace first");
    NFyaml::TNodeRef flags;
    if (configMap.Has("feature_flags")) {
        flags = configMap.at("feature_flags");
        Y_ENSURE(!flags.IsAlias() && !flags.HasAnchor() && flags.Type() == NFyaml::ENodeType::Mapping
            && flags.Tag() == std::optional<TString>("!inherit"),
            "Database feature_flags must use !inherit; change it through config replace first");
        Y_ENSURE(!flags.Map().Has("<<"),
            "Database feature_flags must not use YAML merge keys; change it through config replace first");
    }

    TDatabaseConfigPatch result;
    result.Audit.SetDatabase(request.GetDatabase());
    result.Audit.MutableChanges()->CopyFrom(request.GetChanges());
    result.Audit.SetRequestId(request.GetRequestId());
    for (const auto& change : request.GetChanges()) {
        const auto name = YamlName(change);
        auto* oldValue = result.Audit.AddOldOverrides();
        oldValue->SetName(GetDatabaseSqlFeatureFlagName(*FindDatabaseSqlFeatureFlag(change.GetName())));
        if (flags && flags.Map().Has(name)) {
            bool value;
            Y_ENSURE(TryFromString(flags.Map().at(name).Scalar(), value),
                "Existing feature flag value is not Boolean: " << change.GetName());
            oldValue->SetValue(value);
        }
        auto* newValue = result.Audit.AddNewOverrides();
        newValue->SetName(oldValue->GetName());
        if (change.HasValue()) {
            newValue->SetValue(change.GetValue());
        }
        if (oldValue->HasValue() == change.HasValue()
            && (!change.HasValue() || oldValue->GetValue() == change.GetValue()))
        {
            continue;
        }
        result.Modified = true;
        if (!flags) {
            auto node = document.CreateMapping();
            node.SetTag("!inherit");
            configMap.Append(document.CreateScalar("feature_flags"), node);
            flags = configMap.at("feature_flags");
        }
        auto map = flags.Map();
        if (auto pair = map.pair_at_opt(name)) {
            map.Remove(pair);
        }
        if (change.HasValue()) {
            map.Append(document.CreateScalar(name),
                document.CreateScalar(change.GetValue() ? "true" : "false"));
        }
    }
    if (!result.Modified) {
        result.Config = config;
    } else {
        const auto yaml = document.EmitToCharArray();
        Y_ENSURE(yaml, "Cannot serialize database configuration");
        result.Config = yaml.get();
        if (config.empty()) {
            result.Config = NYamlConfig::ReplaceMetadata(result.Config, NYamlConfig::TDatabaseMetadata{
                .Version = 0,
                .Database = request.GetDatabase(),
            });
        }
    }
    return result;
}

} // namespace NKikimr::NConsole
