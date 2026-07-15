#include "config_migration.h"

#include <ydb/library/yaml_config/public/storage_defaults.h>

#include <util/string/ascii.h>
#include <util/string/cast.h>

#include <algorithm>
#include <array>
#include <initializer_list>
#include <optional>

namespace NKikimr::NYamlConfig {
    namespace {

        constexpr std::array<TStringBuf, 7> MigrationStorageKeys = {
            "hosts",
            "host_configs",
            "nameservice_config",
            "domains_config",
            "blob_storage_config",
            "channel_profile_config",
            "static_erasure",
        };

        constexpr std::array<std::pair<TStringBuf, EDefaultDiskType>, 3> DiskShorthands = {{
            {"rot", EDefaultDiskType::Rot},
            {"ssd", EDefaultDiskType::Ssd},
            {"nvme", EDefaultDiskType::Nvme},
        }};

        constexpr std::array<TStringBuf, 9> MigrationSelectorStorageKeys = {
            "domains_config",
            "blob_storage_config",
            "channel_profile_config",
            "static_erasure",
            "storage_pool_types",
            "fail_domain_type",
            "default_disk_type",
            "erasure",
            "self_management_config",
        };

        std::optional<NFyaml::TMapping> AsMap(const NFyaml::TNodeRef& node) {
            if (node.Type() == NFyaml::ENodeType::Mapping) {
                return node.Map();
            }
            return std::nullopt;
        }

        std::optional<NFyaml::TSequence> AsSequence(const NFyaml::TNodeRef& node) {
            if (node.Type() == NFyaml::ENodeType::Sequence) {
                return node.Sequence();
            }
            return std::nullopt;
        }

        std::optional<NFyaml::TMapping> FindMap(const NFyaml::TMapping& map, TStringBuf key) {
            const TString name(key);
            return map.Has(name) ? AsMap(map.at(name)) : std::nullopt;
        }

        std::optional<NFyaml::TSequence> FindSequence(const NFyaml::TMapping& map, TStringBuf key) {
            const TString name(key);
            return map.Has(name) ? AsSequence(map.at(name)) : std::nullopt;
        }

        std::optional<TString> FindScalar(const NFyaml::TMapping& map, TStringBuf key) {
            const TString name(key);
            if (!map.Has(name)) {
                return std::nullopt;
            }

            auto node = map.at(name);
            return node.Type() == NFyaml::ENodeType::Scalar ? std::make_optional(node.Scalar()) : std::nullopt;
        }

        bool HasOnlyKeys(const NFyaml::TMapping& map, std::initializer_list<TStringBuf> keys) {
            return std::all_of(map.begin(), map.end(), [&](const auto& pair) {
                return pair.Key().Type() == NFyaml::ENodeType::Scalar &&
                       std::find(keys.begin(), keys.end(), pair.Key().Scalar()) != keys.end();
            });
        }

        enum class EPresence {
            Required,
            Optional,
            Forbidden,
        };

        struct TScalarRule {
            TStringBuf Key;
            TStringBuf Value;
            EPresence Presence = EPresence::Optional;
        };

        bool MatchesScalarFields(const NFyaml::TMapping& map, std::initializer_list<TScalarRule> rules, std::initializer_list<TStringBuf> nestedKeys = {}) {
            for (const auto& rule : rules) {
                const TString key(rule.Key);
                const bool present = map.Has(key);
                if ((rule.Presence == EPresence::Required && !present) || (rule.Presence == EPresence::Forbidden && present)) {
                    return false;
                }
                if (present) {
                    auto value = map.at(key);
                    if (value.Type() != NFyaml::ENodeType::Scalar || value.Scalar() != rule.Value) {
                        return false;
                    }
                }
            }

            return std::all_of(map.begin(), map.end(), [&](const auto& pair) {
                if (pair.Key().Type() != NFyaml::ENodeType::Scalar) {
                    return false;
                }
                const auto key = pair.Key().Scalar();
                return std::any_of(rules.begin(), rules.end(), [&](const auto& rule) {
                           return key == rule.Key;
                       }) ||
                       std::find(nestedKeys.begin(), nestedKeys.end(), key) != nestedKeys.end();
            });
        }

        void ReplaceMapValue(NFyaml::TDocument& targetDoc, NFyaml::TMapping& targetMap, const NFyaml::TNodeRef& sourceKey, const NFyaml::TNodeRef& sourceValue) {
            if (auto current = targetMap.pair_at_opt(sourceKey.Scalar()); current) {
                targetMap.Remove(current.Key());
            }
            targetMap.Append(sourceKey.Copy(targetDoc).Ref(), sourceValue.Copy(targetDoc).Ref());
        }

        void PromoteDomainsSecurityConfig(NFyaml::TDocument& doc, NFyaml::TMapping& config) {
            if (!config.Has("domains_config")) {
                return;
            }
            auto domains = FindMap(config, "domains_config");
            Y_ENSURE_EX(domains, TYamlConfigEx() << "'config.domains_config' section must be a mapping");

            auto security = domains->pair_at_opt("security_config");
            if (!security) {
                return;
            }
            if (!config.Has("security_config")) {
                config.Append(doc.CreateScalar("security_config"), security.Value().Copy(doc).Ref());
            }
            domains->Remove(security.Key());
        }

        NFyaml::TMapping GetMainConfig(NFyaml::TDocument& doc, TStringBuf description = "Config") {
            auto rootNode = doc.Root();
            Y_ENSURE_EX(rootNode && rootNode.Type() == NFyaml::ENodeType::Mapping, TYamlConfigEx() << description << " must be a non-empty mapping in MainConfig format");
            auto root = rootNode.Map();
            Y_ENSURE_EX(root.Has("config"), TYamlConfigEx() << description << " must have a 'config' section");
            auto config = AsMap(root.at("config"));
            Y_ENSURE_EX(config, TYamlConfigEx() << description << " 'config' section must be a mapping");
            return *config;
        }

        void PromoteLegacyValue(NFyaml::TDocument& doc, NFyaml::TMapping& config, TStringBuf targetKey, const NFyaml::TNodeRef& legacyValue, TStringBuf legacyPath) {
            const TString key(targetKey);
            if (auto current = config.pair_at_opt(key); current) {
                Y_ENSURE_EX(current.Value().DeepEqual(legacyValue), TYamlConfigEx() << "Conflicting values at 'config." << targetKey << "' and '" << legacyPath << "'");
                return;
            }
            config.Append(doc.CreateScalar(key), legacyValue.Copy(doc).Ref());
        }

        void RemoveRedundantValue(NFyaml::TMapping& config, TStringBuf key, const NFyaml::TNodeRef& legacyValue, TStringBuf legacyPath) {
            auto current = config.pair_at_opt(TString(key));
            if (!current) {
                return;
            }
            Y_ENSURE_EX(current.Value().DeepEqual(legacyValue), TYamlConfigEx() << "Conflicting values at 'config." << key << "' and '" << legacyPath << "'");
            config.Remove(current.Key());
        }

        std::optional<NFyaml::TMapping> GetSingleDomain(const NFyaml::TMapping& domains) {
            if (!domains.Has("domain")) {
                return std::nullopt;
            }

            auto domainsList = AsSequence(domains.at("domain"));
            Y_ENSURE_EX(domainsList, TYamlConfigEx() << "'config.domains_config.domain' section must be a sequence");
            Y_ENSURE_EX(domainsList->size() <= 1, TYamlConfigEx() << "Cannot clean up config with more than one 'config.domains_config.domain' entry");
            if (domainsList->empty()) {
                return std::nullopt;
            }

            auto domain = AsMap(domainsList->at(0));
            Y_ENSURE_EX(domain, TYamlConfigEx() << "'config.domains_config.domain[0]' entry must be a mapping");
            return domain;
        }

        void RemoveMapValue(NFyaml::TMapping& map, TStringBuf key) {
            if (auto value = map.pair_at_opt(TString(key)); value) {
                map.Remove(value.Key());
            }
        }

        bool CanLiftStoragePoolTypes(const NFyaml::TMapping& domains, const NFyaml::TMapping& domain) {
            return HasOnlyKeys(domains, {"domain"}) && HasOnlyKeys(domain, {"name", "storage_pool_types"});
        }

        void PromoteSelectorDomainsSecurityConfig(NFyaml::TDocument& doc, NFyaml::TMapping& selectorConfig) {
            auto domainsEntry = selectorConfig.pair_at_opt("domains_config");
            if (!domainsEntry) {
                return;
            }

            auto domains = AsMap(domainsEntry.Value());
            const auto tag = domainsEntry.Value().Tag();
            if (!domains || !tag || *tag != "!inherit" || !HasOnlyKeys(*domains, {"security_config"}) || !domains->Has("security_config")) {
                return;
            }

            auto security = domains->pair_at("security_config");
            if (auto current = selectorConfig.pair_at_opt("security_config"); current) {
                Y_ENSURE_EX(current.Value().DeepEqual(security.Value()),
                            TYamlConfigEx() << "Conflicting values at 'selector_config.config.security_config' and 'selector_config.config.domains_config.security_config'");
            } else {
                selectorConfig.Append(doc.CreateScalar("security_config"), security.Value().Copy(doc).Ref());
            }
            selectorConfig.Remove(domainsEntry.Key());
        }

        void ValidateCleanupPrerequisites(NFyaml::TDocument& doc, const NFyaml::TMapping& config) {
            auto selfManagement = FindMap(config, "self_management_config");
            auto selfManagementEnabled = selfManagement ? FindScalar(*selfManagement, "enabled") : std::nullopt;
            Y_ENSURE_EX(selfManagementEnabled && AsciiEqualsIgnoreCase(*selfManagementEnabled, "true"),
                        TYamlConfigEx() << "Cleanup requires 'config.self_management_config.enabled: true'");

            auto featureFlags = FindMap(config, "feature_flags");
            auto configV2Enabled = featureFlags ? FindScalar(*featureFlags, "switch_to_config_v2") : std::nullopt;
            Y_ENSURE_EX(configV2Enabled && AsciiEqualsIgnoreCase(*configV2Enabled, "true"),
                        TYamlConfigEx() << "Cleanup requires 'config.feature_flags.switch_to_config_v2: true'");

            auto root = doc.Root().Map();
            if (!root.Has("selector_config")) {
                return;
            }
            auto selectors = AsSequence(root.at("selector_config"));
            Y_ENSURE_EX(selectors, TYamlConfigEx() << "'selector_config' section must be a sequence");
            for (const auto& selectorNode : *selectors) {
                auto selector = AsMap(selectorNode);
                Y_ENSURE_EX(selector, TYamlConfigEx() << "'selector_config' entry must be a mapping");
                if (!selector->Has("config")) {
                    continue;
                }
                auto selectorConfig = FindMap(*selector, "config");
                Y_ENSURE_EX(selectorConfig, TYamlConfigEx() << "'selector_config.config' section must be a mapping");
                PromoteSelectorDomainsSecurityConfig(doc, *selectorConfig);
                for (const auto key : MigrationSelectorStorageKeys) {
                    Y_ENSURE_EX(!selectorConfig->Has(TString(key)), TYamlConfigEx() << "Cannot safely clean up selector override 'selector_config.config." << key << "'");
                }
            }
        }

        std::optional<TString> GetEffectiveErasure(const NFyaml::TMapping& config) {
            std::optional<TString> result;
            for (const auto key : {TStringBuf("erasure"), TStringBuf("static_erasure")}) {
                if (!config.Has(TString(key))) {
                    continue;
                }
                auto value = FindScalar(config, key);
                if (!value || (result && *result != *value)) {
                    return std::nullopt;
                }
                result = *value;
            }
            return result;
        }

        std::optional<size_t> GetHostCount(const NFyaml::TMapping& config) {
            auto hosts = FindSequence(config, "hosts");
            return hosts ? std::make_optional(hosts->size()) : std::nullopt;
        }

        std::optional<EDefaultDiskType> ParseDriveType(const NFyaml::TNodeRef& driveNode, EDefaultDiskType fallback) {
            auto drive = AsMap(driveNode);
            if (!drive) {
                return std::nullopt;
            }
            if (!drive->Has("type")) {
                return fallback;
            }

            auto type = FindScalar(*drive, "type");
            if (!type) {
                return std::nullopt;
            }
            return *type == "RAM" ? std::make_optional(EDefaultDiskType::Ssd) : TryParseDefaultDiskType(*type);
        }

        bool MergeSameDiskType(std::optional<EDefaultDiskType>& result, EDefaultDiskType candidate) {
            if (result && *result != candidate) {
                return false;
            }
            result = candidate;
            return true;
        }

        std::optional<EDefaultDiskType> GetShorthandDiskType(const NFyaml::TSequence& entries) {
            std::optional<EDefaultDiskType> result;
            for (const auto& entryNode : entries) {
                auto entry = AsMap(entryNode);
                if (!entry) {
                    return std::nullopt;
                }
                for (const auto& [key, type] : DiskShorthands) {
                    if (!entry->Has(TString(key))) {
                        continue;
                    }
                    auto paths = FindSequence(*entry, key);
                    if (!paths) {
                        return std::nullopt;
                    }
                    if (!paths->empty() && !MergeSameDiskType(result, type)) {
                        return std::nullopt;
                    }
                }
            }
            return result;
        }

        std::optional<EDefaultDiskType> GetSingleNodeDiskType(const NFyaml::TMapping& config, EDefaultDiskType fallback) {
            if (config.Has("host_configs")) {
                auto hostConfigs = FindSequence(config, "host_configs");
                if (!hostConfigs) {
                    return std::nullopt;
                }
                if (!hostConfigs->empty()) {
                    auto first = AsMap(hostConfigs->at(0));
                    if (!first) {
                        return std::nullopt;
                    }
                    if (first->Has("drive")) {
                        auto drives = FindSequence(*first, "drive");
                        if (!drives) {
                            return std::nullopt;
                        }
                        if (!drives->empty()) {
                            return ParseDriveType(drives->at(0), fallback);
                        }
                    }
                    return GetShorthandDiskType(*hostConfigs);
                }
            }

            auto hosts = FindSequence(config, "hosts");
            if (!hosts || hosts->size() != 1) {
                return std::nullopt;
            }
            auto host = AsMap(hosts->at(0));
            auto drives = host ? FindSequence(*host, "drive") : std::nullopt;
            if (!drives || drives->empty()) {
                return std::nullopt;
            }
            return ParseDriveType(drives->at(0), fallback);
        }

        bool IsDefaultDiskTypeAlreadyImplied(const NFyaml::TMapping& config, EDefaultDiskType candidate) {
            bool found = false;
            for (const auto section : {TStringBuf("host_configs"), TStringBuf("hosts")}) {
                if (!config.Has(TString(section))) {
                    continue;
                }
                auto entries = FindSequence(config, section);
                if (!entries) {
                    return false;
                }
                for (const auto& entryNode : *entries) {
                    auto entry = AsMap(entryNode);
                    if (!entry) {
                        return false;
                    }
                    if (entry->Has("drive")) {
                        auto drives = FindSequence(*entry, "drive");
                        if (!drives) {
                            return false;
                        }
                        for (const auto& drive : *drives) {
                            const auto driveType = ParseDriveType(drive, EDefaultDiskType::Ssd);
                            if (!driveType || *driveType != candidate) {
                                return false;
                            }
                            found = true;
                        }
                    }
                    for (const auto& [key, type] : DiskShorthands) {
                        if (entry->Has(TString(key))) {
                            auto paths = FindSequence(*entry, key);
                            if (!paths || (!paths->empty() && type != candidate)) {
                                return false;
                            }
                            found |= !paths->empty();
                        }
                    }
                }
            }
            return found;
        }

        std::optional<EDefaultDiskType> ParsePDiskFilter(const NFyaml::TMapping& poolConfig) {
            auto filters = FindSequence(poolConfig, "pdisk_filter");
            if (!filters || filters->size() != 1) {
                return std::nullopt;
            }
            auto filter = AsMap(filters->at(0));
            if (!filter || !HasOnlyKeys(*filter, {"property"})) {
                return std::nullopt;
            }
            auto properties = FindSequence(*filter, "property");
            if (!properties || properties->size() != 1) {
                return std::nullopt;
            }
            auto property = AsMap(properties->at(0));
            if (!property || !HasOnlyKeys(*property, {"type"})) {
                return std::nullopt;
            }
            auto type = FindScalar(*property, "type");
            return type ? TryParseDefaultDiskType(*type) : std::nullopt;
        }

        std::optional<EFailDomainType> ParseFailDomainType(const std::optional<NFyaml::TMapping>& poolConfig) {
            if (!poolConfig || !poolConfig->Has("geometry")) {
                return EFailDomainType::Rack;
            }

            auto geometry = FindMap(*poolConfig, "geometry");
            constexpr std::array<TStringBuf, 4> keys = {
                "realm_level_begin",
                "realm_level_end",
                "domain_level_begin",
                "domain_level_end",
            };
            if (!geometry || geometry->size() != keys.size()) {
                return std::nullopt;
            }

            std::array<ui32, keys.size()> values{};
            for (size_t i = 0; i < keys.size(); ++i) {
                auto value = FindScalar(*geometry, keys[i]);
                if (!value || !TryFromString(*value, values[i])) {
                    return std::nullopt;
                }
            }
            return TryInferFailDomainType({values[0], values[1], values[2], values[3]});
        }

        struct TStoragePool {
            NFyaml::TMapping Type;
            std::optional<NFyaml::TMapping> Config;
            std::optional<EDefaultDiskType> DiskType;
            std::optional<EFailDomainType> FailDomainType;
        };

        std::optional<TVector<TStoragePool>> AnalyzeStoragePools(const NFyaml::TNodeRef& node) {
            auto sequence = AsSequence(node);
            if (!sequence) {
                return std::nullopt;
            }

            TVector<TStoragePool> result;
            result.reserve(sequence->size());
            for (const auto& poolNode : *sequence) {
                auto type = AsMap(poolNode);
                if (!type) {
                    return std::nullopt;
                }

                std::optional<NFyaml::TMapping> config;
                if (type->Has("pool_config")) {
                    config = FindMap(*type, "pool_config");
                    if (!config) {
                        return std::nullopt;
                    }
                }
                result.push_back({
                    .Type = *type,
                    .Config = config,
                    .DiskType = config ? ParsePDiskFilter(*config) : std::nullopt,
                    .FailDomainType = ParseFailDomainType(config),
                });
            }
            return result;
        }

        template <class T, class TProjection>
        std::optional<T> GetCommonValue(const TVector<TStoragePool>& pools, TProjection projection) {
            std::optional<T> result;
            for (const auto& pool : pools) {
                auto value = projection(pool);
                if (!value || (result && *result != *value)) {
                    return std::nullopt;
                }
                result = value;
            }
            return result;
        }

        struct TGeneratedDefaults {
            EDefaultDiskType DiskType;
            TString Erasure;
            bool SingleNode;
        };

        bool MatchesStorageDefaults(const TVector<TStoragePool>& pools, const TGeneratedDefaults& defaults, EFailDomainType failDomainType) {
            if (pools.size() != 1) {
                return false;
            }

            const auto& pool = pools.front();
            const auto kind = DefaultStoragePoolKind(defaults.DiskType);
            if (!MatchesScalarFields(
                    pool.Type,
                    {{"kind", kind, defaults.SingleNode ? EPresence::Required : EPresence::Optional}},
                    {"pool_config"})) {
                return false;
            }
            if (!pool.Config) {
                return !defaults.SingleNode && failDomainType == EFailDomainType::Rack;
            }

            const auto single = defaults.SingleNode;
            if (!MatchesScalarFields(
                    *pool.Config,
                    {
                        {"box_id", "1", single ? EPresence::Required : EPresence::Optional},
                        {"erasure_species", defaults.Erasure, single ? EPresence::Required : EPresence::Optional},
                        {"kind", kind, single ? EPresence::Forbidden : EPresence::Optional},
                        {"vdisk_kind", "Default", single ? EPresence::Required : EPresence::Optional},
                    },
                    {"geometry", "pdisk_filter"})) {
                return false;
            }

            const bool diskMatches = pool.Config->Has("pdisk_filter") ? pool.DiskType == defaults.DiskType : !single;
            const bool geometryMatches = single ? !pool.Config->Has("geometry") : pool.FailDomainType == failDomainType;
            return diskMatches && geometryMatches;
        }

        bool MatchesChannelProfileDefaults(const NFyaml::TNodeRef& node, const TGeneratedDefaults& defaults) {
            auto config = AsMap(node);
            if (!config || !HasOnlyKeys(*config, {"profile"})) {
                return false;
            }
            auto profiles = FindSequence(*config, "profile");
            if (!profiles || profiles->size() != 1) {
                return false;
            }
            auto profile = AsMap(profiles->at(0));
            if (!profile || !MatchesScalarFields(*profile, {{"profile_id", "0", EPresence::Optional}}, {"channel"})) {
                return false;
            }
            auto channels = FindSequence(*profile, "channel");
            if (!channels || channels->size() != 3) {
                return false;
            }

            const auto kind = DefaultStoragePoolKind(defaults.DiskType);
            const TString pdiskCategory = ToString(defaults.SingleNode ? ui64{1} : DefaultPDiskCategory(defaults.DiskType));
            const auto requiredForSingle = defaults.SingleNode ? EPresence::Required : EPresence::Optional;
            const auto pdiskPresence = defaults.SingleNode || pdiskCategory != "0" ? EPresence::Required : EPresence::Optional;
            return std::all_of(channels->begin(), channels->end(), [&](const auto& channelNode) {
                auto channel = AsMap(channelNode);
                return channel && MatchesScalarFields(
                                      *channel,
                                      {
                                          {"erasure_species", defaults.Erasure, requiredForSingle},
                                          {"pdisk_category", pdiskCategory, pdiskPresence},
                                          {"storage_pool_kind", kind, requiredForSingle},
                                          {"vdisk_category", "Default", EPresence::Optional},
                                      });
            });
        }

        template <class T>
        struct TEffectiveSetting {
            std::optional<T> Value;
            bool Explicit = false;
            bool Consistent = true;

            bool Observe(std::optional<T> candidate, bool mayAdopt = true) {
                if (!candidate || !Consistent) {
                    return false;
                }
                if (Value && *Value != *candidate) {
                    Consistent = false;
                    return false;
                }
                if (!Value && !Explicit && mayAdopt) {
                    Value = candidate;
                    return true;
                }
                return false;
            }

            explicit operator bool() const {
                return Consistent && Value.has_value();
            }
        };

        template <class T, class TParser>
        TEffectiveSetting<T> ReadSetting(const NFyaml::TMapping& config, TStringBuf key, TParser parser) {
            TEffectiveSetting<T> result;
            result.Explicit = config.Has(TString(key));
            if (!result.Explicit) {
                return result;
            }
            auto value = FindScalar(config, key);
            result.Value = value ? parser(*value) : std::nullopt;
            result.Consistent = result.Value.has_value();
            return result;
        }

        std::optional<TGeneratedDefaults> GetGeneratedDefaults(const NFyaml::TMapping& config, std::optional<size_t> hostCount,
                                                               const std::optional<TString>& erasure, const TEffectiveSetting<EDefaultDiskType>& diskType) {
            if (!hostCount || *hostCount == 0 || !diskType) {
                return std::nullopt;
            }

            const bool singleNode = *hostCount == 1;
            if (singleNode) {
                const auto fallback = diskType.Explicit ? *diskType.Value : EDefaultDiskType::Ssd;
                if (GetSingleNodeDiskType(config, fallback) != diskType.Value) {
                    return std::nullopt;
                }
                return TGeneratedDefaults{*diskType.Value, "none", true};
            }
            if (!erasure) {
                return std::nullopt;
            }
            return TGeneratedDefaults{*diskType.Value, *erasure, false};
        }

    } // anonymous namespace

    TMigrationConfigMergeResult MergeConfigsForMigration(const TString& staticConfig, const TString& dynamicConfig) {
        auto staticDoc = NFyaml::TDocument::Parse(staticConfig);
        auto dynamicDoc = NFyaml::TDocument::Parse(dynamicConfig);

        auto staticRootNode = staticDoc.Root();
        Y_ENSURE_EX(staticRootNode && staticRootNode.Type() == NFyaml::ENodeType::Mapping, TYamlConfigEx() << "Static config must be a non-empty mapping in simple V1 format");
        auto staticRoot = staticRootNode.Map();
        Y_ENSURE_EX(!staticRoot.Has("config"), TYamlConfigEx() << "Static config must use simple V1 format without a 'config' wrapper");

        auto dynamicConfigMap = GetMainConfig(dynamicDoc, "Dynamic config");
        PromoteDomainsSecurityConfig(dynamicDoc, dynamicConfigMap);

        TVector<TString> staticConfigPrecedencePaths;
        for (const auto& pair : staticRoot) {
            const auto key = pair.Key().Scalar();
            const bool staticWins = std::find(MigrationStorageKeys.begin(), MigrationStorageKeys.end(), key) != MigrationStorageKeys.end();
            if (staticWins && dynamicConfigMap.Has(key)) {
                staticConfigPrecedencePaths.push_back(TStringBuilder() << "config." << key);
            }
            if (staticWins || !dynamicConfigMap.Has(key)) {
                ReplaceMapValue(dynamicDoc, dynamicConfigMap, pair.Key(), pair.Value());
            }
        }

        PromoteDomainsSecurityConfig(dynamicDoc, dynamicConfigMap);
        return {
            .Config = std::move(dynamicDoc),
            .StaticConfigPrecedencePaths = std::move(staticConfigPrecedencePaths),
        };
    }

    NFyaml::TDocument CleanupConfigV2Migration(const TString& input) {
        auto doc = NFyaml::TDocument::Parse(input);
        auto config = GetMainConfig(doc);
        Y_ENSURE_EX(config.Has("domains_config") && config.Has("blob_storage_config"),
                    TYamlConfigEx() << "Config must contain both 'config.domains_config' and 'config.blob_storage_config'; use a freshly fetched config before cleanup");

        ValidateCleanupPrerequisites(doc, config);

        auto domains = FindMap(config, "domains_config");
        Y_ENSURE_EX(domains, TYamlConfigEx() << "'config.domains_config' section must be a mapping");
        auto blobStorage = FindMap(config, "blob_storage_config");
        Y_ENSURE_EX(blobStorage, TYamlConfigEx() << "'config.blob_storage_config' section must be a mapping");
        auto domain = GetSingleDomain(*domains);

        auto failDomain = ReadSetting<EFailDomainType>(config, "fail_domain_type", TryParseFailDomainType);
        auto diskType = ReadSetting<EDefaultDiskType>(config, "default_disk_type", TryParseDefaultDiskType);
        Y_ENSURE_EX(!failDomain.Explicit || failDomain, TYamlConfigEx() << "Invalid 'config.fail_domain_type'");
        Y_ENSURE_EX(!diskType.Explicit || diskType, TYamlConfigEx() << "Invalid 'config.default_disk_type'");
        const auto erasure = GetEffectiveErasure(config);
        const auto hostCount = GetHostCount(config);

        if (auto security = domains->pair_at_opt("security_config"); security) {
            PromoteLegacyValue(doc, config, "security_config", security.Value(), "config.domains_config.security_config");
            domains->Remove(security.Key());
        }
        RemoveMapValue(*domains, "state_storage");

        std::optional<NFyaml::TNodeRef> storagePoolTypes;
        std::optional<TVector<TStoragePool>> storagePools;
        if (domain) {
            if (domain->Has("name")) {
                auto name = FindScalar(*domain, "name");
                Y_ENSURE_EX(name, TYamlConfigEx() << "'config.domains_config.domain[0].name' must be a scalar");
            }

            if (domain->Has("storage_pool_types")) {
                storagePoolTypes = domain->at("storage_pool_types");
                storagePools = AnalyzeStoragePools(*storagePoolTypes);
                if (storagePools) {
                    const auto inferredFailDomain = GetCommonValue<EFailDomainType>(*storagePools, [](const auto& pool) { return pool.FailDomainType; });
                    if (failDomain.Observe(inferredFailDomain)) {
                        config.Append(doc.CreateScalar("fail_domain_type"), doc.CreateScalar(TString(FailDomainTypeName(*failDomain.Value))));
                    }

                    const auto inferredDiskType = GetCommonValue<EDefaultDiskType>(*storagePools, [](const auto& pool) { return pool.DiskType; });
                    const bool impliedByDrives = inferredDiskType && IsDefaultDiskTypeAlreadyImplied(config, *inferredDiskType);
                    if (diskType.Observe(inferredDiskType, impliedByDrives)) {
                        config.Append(doc.CreateScalar("default_disk_type"), doc.CreateScalar(TString(DefaultDiskTypeName(*diskType.Value))));
                    }
                }
            }
        }

        const auto defaults = GetGeneratedDefaults(config, hostCount, erasure, diskType);
        if (storagePoolTypes) {
            const bool redundant = storagePools && defaults && failDomain && (!defaults->SingleNode || failDomain.Value == EFailDomainType::Rack) &&
                                   MatchesStorageDefaults(*storagePools, *defaults, *failDomain.Value);
            if (redundant) {
                RemoveRedundantValue(config, "storage_pool_types", *storagePoolTypes, "config.domains_config.domain[0].storage_pool_types");
                domain->Remove(domain->pair_at("storage_pool_types").Key());
            } else if (CanLiftStoragePoolTypes(*domains, *domain)) {
                PromoteLegacyValue(doc, config, "storage_pool_types", *storagePoolTypes, "config.domains_config.domain[0].storage_pool_types");
                domain->Remove(domain->pair_at("storage_pool_types").Key());
            } else {
                RemoveRedundantValue(config, "storage_pool_types", *storagePoolTypes, "config.domains_config.domain[0].storage_pool_types");
            }
        }

        if (domain && HasOnlyKeys(*domains, {"domain"}) && HasOnlyKeys(*domain, {"name"})) {
            if (domain->Has("name")) {
                const auto name = domain->at("name");
                if (name.Scalar() == "Root") {
                    RemoveRedundantValue(config, "domain_name", name, "config.domains_config.domain[0].name");
                } else {
                    PromoteLegacyValue(doc, config, "domain_name", name, "config.domains_config.domain[0].name");
                }
            }
            auto domainList = FindSequence(*domains, "domain");
            domainList->Remove(domainList->at(0));
            domains->Remove(domains->pair_at("domain").Key());
        } else if (auto domainList = FindSequence(*domains, "domain"); domainList && domainList->empty() && HasOnlyKeys(*domains, {"domain"})) {
            domains->Remove(domains->pair_at("domain").Key());
        }

        if (auto channelProfile = config.pair_at_opt("channel_profile_config");
            channelProfile && defaults && MatchesChannelProfileDefaults(channelProfile.Value(), *defaults)) {
            config.Remove(channelProfile.Key());
        }

        if (auto staticErasure = config.pair_at_opt("static_erasure"); staticErasure) {
            Y_ENSURE_EX(staticErasure.Value().Type() == NFyaml::ENodeType::Scalar, TYamlConfigEx() << "Invalid 'config.static_erasure'");
            PromoteLegacyValue(doc, config, "erasure", staticErasure.Value(), "config.static_erasure");
            config.Remove(staticErasure.Key());
        }

        for (const auto key : {TStringBuf("service_set"), TStringBuf("define_host_config"), TStringBuf("define_box")}) {
            RemoveMapValue(*blobStorage, key);
        }

        if (domains->empty()) {
            config.Remove(config.pair_at("domains_config").Key());
        }
        if (blobStorage->empty()) {
            config.Remove(config.pair_at("blob_storage_config").Key());
        }
        return doc;
    }

} // namespace NKikimr::NYamlConfig
