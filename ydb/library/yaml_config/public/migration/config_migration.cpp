#include "config_migration.h"
#include "yaml_helpers.h"

#include <util/generic/hash_set.h>
#include <util/string/ascii.h>

#include <array>
#include <exception>
#include <optional>
#include <utility>

namespace NKikimr::NYamlConfig {
    namespace {

        using NMigrationDetail::AsMap;
        using NMigrationDetail::AsSequence;
        using NMigrationDetail::FindMap;
        using NMigrationDetail::FindScalar;

        constexpr std::array<TStringBuf, 3> StaticGroupTopologyKeys = {
            "pdisks",
            "vdisks",
            "groups",
        };

        constexpr std::array<TStringBuf, 4> DomainRepresentationKeys = {
            "domains_config",
            "security_config",
            "storage_pool_types",
            "domain_name",
        };

        constexpr std::array<TStringBuf, 3> ErasureRepresentationKeys = {
            "static_erasure",
            "erasure",
            "self_management_config",
        };

        struct TMergeConflict {
            TString Placeholder;
            TString StaticSection;
            TString DynamicSection;
        };

        bool IsStaticOwnedKey(TStringBuf key) {
            return key == "hosts" || key == "host_configs" || key == "static_erasure";
        }

        std::optional<NFyaml::TNodeRef> FindNode(const NFyaml::TMapping& map, TStringBuf key) {
            const TString name(key);
            return map.Has(name) ? std::make_optional(map.at(name)) : std::nullopt;
        }

        std::optional<NFyaml::TNodeRef> FindNestedNode(const NFyaml::TMapping& map, TStringBuf section, TStringBuf key) {
            const auto nested = FindMap(map, section);
            return nested ? FindNode(*nested, key) : std::nullopt;
        }

        bool ValuesConflict(std::optional<NFyaml::TNodeRef> lhs, std::optional<NFyaml::TNodeRef> rhs) {
            return lhs && rhs && !lhs->DeepEqual(*rhs);
        }

        void RemoveMapValue(NFyaml::TMapping& map, TStringBuf key) {
            if (auto value = map.pair_at_opt(TString(key)); value) {
                map.Remove(value.Key());
            }
        }

        void ReplaceMapValue(NFyaml::TDocument& targetDoc, NFyaml::TMapping& targetMap, const NFyaml::TNodeRef& sourceKey,
                             const NFyaml::TNodeRef& sourceValue) {
            if (auto current = targetMap.pair_at_opt(sourceKey.Scalar()); current) {
                targetMap.Remove(current.Key());
            }
            targetMap.Append(sourceKey.Copy(targetDoc).Ref(), sourceValue.Copy(targetDoc).Ref());
        }

        TString EmitMappingEntry(const NFyaml::TNodeRef& key, const NFyaml::TNodeRef& value) {
            auto doc = NFyaml::TDocument::Parse("__entry__: null\n");
            auto entry = doc.Root().Map().pair_at("__entry__");
            entry.SetKey(key.Copy(doc).Ref());
            entry.SetValue(value.Copy(doc).Ref());
            TString result(doc.EmitToCharArray().get());
            while (!result.empty() && (result.back() == '\n' || result.back() == '\r')) {
                result.pop_back();
            }
            return result;
        }

        template <size_t Size>
        bool Contains(const std::array<TStringBuf, Size>& keys, TStringBuf key) {
            for (const auto candidate : keys) {
                if (candidate == key) {
                    return true;
                }
            }
            return false;
        }

        template <size_t Size>
        TString EmitMappingEntries(const NFyaml::TMapping& map, const std::array<TStringBuf, Size>& keys) {
            TStringBuilder result;
            bool first = true;
            for (const auto& pair : map) {
                if (pair.Key().Type() != NFyaml::ENodeType::Scalar || !Contains(keys, pair.Key().Scalar())) {
                    continue;
                }
                if (!first) {
                    result << '\n';
                }
                first = false;
                result << EmitMappingEntry(pair.Key(), pair.Value());
            }
            return result;
        }

        TString Indent(TStringBuf value, TStringBuf indentation) {
            TStringBuilder result;
            size_t begin = 0;
            while (begin < value.size()) {
                const size_t end = value.find('\n', begin);
                result << indentation << value.SubStr(begin, end == TStringBuf::npos ? TStringBuf::npos : end - begin);
                if (end == TStringBuf::npos) {
                    break;
                }
                result << '\n';
                begin = end + 1;
            }
            return result;
        }

        TString MakeConflictPlaceholderPrefix(const NFyaml::TDocument& staticDoc, const NFyaml::TDocument& dynamicDoc) {
            const TString staticConfig(staticDoc.EmitToCharArray().get());
            const TString dynamicConfig(dynamicDoc.EmitToCharArray().get());
            for (size_t suffix = 0;; ++suffix) {
                const TString candidate = TStringBuilder() << "__ydb_config_migration_conflict_" << suffix << "_";
                if (!staticConfig.Contains(candidate) && !dynamicConfig.Contains(candidate)) {
                    return candidate;
                }
            }
        }

        void AddConflict(NFyaml::TDocument& targetDoc, NFyaml::TNodePairRef target, TString staticSection,
                         TString dynamicSection, TStringBuf placeholderPrefix, TVector<TMergeConflict>& conflicts) {
            const TString placeholder = TStringBuilder() << placeholderPrefix << conflicts.size() << "__";
            conflicts.push_back({
                .Placeholder = placeholder,
                .StaticSection = std::move(staticSection),
                .DynamicSection = std::move(dynamicSection),
            });
            target.SetValue(targetDoc.CreateScalar(placeholder));
        }

        void AddConflict(NFyaml::TDocument& targetDoc, NFyaml::TNodePairRef target,
                         const NFyaml::TNodePairRef& source, TStringBuf placeholderPrefix,
                         TVector<TMergeConflict>& conflicts) {
            AddConflict(targetDoc, target, EmitMappingEntry(source.Key(), source.Value()),
                        EmitMappingEntry(target.Key(), target.Value()), placeholderPrefix, conflicts);
        }

        void AddMissingStaticConflict(NFyaml::TDocument& targetDoc, NFyaml::TNodePairRef target,
                                      TStringBuf placeholderPrefix, TVector<TMergeConflict>& conflicts) {
            AddConflict(targetDoc, target, {}, EmitMappingEntry(target.Key(), target.Value()),
                        placeholderPrefix, conflicts);
        }

        template <size_t Size>
        void AddRepresentationConflict(NFyaml::TDocument& targetDoc, NFyaml::TMapping& target,
                                       const NFyaml::TMapping& source, const std::array<TStringBuf, Size>& keys,
                                       TStringBuf placeholderPrefix, THashSet<TString>& handledSourceKeys,
                                       TVector<TMergeConflict>& conflicts) {
            std::optional<TString> anchor;
            for (const auto& pair : target) {
                if (pair.Key().Type() == NFyaml::ENodeType::Scalar && Contains(keys, pair.Key().Scalar())) {
                    anchor = pair.Key().Scalar();
                    break;
                }
            }
            Y_ENSURE_EX(anchor, TYamlConfigEx() << "Failed to locate a dynamic representation for merge conflict");

            auto targetEntry = target.pair_at(*anchor);
            AddConflict(targetDoc, targetEntry, EmitMappingEntries(source, keys), EmitMappingEntries(target, keys),
                        placeholderPrefix, conflicts);

            for (const auto key : keys) {
                const TString name(key);
                if (name != *anchor) {
                    RemoveMapValue(target, key);
                }
                if (source.Has(name)) {
                    handledSourceKeys.insert(name);
                }
            }
        }

        bool DomainRepresentationsConflict(const NFyaml::TMapping& staticConfig,
                                           const NFyaml::TMapping& dynamicConfig) {
            const bool securityConflict =
                ValuesConflict(FindNestedNode(staticConfig, "domains_config", "security_config"),
                               FindNode(dynamicConfig, "security_config")) ||
                ValuesConflict(FindNode(staticConfig, "security_config"),
                               FindNestedNode(dynamicConfig, "domains_config", "security_config"));

            const bool storagePoolsConflict =
                (staticConfig.Has("domains_config") && dynamicConfig.Has("storage_pool_types")) ||
                (staticConfig.Has("storage_pool_types") && dynamicConfig.Has("domains_config")) ||
                (staticConfig.Has("storage_pool_types") && dynamicConfig.Has("domain_name")) ||
                (staticConfig.Has("domain_name") && dynamicConfig.Has("storage_pool_types"));

            return securityConflict || storagePoolsConflict;
        }

        bool ErasureRepresentationsConflict(const NFyaml::TMapping& staticConfig,
                                            const NFyaml::TMapping& dynamicConfig) {
            return ValuesConflict(FindNode(staticConfig, "static_erasure"), FindNode(dynamicConfig, "erasure")) ||
                ValuesConflict(FindNode(staticConfig, "erasure"), FindNode(dynamicConfig, "static_erasure")) ||
                ValuesConflict(FindNode(staticConfig, "static_erasure"),
                               FindNestedNode(dynamicConfig, "self_management_config", "erasure_species")) ||
                ValuesConflict(FindNode(staticConfig, "erasure"),
                               FindNestedNode(dynamicConfig, "self_management_config", "erasure_species")) ||
                ValuesConflict(FindNestedNode(staticConfig, "self_management_config", "erasure_species"),
                               FindNode(dynamicConfig, "static_erasure")) ||
                ValuesConflict(FindNestedNode(staticConfig, "self_management_config", "erasure_species"),
                               FindNode(dynamicConfig, "erasure"));
        }

        NFyaml::TDocument CopyToDocument(const NFyaml::TNodeRef& node) {
            auto result = NFyaml::TDocument::Parse("{}");
            result.SetRoot(node.Copy(result).Ref());
            return result;
        }

        bool HasMergeableServiceSet(const NFyaml::TMapping& blobStorage) {
            if (!blobStorage.Has("service_set")) {
                return true;
            }
            const auto serviceSet = blobStorage.at("service_set");
            return !serviceSet.Tag() && serviceSet.Type() == NFyaml::ENodeType::Mapping;
        }

        void RemoveStaticGroupTopologyFromBlobStorage(NFyaml::TDocument& doc) {
            auto blobStorage = doc.Root().Map();
            if (!blobStorage.Has("service_set")) {
                return;
            }

            auto serviceSet = blobStorage.at("service_set").Map();
            for (const auto key : StaticGroupTopologyKeys) {
                RemoveMapValue(serviceSet, key);
            }
            if (serviceSet.empty()) {
                RemoveMapValue(blobStorage, "service_set");
            }
        }

        void ApplyStaticGroupTopology(NFyaml::TDocument& targetDoc, NFyaml::TMapping& targetBlobStorage,
                                      const NFyaml::TMapping& sourceBlobStorage) {
            std::optional<NFyaml::TMapping> targetServiceSet;
            if (targetBlobStorage.Has("service_set")) {
                targetServiceSet = targetBlobStorage.at("service_set").Map();
                for (const auto key : StaticGroupTopologyKeys) {
                    RemoveMapValue(*targetServiceSet, key);
                }
            }

            if (sourceBlobStorage.Has("service_set")) {
                auto sourceServiceSet = sourceBlobStorage.at("service_set").Map();
                for (const auto key : StaticGroupTopologyKeys) {
                    if (auto sourceValue = sourceServiceSet.pair_at_opt(TString(key)); sourceValue) {
                        if (!targetServiceSet) {
                            targetBlobStorage.Append(targetDoc.CreateScalar("service_set"), targetDoc.CreateMapping());
                            targetServiceSet = targetBlobStorage.at("service_set").Map();
                        }
                        ReplaceMapValue(targetDoc, *targetServiceSet, sourceValue.Key(), sourceValue.Value());
                    }
                }
            }

            if (targetServiceSet && targetServiceSet->empty()) {
                RemoveMapValue(targetBlobStorage, "service_set");
            }
        }

        bool MergeBlobStorageConfig(NFyaml::TDocument& targetDoc, NFyaml::TNodeRef targetValue,
                                    NFyaml::TNodeRef sourceValue) {
            if (targetValue.DeepEqual(sourceValue)) {
                return true;
            }

            if (targetValue.Tag() || sourceValue.Tag() || targetValue.Type() != NFyaml::ENodeType::Mapping ||
                sourceValue.Type() != NFyaml::ENodeType::Mapping) {
                return false;
            }

            auto targetBlobStorage = targetValue.Map();
            auto sourceBlobStorage = sourceValue.Map();
            if (!HasMergeableServiceSet(targetBlobStorage) || !HasMergeableServiceSet(sourceBlobStorage)) {
                return false;
            }

            auto targetWithoutTopology = CopyToDocument(targetValue);
            auto sourceWithoutTopology = CopyToDocument(sourceValue);
            RemoveStaticGroupTopologyFromBlobStorage(targetWithoutTopology);
            RemoveStaticGroupTopologyFromBlobStorage(sourceWithoutTopology);
            if (!targetWithoutTopology.Root().DeepEqual(sourceWithoutTopology.Root())) {
                return false;
            }

            ApplyStaticGroupTopology(targetDoc, targetBlobStorage, sourceBlobStorage);
            return true;
        }

        TString EmitMergeResult(NFyaml::TDocument& doc, const TVector<TMergeConflict>& conflicts,
                                TStringBuf staticConfigName, TStringBuf dynamicConfigName) {
            TString result(doc.EmitToCharArray().get());
            for (const auto& conflict : conflicts) {
                const size_t placeholder = result.find(conflict.Placeholder);
                Y_ENSURE_EX(placeholder != TString::npos, TYamlConfigEx() << "Failed to render merge conflict");
                Y_ENSURE_EX(result.find(conflict.Placeholder, placeholder + conflict.Placeholder.size()) == TString::npos,
                            TYamlConfigEx() << "Merge conflict placeholder is not unique");

                const size_t lineBegin = result.rfind('\n', placeholder);
                const size_t begin = lineBegin == TString::npos ? 0 : lineBegin + 1;
                const size_t lineEnd = result.find('\n', placeholder);
                const size_t end = lineEnd == TString::npos ? result.size() : lineEnd;
                size_t contentBegin = begin;
                while (contentBegin < end && result[contentBegin] == ' ') {
                    ++contentBegin;
                }
                const TStringBuf indentation(result.data() + begin, contentBegin - begin);

                TStringBuilder replacement;
                replacement << "<<<<<<< " << staticConfigName << '\n';
                if (!conflict.StaticSection.empty()) {
                    replacement << Indent(conflict.StaticSection, indentation) << '\n';
                }
                replacement << "=======\n";
                if (!conflict.DynamicSection.empty()) {
                    replacement << Indent(conflict.DynamicSection, indentation) << '\n';
                }
                replacement << ">>>>>>> " << dynamicConfigName;
                result.replace(begin, end - begin, TString(replacement));
            }
            return result;
        }

        NFyaml::TMapping GetMainConfig(NFyaml::TDocument& doc, TStringBuf description = "Config") {
            auto rootNode = doc.Root();
            Y_ENSURE_EX(rootNode && rootNode.Type() == NFyaml::ENodeType::Mapping,
                        TYamlConfigEx() << description << " must be a non-empty mapping in MainConfig format");
            auto root = rootNode.Map();
            Y_ENSURE_EX(root.Has("config"), TYamlConfigEx() << description << " must have a 'config' section");
            auto config = AsMap(root.at("config"));
            Y_ENSURE_EX(config, TYamlConfigEx() << description << " 'config' section must be a mapping");
            return *config;
        }

        NFyaml::TDocument ParseMigrationConfig(const TString& input) {
            size_t lineNumber = 1;
            for (size_t begin = 0; begin < input.size(); ++lineNumber) {
                const size_t end = input.find('\n', begin);
                const TStringBuf line(input.data() + begin, end == TString::npos ? input.size() - begin : end - begin);
                Y_ENSURE_EX(!line.StartsWith("<<<<<<<"),
                            TYamlConfigEx() << "Input contains an unresolved merge conflict at line " << lineNumber
                                            << "; resolve all conflict markers before continuing");
                if (end == TString::npos) {
                    break;
                }
                begin = end + 1;
            }
            return NFyaml::TDocument::Parse(input);
        }

        NFyaml::TDocument ParseNamedConfig(const TString& input, TStringBuf name) {
            try {
                return NFyaml::TDocument::Parse(input);
            } catch (const std::exception& error) {
                ythrow TYamlConfigEx() << "Failed to parse '" << name << "': " << error.what();
            }
        }

        NFyaml::TMapping GetOrCreateMap(NFyaml::TDocument& doc, NFyaml::TMapping& parent, TStringBuf key, TStringBuf path) {
            const TString name(key);
            if (parent.Has(name)) {
                auto map = AsMap(parent.at(name));
                Y_ENSURE_EX(map, TYamlConfigEx() << "'" << path << "' section must be a mapping");
                return *map;
            }

            parent.Append(doc.CreateScalar(name), doc.CreateMapping());
            return parent.at(name).Map();
        }

        void SetScalar(NFyaml::TDocument& doc, NFyaml::TMapping& map, TStringBuf key, TStringBuf value) {
            const TString name(key);
            if (auto current = map.pair_at_opt(name); current) {
                current.SetValue(doc.CreateScalar(TString(value)));
            } else {
                map.Append(doc.CreateScalar(name), doc.CreateScalar(TString(value)));
            }
        }

        void SetBool(NFyaml::TDocument& doc, NFyaml::TMapping& map, TStringBuf key, bool enabled) {
            const TString scalar = enabled ? "true" : "false";
            SetScalar(doc, map, key, scalar);
        }

        bool ConfigV2FeatureFlagEnabled(const NFyaml::TMapping& config) {
            const auto featureFlags = FindMap(config, "feature_flags");
            const auto enabled = featureFlags ? FindScalar(*featureFlags, "switch_to_config_v2") : std::nullopt;
            return enabled && AsciiEqualsIgnoreCase(*enabled, "true");
        }

        bool SelfManagementEnabled(const NFyaml::TMapping& config) {
            const auto selfManagement = FindMap(config, "self_management_config");
            const auto enabled = selfManagement ? FindScalar(*selfManagement, "enabled") : std::nullopt;
            return enabled && AsciiEqualsIgnoreCase(*enabled, "true");
        }

        template <class TCallback>
        void ForEachSelectorConfig(NFyaml::TDocument& doc, TCallback&& callback) {
            auto root = doc.Root().Map();
            if (!root.Has("selector_config")) {
                return;
            }

            auto selectors = AsSequence(root.at("selector_config"));
            Y_ENSURE_EX(selectors, TYamlConfigEx() << "'selector_config' section must be a sequence");
            for (size_t index = 0; index < selectors->size(); ++index) {
                const auto selectorNode = selectors->at(index);
                const TString selectorPath = TStringBuilder() << "selector_config[" << index << "]";
                auto selector = AsMap(selectorNode);
                Y_ENSURE_EX(selector, TYamlConfigEx() << "'" << selectorPath << "' entry must be a mapping");
                if (!selector->Has("config")) {
                    continue;
                }

                const TString configPath = TStringBuilder() << selectorPath << ".config";
                auto selectorConfig = FindMap(*selector, "config");
                Y_ENSURE_EX(selectorConfig, TYamlConfigEx() << "'" << configPath << "' section must be a mapping");
                callback(*selectorConfig, configPath);
            }
        }

        void EnsureSelectorsKeepValue(NFyaml::TDocument& doc, TStringBuf section, TStringBuf key, bool requiredValue) {
            ForEachSelectorConfig(doc, [&](const NFyaml::TMapping& selectorConfig, TStringBuf configPath) {
                const TString sectionName(section);
                if (!selectorConfig.Has(sectionName)) {
                    return;
                }

                const TString sectionPath = TStringBuilder() << configPath << "." << section;
                const auto sectionNode = selectorConfig.at(sectionName);
                const auto sectionMap = AsMap(sectionNode);
                Y_ENSURE_EX(sectionMap, TYamlConfigEx() << "'" << sectionPath << "' section must be a mapping");

                const TString keyName(key);
                const auto tag = sectionNode.Tag();
                const bool inherits = tag && *tag == "!inherit";
                const auto value = FindScalar(*sectionMap, key);
                const TStringBuf requiredScalar = requiredValue ? TStringBuf("true") : TStringBuf("false");
                const bool keepsValue = value ? AsciiEqualsIgnoreCase(*value, requiredScalar)
                                              : inherits || !requiredValue;
                Y_ENSURE_EX(keepsValue,
                            TYamlConfigEx() << "Selector at '" << sectionPath << "' does not preserve required 'config."
                                            << section << "." << key << ": " << requiredScalar
                                            << "'; use '!inherit' without overriding '" << key << "', or set '"
                                            << key << ": " << requiredScalar << "'");
            });
        }

        void ValidateCleanupPrerequisites(NFyaml::TDocument& doc, const NFyaml::TMapping& config) {
            const auto selfManagement = FindMap(config, "self_management_config");
            const auto selfManagementEnabled = selfManagement ? FindScalar(*selfManagement, "enabled") : std::nullopt;
            Y_ENSURE_EX(selfManagementEnabled && AsciiEqualsIgnoreCase(*selfManagementEnabled, "true"),
                        TYamlConfigEx() << "Cleanup requires 'config.self_management_config.enabled: true'");

            Y_ENSURE_EX(ConfigV2FeatureFlagEnabled(config),
                        TYamlConfigEx() << "Cleanup requires 'config.feature_flags.switch_to_config_v2: true'");
            EnsureSelectorsKeepValue(doc, "feature_flags", "switch_to_config_v2", true);
            EnsureSelectorsKeepValue(doc, "self_management_config", "enabled", true);
        }

        void RemoveStaticGroupTopology(NFyaml::TMapping& config, TStringBuf configPath) {
            if (!config.Has("blob_storage_config")) {
                return;
            }

            const TString blobStoragePath = TStringBuilder() << configPath << ".blob_storage_config";
            auto blobStorage = FindMap(config, "blob_storage_config");
            Y_ENSURE_EX(blobStorage, TYamlConfigEx() << "'" << blobStoragePath << "' section must be a mapping");
            if (!blobStorage->Has("service_set")) {
                return;
            }

            const TString serviceSetPath = TStringBuilder() << blobStoragePath << ".service_set";
            auto serviceSet = FindMap(*blobStorage, "service_set");
            Y_ENSURE_EX(serviceSet, TYamlConfigEx() << "'" << serviceSetPath << "' section must be a mapping");
            for (const auto key : StaticGroupTopologyKeys) {
                RemoveMapValue(*serviceSet, key);
            }
        }

        void RemoveLegacyStateStorage(NFyaml::TMapping& config) {
            if (!config.Has("domains_config")) {
                return;
            }

            auto domains = FindMap(config, "domains_config");
            Y_ENSURE_EX(domains, TYamlConfigEx() << "'config.domains_config' section must be a mapping");
            RemoveMapValue(*domains, "state_storage");
        }

        void EnsureNoSelectorStateStorage(const NFyaml::TMapping& config, TStringBuf configPath) {
            if (!config.Has("domains_config")) {
                return;
            }

            const TString domainsPath = TStringBuilder() << configPath << ".domains_config";
            auto domains = FindMap(config, "domains_config");
            Y_ENSURE_EX(domains, TYamlConfigEx() << "'" << domainsPath << "' section must be a mapping");
            Y_ENSURE_EX(!domains->Has("state_storage"),
                        TYamlConfigEx() << "Cannot safely clean up legacy State Storage override at '"
                                        << domainsPath << ".state_storage'");
        }

        void CleanupSelectors(NFyaml::TDocument& doc) {
            ForEachSelectorConfig(doc, [&](NFyaml::TMapping& selectorConfig, TStringBuf configPath) {
                RemoveStaticGroupTopology(selectorConfig, configPath);
                EnsureNoSelectorStateStorage(selectorConfig, configPath);
            });
        }

    } // anonymous namespace

    TMigrationConfigMergeResult MergeConfigsForMigration(const TString& staticConfig, const TString& dynamicConfig,
                                                         TStringBuf staticConfigName, TStringBuf dynamicConfigName) {
        auto staticDoc = ParseNamedConfig(staticConfig, staticConfigName);
        auto dynamicDoc = ParseNamedConfig(dynamicConfig, dynamicConfigName);

        auto staticRootNode = staticDoc.Root();
        Y_ENSURE_EX(staticRootNode && staticRootNode.Type() == NFyaml::ENodeType::Mapping,
                    TYamlConfigEx() << "Static config must be a non-empty mapping in simple V1 format");
        auto staticRoot = staticRootNode.Map();
        Y_ENSURE_EX(!staticRoot.Has("config"), TYamlConfigEx() << "Static config must use simple V1 format without a 'config' wrapper");

        auto dynamicConfigMap = GetMainConfig(dynamicDoc, "Dynamic config");
        const TString placeholderPrefix = MakeConflictPlaceholderPrefix(staticDoc, dynamicDoc);
        TVector<TMergeConflict> conflicts;
        for (const auto key : {TStringBuf("nameservice_config"), TStringBuf("blob_storage_config")}) {
            if (!staticRoot.Has(TString(key))) {
                if (auto dynamicValue = dynamicConfigMap.pair_at_opt(TString(key)); dynamicValue) {
                    AddMissingStaticConflict(dynamicDoc, dynamicValue, placeholderPrefix, conflicts);
                }
            }
        }

        THashSet<TString> handledStaticKeys;
        if (DomainRepresentationsConflict(staticRoot, dynamicConfigMap)) {
            AddRepresentationConflict(dynamicDoc, dynamicConfigMap, staticRoot, DomainRepresentationKeys,
                                      placeholderPrefix, handledStaticKeys, conflicts);
        }
        if (ErasureRepresentationsConflict(staticRoot, dynamicConfigMap)) {
            AddRepresentationConflict(dynamicDoc, dynamicConfigMap, staticRoot, ErasureRepresentationKeys,
                                      placeholderPrefix, handledStaticKeys, conflicts);
        }

        for (const auto& pair : staticRoot) {
            Y_ENSURE_EX(pair.Key().Type() == NFyaml::ENodeType::Scalar,
                        TYamlConfigEx() << "Static config mapping keys must be scalars");
            const auto key = pair.Key().Scalar();
            if (handledStaticKeys.contains(TString(key))) {
                continue;
            }
            if (IsStaticOwnedKey(key)) {
                ReplaceMapValue(dynamicDoc, dynamicConfigMap, pair.Key(), pair.Value());
            } else if (auto current = dynamicConfigMap.pair_at_opt(key); current) {
                if (!current.Value().DeepEqual(pair.Value()) &&
                    (key != "blob_storage_config" || !MergeBlobStorageConfig(dynamicDoc, current.Value(), pair.Value()))) {
                    AddConflict(dynamicDoc, current, pair, placeholderPrefix, conflicts);
                }
            } else {
                dynamicConfigMap.Append(pair.Key().Copy(dynamicDoc).Ref(), pair.Value().Copy(dynamicDoc).Ref());
            }
        }

        return {
            .Config = EmitMergeResult(dynamicDoc, conflicts, staticConfigName, dynamicConfigName),
            .HasConflicts = !conflicts.empty(),
        };
    }

    NFyaml::TDocument SetConfigV2FeatureFlag(const TString& input, bool enabled) {
        auto doc = ParseMigrationConfig(input);
        auto config = GetMainConfig(doc);

        if (!enabled) {
            const auto selfManagement = FindMap(config, "self_management_config");
            const auto selfManagementEnabled = selfManagement ? FindScalar(*selfManagement, "enabled") : std::nullopt;
            Y_ENSURE_EX(!selfManagementEnabled || !AsciiEqualsIgnoreCase(*selfManagementEnabled, "true"),
                        TYamlConfigEx() << "Disable self-management before disabling the config V2 feature flag");
            EnsureSelectorsKeepValue(doc, "self_management_config", "enabled", false);
        }

        auto featureFlags = GetOrCreateMap(doc, config, "feature_flags", "config.feature_flags");
        SetBool(doc, featureFlags, "switch_to_config_v2", enabled);
        EnsureSelectorsKeepValue(doc, "feature_flags", "switch_to_config_v2", enabled);
        return doc;
    }

    NFyaml::TDocument SetSelfManagement(const TString& input, bool enabled) {
        auto doc = ParseMigrationConfig(input);
        auto config = GetMainConfig(doc);
        if (enabled) {
            Y_ENSURE_EX(ConfigV2FeatureFlagEnabled(config),
                        TYamlConfigEx() << "Self-management requires 'config.feature_flags.switch_to_config_v2: true'");
            EnsureSelectorsKeepValue(doc, "feature_flags", "switch_to_config_v2", true);
        }

        auto selfManagement = GetOrCreateMap(doc, config, "self_management_config", "config.self_management_config");
        SetBool(doc, selfManagement, "enabled", enabled);
        EnsureSelectorsKeepValue(doc, "self_management_config", "enabled", enabled);
        return doc;
    }

    bool IsSelfManagementEnabled(const TString& input) {
        auto doc = ParseMigrationConfig(input);
        return SelfManagementEnabled(GetMainConfig(doc));
    }

    void SetDiskFailDomainType(NFyaml::TDocument& doc) {
        auto config = GetMainConfig(doc);
        SetScalar(doc, config, "fail_domain_type", "disk");
    }

    bool HasDiskFailDomainType(NFyaml::TDocument& doc) {
        const auto failDomainType = FindScalar(GetMainConfig(doc), "fail_domain_type");
        return failDomainType && AsciiEqualsIgnoreCase(*failDomainType, "disk");
    }

    NFyaml::TDocument CleanupConfigV2Migration(const TString& input) {
        auto doc = ParseMigrationConfig(input);
        auto config = GetMainConfig(doc);
        ValidateCleanupPrerequisites(doc, config);
        RemoveStaticGroupTopology(config, "config");
        RemoveLegacyStateStorage(config);
        CleanupSelectors(doc);
        return doc;
    }

} // namespace NKikimr::NYamlConfig
