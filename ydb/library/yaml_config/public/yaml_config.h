#pragma once

#include <ydb/library/fyamlcpp/fyamlcpp.h>
#include <ydb/library/actors/core/actor.h>

#include <openssl/sha.h>

#include <functional>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/set.h>
#include <util/generic/map.h>
#include <util/generic/hash.h>
#include <util/stream/str.h>

#include <unordered_map>
#include <map>
#include <string>
#include <variant>

namespace NKikimr::NYamlConfig {

struct TYamlConfigEx : public yexception {};

using TDocumentConfig = std::pair<NFyaml::TDocument, NFyaml::TNodeRef>;

/**
 * Open - labels like tenant, where we don't know final set of values
 * Closed - labels with predefined set of values, e.g. size
 */
enum class EYamlConfigLabelTypeClass {
    Open,
    Closed,
};

/**
 * TNamedLabel - representation of label used for selector
 */
class TNamedLabel {
public:
    TString Name;
    TString Value;
    bool Inv = false;

    bool operator<(const TNamedLabel& other) const { return Name < other.Name; }
};

/**
 * TLabelType - represents known set of values for a label with its type
 */
class TLabelType {
public:
    EYamlConfigLabelTypeClass Class;
    TSet<TString> Values;

    bool operator==(const TLabelType& other) const { return Values == other.Values; }
};

class TLabelValueSet {
public:
    TSet<TString> Values;

    bool operator==(const TLabelValueSet& other) const { return Values == other.Values; }
};

class TSelector {
public:
    TMap<TString, TLabelValueSet> In;
    TMap<TString, TLabelValueSet> NotIn;
};

/**
 * TLabel is a representation of label for config resolution
 *
 * It can be in three states:
 * - Empty with Type == EType::Empty and Value == ""
 *   it equals empty or undefined label
 * - Common with Type == EType::Common and Value == arbitrary string
 *   it equals defined label with corresponding value
 * - Negative with Type == EType::Negative and Value == ""
 *   it is used for Open labels and equals any label not present in labels
 *   discovered by CollectLabels (and also not equals Empty label)
 */
struct TLabel {
    enum class EType {
        Negative = 0,
        Empty,
        Common,
    };

    EType Type;
    TString Value;

    bool operator<(const TLabel& other) const {
        int lhs = static_cast<int>(Type);
        int rhs = static_cast<int>(other.Type);
        return std::tie(lhs, Value) < std::tie(rhs, other.Value);
    }

    bool operator==(const TLabel& other) const {
        int lhs = static_cast<int>(Type);
        int rhs = static_cast<int>(other.Type);
        return std::tie(lhs, Value) == std::tie(rhs, other.Value);
    }
};

struct TIncompatibilityRule {
    struct TLabelPattern {
        TString Name;
        std::variant<std::monostate, THashSet<TString>> Values;
        bool Negated = false;
        
        bool Matches(const TLabel& label, const TString& actualLabelName) const;
    };
    
    TVector<TLabelPattern> Patterns;
    TString RuleName;
    
    enum class ESource {
        BuiltIn,
        UserDefined
    };
    ESource Source = ESource::BuiltIn;
};

class TIncompatibilityRules {
public:
    static constexpr const char* UNSET_LABEL_MARKER = "$unset";
    static constexpr const char* EMPTY_LABEL_MARKER = "$empty";
    
    static TIncompatibilityRules GetDefaultRules();
    
    void AddRule(TIncompatibilityRule rule);
    void RemoveRule(const TString& ruleName);
    
    bool IsCompatible(const TVector<TLabel>& combination,
                     const TVector<std::pair<TString, TSet<TLabel>>>& labelNames) const;
    
    bool IsCompatible(const TMap<TString, TString>& labels) const;
    
    void MergeWith(const TIncompatibilityRules& userRules);
    
    size_t GetRuleCount() const { return RulesByName.size(); }
    size_t GetDisabledCount() const { return DisabledRules.size(); }

    bool HasRules() const { return !RulesByName.empty() || !DisabledRules.empty(); }

private:
    TMap<TString, TIncompatibilityRule> RulesByName;
    THashSet<TString> DisabledRules;
    
    friend TIncompatibilityRules ParseIncompatibilityRules(const NFyaml::TNodeRef& root);
};

struct TYamlConfigModel {
    struct TSelectorModel {
        TString Description;
        TSelector Selector;
        NFyaml::TNodeRef Config;
    };

    const NFyaml::TDocument& Doc;
    NFyaml::TNodeRef Config;
    TMap<TString, TLabelType> AllowedLabels;
    TVector<TSelectorModel> Selectors;
    TIncompatibilityRules IncompatibilityRules;
};

/**
 * Collects all labels present in document
 * For Open labels gathers all labels from all selectors
 * For Closed labels additionally validates that there is no additional labels
 */
TMap<TString, TLabelType> CollectLabels(NFyaml::TDocument& doc);

/**
 * Parses config and fills corresponding struct
 */
TYamlConfigModel ParseConfig(NFyaml::TDocument& doc);

/**
 * Generates config for specific set of labels applying all matching selectors
 */
TDocumentConfig Resolve(
    const NFyaml::TDocument& doc,
    const TSet<TNamedLabel>& labels);

struct TResolvedConfig {
    TVector<TString> Labels;
    TMap<TSet<TVector<TLabel>>, TDocumentConfig> Configs;
};

/**
 * Generates configs for all label combinations
 */
TResolvedConfig ResolveAll(NFyaml::TDocument& doc);

/**
 * Generates unique resolved documents without materializing label combinations
 */
void ResolveUniqueDocs(
    NFyaml::TDocument& doc,
    const std::function<void(TDocumentConfig&&)>& onDocument);

/**
 * Calculates hash of resolved config
 * Used to ensure that cli resolves config the same as a server
 */
size_t Hash(const TResolvedConfig& config);

/**
 * Validates single YAML volatile config schema
 */
void ValidateVolatileConfig(NFyaml::TDocument& doc);

/**
 * Appends volatile configs to the end of selectors list
 * **Important**: Document should be a list with selectors
 */
void AppendVolatileConfigs(NFyaml::TDocument& config, NFyaml::TDocument& volatileConfig);

/**
 * Appends volatile configs to the end of selectors list
 * **Important**: Node should be a list with selectors
 */
void AppendVolatileConfigs(NFyaml::TDocument& config, NFyaml::TNodeRef& volatileConfig);

/**
 * Appends database config to the end of selectors list
 * **Important**: Document should be correct DatabaseConfig
 */
void AppendDatabaseConfig(NFyaml::TDocument& config, NFyaml::TDocument& databaseConfig);

/**
 * Fuses base config with console config (top-level per-key merge)
 * - Console config values take precedence (console wins)
 * - Base config fills in missing top-level keys only
 * - metadata, allowed_labels, selector_config from console preserved
 * @param baseConfig - simple format config (just fields, no metadata/selectors)
 * @param consoleConfig - full format config (metadata, config, allowed_labels, selector_config)
 * @return fused config document
 */
NFyaml::TDocument FuseConfigs(const TString& baseConfig, const TString& consoleConfig);

/**
 * Parses config version
 */
ui64 GetVersion(const TString& config);

/**
 * Represents config metadata
 */
struct TMainMetadata {
    std::optional<ui64> Version;
    std::optional<TString> Cluster;
};

/**
 * Represents config metadata
 */
struct TStorageMetadata {
    std::optional<ui64> Version;
    std::optional<TString> Cluster;
};

/**
 * Represents volatile config metadata
 */
struct TVolatileMetadata {
    std::optional<ui64> Version;
    std::optional<TString> Cluster;
    std::optional<ui64> Id;
};

/**
 * Represents database config metadata
 */
struct TDatabaseMetadata {
    // maybe we should enforce Cluster as well
    std::optional<ui64> Version;
    std::optional<TString> Database;
};

struct TError {
    TString Error;
};

/**
 * Parses config metadata
 */
std::variant<TMainMetadata, TDatabaseMetadata, TError> GetGenericMetadata(const TString& config);

/**
 * Parses config metadata
 */
TMainMetadata GetMainMetadata(const TString& config);

/**
 * Parses database config metadata
 */
TDatabaseMetadata GetDatabaseMetadata(const TString& config);

/**
 * Parses storage config metadata
 */
TStorageMetadata GetStorageMetadata(const TString& config);

/**
 * Parses volatile config metadata
 */
TVolatileMetadata GetVolatileMetadata(const TString& config);

/**
 * Replaces metadata in config
 */
TString ReplaceMetadata(const TString& config, const TMainMetadata& metadata);

/**
 * Takes valid MainConfig and increases version exactly by one
 */
 TString UpgradeMainConfigVersion(const TString& config);

/**
 * Takes valid MainConfig and increases version exactly by one
 */
TString UpgradeStorageConfigVersion(const TString& config);

/**
 * Replaces metadata in database config
 */
TString ReplaceMetadata(const TString& config, const TDatabaseMetadata& metadata);

/**
 * Replaces metadata in storage config
 */
TString ReplaceMetadata(const TString& config, const TStorageMetadata& metadata);

/**
 * Replaces volatile metadata in config
 */
TString ReplaceMetadata(const TString& config, const TVolatileMetadata& metadata);

/**
 * Checks whether string is volatile config or not
 */
bool IsVolatileConfig(const TString& config);

/**
 * Checks whether string is main config or not
 */
bool IsMainConfig(const TString& config);

/**
 * Checks whether string is storage config or not
 */
bool IsStorageConfig(const TString& config);

/**
 * Checks whether string is main config or not
 */
bool IsDatabaseConfig(const TString& config);

/**
 * Checks whether string is static config or not
 */
bool IsStaticConfig(const TString& config);

/**
 * Strips metadata from config
 */
TString StripMetadata(const TString& config);

} // namespace NKikimr::NYamlConfig
