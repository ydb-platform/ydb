#pragma once

#include <ydb/library/fyamlcpp/fyamlcpp.h>
#include <ydb/library/actors/core/actor.h>

#include <openssl/sha.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/set.h>
#include <util/generic/map.h>
#include <util/stream/str.h>

#include <unordered_map>
#include <map>
#include <string>

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

struct TResolvedConfig {
    TVector<TString> Labels;
    TMap<TSet<TVector<TLabel>>, TDocumentConfig> Configs;
};

/**
 * Generates configs for all label combinations
 */
TResolvedConfig ResolveAll(NFyaml::TDocument& doc);

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
 * Parses config version
 */
ui64 GetVersion(const TString& config);

/**
 * Represents config metadata
 */
struct TMetadata {
    std::optional<ui64> Version;
    std::optional<TString> Cluster;
};

/**
 * Parses config metadata
 */
TMetadata GetMetadata(const TString& config);

/**
 * Represents volatile config metadata
 */
struct TVolatileMetadata {
    std::optional<ui64> Version;
    std::optional<TString> Cluster;
    std::optional<ui64> Id;
};

/**
 * Parses volatile config metadata
 */
TVolatileMetadata GetVolatileMetadata(const TString& config);

/**
 * Replaces metadata in config
 */
TString ReplaceMetadata(const TString& config, const TMetadata& metadata);

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
 * Strips metadata from config
 */
TString StripMetadata(const TString& config);

} // namespace NKikimr::NYamlConfig
