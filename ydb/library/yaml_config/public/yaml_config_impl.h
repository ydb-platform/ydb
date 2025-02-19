#pragma once

#include "yaml_config.h"

namespace NKikimr::NYamlConfig {

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
* Replaces metadata in database config
*/
TString ReplaceMetadata(const TString& config, const TDatabaseMetadata& metadata);

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

class TLabelSet {
public:
    TString Name;
    TSet<TString> Values;
    bool Inv = false;

    bool operator<(const TLabelSet& other) const { return Name < other.Name; }
};

TString GetKey(const NFyaml::TNodeRef& node, TString key);

bool Fit(const TSelector& selector, const TSet<TNamedLabel>& labels);

bool Fit(
    const TSelector& selector,
    const TVector<TLabel>& labels,
    const TVector<std::pair<TString, TSet<TLabel>>>& names);

bool IsMapInherit(const NFyaml::TNodeRef& node);

bool IsSeqInherit(const NFyaml::TNodeRef& node);

bool IsRemove(const NFyaml::TNodeRef& node);

void RemoveTags(NFyaml::TDocument& doc);

void Inherit(NFyaml::TMapping& toMap, const NFyaml::TMapping& fromMap);

void Inherit(NFyaml::TSequence& toSeq, const NFyaml::TSequence& fromSeq, const TString& key);

void Apply(NFyaml::TNodeRef& to, const NFyaml::TNodeRef& from);

void Combine(
    TVector<TVector<TLabel>>& labelCombinations,
    TVector<TLabel>& combination,
    const TVector<std::pair<TString, TSet<TLabel>>>& labels,
    size_t offset);

TSelector ParseSelector(const NFyaml::TNodeRef& selectors);

} // namespace NKikimr::NYamlConfig
