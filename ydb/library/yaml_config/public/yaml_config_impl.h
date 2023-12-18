#pragma once

#include "yaml_config.h"

namespace NKikimr::NYamlConfig {

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
