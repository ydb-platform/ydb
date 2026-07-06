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

void CombineWithRules(
    TVector<TVector<TLabel>>& labelCombinations,
    TVector<TLabel>& combination,
    const TVector<std::pair<TString, TSet<TLabel>>>& labels,
    const TIncompatibilityRules& rules,
    size_t offset,
    size_t& prunedCount);

TSelector ParseSelector(const NFyaml::TNodeRef& selectors);

TIncompatibilityRules ParseIncompatibilityRules(const NFyaml::TNodeRef& root);

/**
 * Apply all matching selectors for specific set of labels
 *
 *  - existing 'config' nodes tags are preserved (if any) at all nested levels
 *  - tags (if any) of added nodes at any nested level are propagated from selectors into 'config'
 */
void ApplySelectors(NFyaml::TDocument& doc, const TSet<TNamedLabel>& labels);

/**
 * Re-create node tags to prevent dangling references after cross-document copy
 *
 *  - fy_node_copy does fyn->tag = fy_token_ref(fyn_from->tag), which only
 *    increments the refcount on the same tag token; the token's fy_input
 *    still references the source document's memory (input buffer for
 *    parser-created tags, or TUserDataHolder-owned TString for SetTag tags)
 *  - when the source document is destroyed, that backing memory is freed
 *    and the copied node's tag token becomes a dangling reference
 *  - this function recursively re-sets every tag via SetTag, which allocates
 *    a fresh fy_input and TUserDataHolder per node in the target document
 */
void DeepCopyTags(NFyaml::TNodeRef node);

} // namespace NKikimr::NYamlConfig
