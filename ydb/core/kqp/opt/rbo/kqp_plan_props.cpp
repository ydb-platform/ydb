#include "kqp_operator.h"

namespace NKikimr::NKqp {

// Out of line so plan properties can own operators without an include cycle.
TSubplanEntry::TSubplanEntry(TIntrusivePtr<IOperator> plan, ESubplanType type,
    TOrderedIUs<> tuple, std::optional<TInfoUnitId> resultIU)
    : Plan(std::move(plan))
    , Tuple(std::move(tuple))
    , ResultIU(resultIU)
    , Type(type)
{}

TSubplanEntry::~TSubplanEntry() = default;
TSubplanEntry::TSubplanEntry(TSubplanEntry&&) noexcept = default;
TSubplanEntry& TSubplanEntry::operator=(TSubplanEntry&&) noexcept = default;

void TSubplans::Add(TInfoUnitId binding, TIntrusivePtr<IOperator> plan, ESubplanType type,
    TOrderedIUs<> tuple, std::optional<TInfoUnitId> resultIU)
{
    Y_ENSURE(binding != TUnorderedIUs::InvalidBit, "Invalid subplan binding ID");
    Y_ENSURE(plan, "Cannot register a null subplan for " << binding);
    Y_ENSURE((type != ESubplanType::EXISTS) == resultIU.has_value(), "Scalar/IN subplan requires a result binding");
    Y_ENSURE(!resultIU || *resultIU != TUnorderedIUs::InvalidBit, "Invalid subplan result ID");
    Y_ENSURE(!Contains(binding), "Duplicate subplan binding " << binding);
    Entries.Add(binding, std::make_unique<TSubplanEntry>(std::move(plan), type, std::move(tuple), resultIU));
}

void TSubplans::ReplacePlan(TInfoUnitId binding, TIntrusivePtr<IOperator> plan) {
    Y_ENSURE(plan, "Cannot replace " << binding << " with a null subplan");
    Entries.At(binding)->Plan = std::move(plan);
}

void TSubplans::RefreshDependencies(TInfoUnitId binding) {
    auto& entry = *Entries.At(binding);
    TUnorderedIUs dependencies;
    for (const auto& item : IterateSubtree(entry.Plan.get())) {
        if (item.Current->Kind == EOperator::AddDependencies) {
            dependencies.UnionWith(CastOperator<TOpAddDependencies>(item.Current)->GetDependencies().MappedIUs());
        }
    }
    entry.DependentIUs = std::move(dependencies);
}

bool TSubplans::RebindInputs(TInfoUnitId binding, const TSubstitutions& substitutions) {
    if (substitutions.Keys().Empty()) {
        return false;
    }
    for (const auto& [from, to] : substitutions.Items()) {
        Y_ENSURE(!Contains(from) && !Contains(to), "Subplan result bindings cannot be rebound as parameters");
    }
    auto& entry = *Entries.At(binding);
    bool changed = false;
    for (size_t i = 0; i < entry.Tuple.Items().size(); ++i) {
        const auto id = entry.Tuple.Items()[i];
        if (const auto replacement = Substitute(id, substitutions); replacement != id) {
            entry.Tuple.ReplaceAt(i, replacement);
            changed = true;
        }
    }
    // Nested calls capture IDs defined in this scope. Their own parameters and
    // all local expressions remain valid when this scope's outer source changes.
    for (const auto& item : IterateSubtree(entry.Plan.get())) {
        if (item.Current->Kind == EOperator::AddDependencies) {
            changed |= CastOperator<TOpAddDependencies>(item.Current)->RebindCaptures(substitutions);
        }
    }
    if (changed) {
        RefreshDependencies(binding);
    }
    return changed;
}

bool TSubplans::RenameExternalReferences(const TSubstitutions& substitutions) {
    bool changed = false;
    for (const auto binding : Entries.Keys()) {
        changed |= RebindInputs(binding, substitutions);
    }
    return changed;
}

void TSubplans::RebindResults(const TSubstitutions& substitutions) {
    for (const auto binding : Entries.Keys()) {
        RebindResult(binding, substitutions);
    }
}

void TSubplans::RebindResult(TInfoUnitId binding, const TSubstitutions& substitutions) {
    auto& result = Entries.At(binding)->ResultIU;
    if (result) {
        result = Substitute(*result, substitutions);
    }
}

TVector<TString> TColumnLineage::GetAliases(const TMappedIUs<ui32>& relations) const {
    TSet<TString> aliases;
    TMap<TString, TSet<ui32>> instances;
    for (const auto& [id, relation] : relations.Items()) {
        const auto& entry = Relations_.at(relation);
        if (entry.SourceAlias.empty()) {
            aliases.insert(entry.TableName);
        } else {
            instances[entry.SourceAlias].insert(relation);
        }
    }
    for (const auto& [alias, aliasRelations] : instances) {
        for (size_t index = 0; index < aliasRelations.size(); ++index) {
            aliases.insert(index ? TStringBuilder() << alias << "_#" << index : alias);
        }
    }
    return TVector<TString>(aliases.begin(), aliases.end());
}

} // namespace NKikimr::NKqp
