#include "kqp_operator.h"

namespace NKikimr::NKqp {

namespace {

// Substitution is simultaneous: visit each original position once. Repeated
// IDs, directions, external labels and child positions retain their contracts.
template <class TValue>
void Substitute(TOrderedIUs<TValue>& columns, const TSubstitutions& substitutions) {
    for (size_t i = 0; i < columns.Items().size(); ++i) {
        const auto& entry = columns.Items()[i];
        if constexpr (std::is_void_v<TValue>) {
            columns.ReplaceAt(i, Substitute(entry, substitutions));
        } else {
            columns.ReplaceAt(i, Substitute(entry.first, substitutions), entry.second);
        }
    }
}

void Substitute(TUnorderedIUs& columns, const TSubstitutions& substitutions) {
    const auto ids = columns | std::views::transform([&](auto id) { return Substitute(id, substitutions); });
    TUnorderedIUs result;
    result.Assign(ids);
    columns = std::move(result);
}

template <typename TPair>
void Substitute(TPairedIUCollection<TPair>& pairs, const TSubstitutions& substitutions) {
    const auto entries = pairs.Items() | std::views::transform([&](const auto& pair) {
        auto result = pair;
        result.first = Substitute(pair.first, substitutions);
        result.second = Substitute(pair.second, substitutions);
        return result;
    });
    pairs = TPairedIUCollection<TPair>(entries.begin(), entries.end());
}

} // anonymous namespace

void IOperator::RenameUsedIUs(const TSubstitutions&) {}

void TOpMap::RenameUsedIUs(const TSubstitutions& substitutions) {
    // Every RHS reads the original input, including direct copies.
    // Replacing values keeps the map structure and its iterators.
    for (const auto& [id, element] : MapElements.Items()) {
        SetMapElementExpression(id, element.GetExpression().ApplyRenames(substitutions));
    }
}

void TOpFilter::RenameUsedIUs(const TSubstitutions& substitutions) {
    SetFilterExpression(FilterExpr.ApplyRenames(substitutions));
}

void TOpJoin::RenameUsedIUs(const TSubstitutions& substitutions) {
    Substitute(JoinKeys, substitutions);
    for (auto& filter : JoinFilters) {
        filter = filter.ApplyRenames(substitutions);
    }
    // CBO's shuffle keys name join inputs too.
    for (auto* shuffleBy : {&Props.LeftShuffleBy, &Props.RightShuffleBy}) {
        if (*shuffleBy) {
            Substitute(**shuffleBy, substitutions);
        }
    }
}

void TOpUnionAll::RenameUsedIUs(const TSubstitutions& substitutions) {
    for (const auto& [id, row] : Columns.Items()) {
        auto rebound = row;
        for (auto& input : rebound.Inputs) {
            input = Substitute(input, substitutions);
        }
        Columns.Replace(id, std::move(rebound));
    }
}

void TOpLimit::RenameUsedIUs(const TSubstitutions& substitutions) {
    LimitCond = LimitCond.ApplyRenames(substitutions);
    if (OffsetCond) {
        OffsetCond = OffsetCond->ApplyRenames(substitutions);
    }
}

void TOpSort::RenameUsedIUs(const TSubstitutions& substitutions) {
    Substitute(SortElements, substitutions);
    if (LimitCond) {
        LimitCond = LimitCond->ApplyRenames(substitutions);
    }
}

void TOpAggregate::RenameUsedIUs(const TSubstitutions& substitutions) {
    Substitute(KeyColumns, substitutions);
    for (const auto& [id, traits] : Aggregations.Items()) {
        auto rebound = traits;
        rebound.Input = Substitute(traits.Input, substitutions);
        Aggregations.Replace(id, std::move(rebound));
    }
    Props.OutputIUs.reset();
}

void TOpGroupingSets::RenameUsedIUs(const TSubstitutions& substitutions) {
    for (auto& keys : GroupingSets) {
        Substitute(keys, substitutions);
    }
    for (const auto& [output, input] : Columns.Items()) {
        Columns.Replace(output, Substitute(input, substitutions));
    }
    for (const auto& [output, input] : GroupingIndicators.Items()) {
        GroupingIndicators.Replace(output, Substitute(input, substitutions));
    }
}

void TOpWindow::RenameUsedIUs(const TSubstitutions& substitutions) {
    Substitute(PartitionKeys, substitutions);
    Substitute(SortElements, substitutions);
    for (const auto& [id, function] : WindowFuncs.Items()) {
        auto rebound = function;
        Substitute(rebound.Arguments, substitutions);
        WindowFuncs.Replace(id, std::move(rebound));
    }
}

void TOpAddDependencies::RenameUsedIUs(const TSubstitutions& substitutions) {
    RebindCaptures(substitutions);
}

void TOpDependentJoin::RenameUsedIUs(const TSubstitutions& substitutions) {
    Substitute(Dependencies, substitutions);
    TSubstitutions domainColumns;
    for (const auto& [parameter, column] : DomainColumns.Items()) {
        domainColumns.Add(Substitute(parameter, substitutions), Substitute(column, substitutions));
    }
    DomainColumns = std::move(domainColumns);
}

void TOpTableLookup::RenameUsedIUs(const TSubstitutions& substitutions) {
    Substitute(LookupKeys, substitutions);
    if (Prefix) {
        Substitute(Prefix->Equalities, substitutions);
    }
    Substitute(ResidualJoinKeys, substitutions);
    if (FetchedRowFilter) {
        FetchedRowFilter = FetchedRowFilter->ApplyRenames(substitutions);
    }
}

void TOpIndexLookupJoin::RenameUsedIUs(const TSubstitutions& substitutions) {
    Substitute(JoinKeys, substitutions);
}

// Substitution changes bindings, never the external field names.
void TOpTableEffect::RenameUsedIUs(const TSubstitutions& substitutions) {
    Substitute(Columns_, substitutions);
}

void TOpRoot::RenameUsedIUs(const TSubstitutions& substitutions) {
    Substitute(Columns_, substitutions);
}

} // namespace NKikimr::NKqp
