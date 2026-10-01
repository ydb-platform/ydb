#pragma once

#include "kqp_info_unit.h"
#include "kqp_stage_graph.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt.h>

#include <algorithm>
#include <optional>
#include <memory>
#include <utility>

namespace NKikimr {
namespace NKqp {

using namespace NYql;

class IOperator;

enum ESubplanType : ui32 { EXPR, IN_SUBPLAN, EXISTS };

struct TPlanProps;

struct TSubplanEntry {
    TSubplanEntry(TIntrusivePtr<IOperator> plan, ESubplanType type, TOrderedIUs<> tuple,
        std::optional<TInfoUnitId> resultIU);
    ~TSubplanEntry();
    TSubplanEntry(TSubplanEntry&&) noexcept;
    TSubplanEntry& operator=(TSubplanEntry&&) noexcept;

    TIntrusivePtr<IOperator> Plan;
    TOrderedIUs<> Tuple;
    // The producer's scalar/IN result, not the outer binding of this subplan.
    // EXISTS needs no result. Captures and sort keys are not result columns.
    std::optional<TInfoUnitId> ResultIU;
    ESubplanType Type;
    TUnorderedIUs DependentIUs; // Caller-scope IDs, not the subplan's local captures.
};

class TSubplans {
public:
    // A binding is the stable identity of a registry entry. Replacing the
    // referenced plan preserves that identity; changing it requires rewriting
    // every expression that names it and is intentionally unsupported here.
    void Add(TInfoUnitId binding, TIntrusivePtr<IOperator> plan, ESubplanType type,
        TOrderedIUs<> tuple = {}, std::optional<TInfoUnitId> resultIU = std::nullopt);

    // Borrow the owning slot for an in-place rewrite, keeping the graph attached.
    TIntrusivePtr<IOperator>& MutablePlan(TInfoUnitId binding) Y_LIFETIME_BOUND {
        return Entries.At(binding)->Plan;
    }
    void ReplacePlan(TInfoUnitId binding, TIntrusivePtr<IOperator> plan);

    void RefreshDependencies(TInfoUnitId binding);
    // Simultaneous substitution in this call's parameters and IN tuple only.
    // Local capture/result IDs and the subplan's expressions do not change.
    bool RebindInputs(TInfoUnitId binding, const TSubstitutions& substitutions);

    // A global rewrite may remove the producer's result copy. The call binding
    // and external result contract are unchanged; EXISTS still has no result.
    void RebindResults(const TSubstitutions& substitutions);
    // Scoped rewrite at this entry's producer boundary; the call ID stays fixed.
    void RebindResult(TInfoUnitId binding, const TSubstitutions& substitutions);

    void Remove(TInfoUnitId binding) {
        Y_ENSURE(Entries.Remove(binding), "Unknown subplan binding " << binding);
    }

    const TSubplanEntry* Find(TInfoUnitId binding) const Y_LIFETIME_BOUND {
        const auto* entry = Entries.Find(binding);
        return entry ? entry->get() : nullptr;
    }

    const TSubplanEntry& At(TInfoUnitId binding) const Y_LIFETIME_BOUND {
        return *Entries.At(binding);
    }

    bool Contains(TInfoUnitId binding) const {
        return Entries.Keys().Contains(binding);
    }

    bool Empty() const {
        return Entries.Keys().Empty();
    }

    // Every registered call binding.
    const TUnorderedIUs& Bindings() const Y_LIFETIME_BOUND {
        return Entries.Keys();
    }

    // The calls among `ids`.
    TUnorderedIUs CallsIn(const TUnorderedIUs& ids) const {
        auto calls = ids;
        calls.IntersectWith(Bindings());
        return calls;
    }

    // Entries in binding order.
    auto begin() const {
        return TEntryIterator{Entries.Items().begin()};
    }

    auto end() const {
        return TEntryIterator{Entries.Items().end()};
    }

    // Rebind callers after eliminating a copy; never collapse a capture boundary.
    bool RenameExternalReferences(const TSubstitutions& substitutions);

private:
    using TEntries = TMappedIUs<std::unique_ptr<TSubplanEntry>>;

    // Presents (binding, entry) pairs over the owning pointers.
    struct TEntryIterator {
        using TBase = TEntries::TMap::const_iterator;
        using value_type = std::pair<TInfoUnitId, const TSubplanEntry&>;

        value_type operator*() const {
            return {It->first, *It->second};
        }
        TEntryIterator& operator++() {
            ++It;
            return *this;
        }
        bool operator==(const TEntryIterator&) const = default;

        TBase It;
    };

    // Borrowed plan slots and entries must survive other insertions and
    // removals, so each entry is allocated separately from the index.
    TEntries Entries;
};

// Where a binding's values come from, for column statistics and hint aliases.
struct TColumnLineageEntry {
    TString SourceAlias;
    TString TableName;
    TString ColumnName;
    // Distinct for every Read, Aggregate and Replicate port that introduces a
    // relation instance; separates the two sides of a self-join.
    ui32 Relation = 0;

    TString GetRawAlias() const {
        return SourceAlias.empty() ? TableName : SourceAlias;
    }
};

// An ID has one definition, so it has one lineage. The metadata pass rebuilds
// the table: every defining operator adds its own IDs, exactly once.
class TColumnLineage {
public:
    void Clear() {
        Entries_.Clear();
        Relations_.clear();
    }

    ui32 AddRelation(TString sourceAlias, TString tableName = {}) {
        const auto id = Relations_.size();
        Relations_.push_back({std::move(sourceAlias), std::move(tableName)});
        return id;
    }

    ui32 CopyRelation(ui32 relation) {
        const auto source = Relations_.at(relation);
        return AddRelation(source.SourceAlias, source.TableName);
    }

    void Add(TInfoUnitId id, TColumnLineageEntry entry) {
        Entries_.Add(id, std::move(entry));
    }

    const TColumnLineageEntry* Find(TInfoUnitId id) const Y_LIFETIME_BOUND {
        return Entries_.Find(id);
    }

    // Hint aliases of the operator-local relations, sorted. Instances
    // of one source alias are numbered in relation order: t, t_#1, ...
    TVector<TString> GetAliases(const TMappedIUs<ui32>& relations) const;

private:
    TMappedIUs<TColumnLineageEntry> Entries_;
    struct TRelation {
        TString SourceAlias;
        TString TableName;
    };
    TVector<TRelation> Relations_;
};

/**
 * Global plan properties
 */
struct TPlanProps {
    TInfoUnitRegistry InfoUnitRegistry;
    TStageGraph StageGraph;
    TSubplans Subplans;
    TColumnLineage ColumnLineage;
    bool PgSyntax = false;
    bool WithEffects = false;
    bool WithReturning = false;
};

}
}
