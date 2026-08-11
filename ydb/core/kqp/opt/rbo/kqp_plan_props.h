#pragma once

#include "kqp_simple_operator.h"
#include "kqp_info_unit.h"
#include "kqp_stage_graph.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt.h>

#include <algorithm>
#include <optional>
#include <utility>

namespace NKikimr {
namespace NKqp {

using namespace NYql;

class IOperator;

enum ESubplanType : ui32 { EXPR, IN_SUBPLAN, EXISTS };

struct TPlanProps;

struct TSubplanEntry {
    TSubplanEntry(TIntrusivePtr<ISimpleOperator> plan, ESubplanType type, TVector<TInfoUnit> tuple, TInfoUnit iu)
        : Plan(std::move(plan))
        , Tuple(std::move(tuple))
        , Type(type)
        , IU(std::move(iu)) {
    }

    TIntrusivePtr<ISimpleOperator> Plan;
    TVector<TInfoUnit> Tuple;
    ESubplanType Type;
    TInfoUnit IU;
    TVector<TInfoUnit> DependentIUs;
};

class TSubplans {
public:
    void Add(const TInfoUnit& binding, TIntrusivePtr<ISimpleOperator> plan, ESubplanType type, TVector<TInfoUnit> tuple = {}) {
        Y_ENSURE(plan, "Cannot register a null subplan for " << binding.GetFullName());
        const bool inserted = Entries.emplace(
            binding,
            TSubplanEntry(std::move(plan), type, std::move(tuple), binding)).second;
        Y_ENSURE(inserted, "Duplicate subplan binding " << binding.GetFullName());
        OrderedList.push_back(binding);
    }

    void ReplacePlan(const TInfoUnit& binding, TIntrusivePtr<ISimpleOperator> plan) {
        Y_ENSURE(plan, "Cannot replace " << binding.GetFullName() << " with a null subplan");
        Entries.at(binding).Plan = std::move(plan);
    }

    void AddDependentIU(const TInfoUnit& binding, const TInfoUnit& iu) {
        auto& dependentIUs = Entries.at(binding).DependentIUs;
        if (std::find(dependentIUs.begin(), dependentIUs.end(), iu) == dependentIUs.end()) {
            dependentIUs.push_back(iu);
        }
    }

    TVector<TSubplanEntry> Get() {
        TVector<TSubplanEntry> result;
        for (const auto& iu : OrderedList) {
            result.push_back(Entries.at(iu));
        }
        return result;
    }

    void Remove(const TInfoUnit& binding) {
        const auto entryIt = Entries.find(binding);
        Y_ENSURE(entryIt != Entries.end(), "Unknown subplan binding " << binding.GetFullName());
        std::erase(OrderedList, binding);
        Entries.erase(entryIt);
    }

    const TSubplanEntry* Find(const TInfoUnit& binding) const {
        const auto it = Entries.find(binding);
        return it == Entries.end() ? nullptr : &it->second;
    }

    const TSubplanEntry& At(const TInfoUnit& binding) const {
        return Entries.at(binding);
    }

    bool Empty() const {
        return Entries.empty();
    }

    auto begin() const {
        return Entries.begin();
    }

    auto end() const {
        return Entries.end();
    }

    bool RenameReferences(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx);

private:
    THashMap<TInfoUnit, TSubplanEntry, TInfoUnit::THashFunction> Entries;
    TVector<TInfoUnit> OrderedList;
};

class TInfoUnitConstraintSet {
public:
    // A name constraint can be finite, or all names except a finite exception set.
    static TInfoUnitConstraintSet AllExcept(TInfoUnitSet except) {
        TInfoUnitConstraintSet result;
        result.AllExcept_ = true;
        result.Units_ = std::move(except);
        return result;
    }

    bool Empty() const {
        return !AllExcept_ && Units_.empty();
    }

    bool IsAllExcept() const {
        return AllExcept_;
    }

    bool contains(const TInfoUnit& iu) const {
        return AllExcept_ ? !Units_.contains(iu) : Units_.contains(iu);
    }

    const TInfoUnitSet& GetUnits() const {
        return Units_;
    }

    bool UnionWith(const TInfoUnit& iu);
    bool UnionWith(const TInfoUnitSet& ius);
    bool UnionWith(const TInfoUnitConstraintSet& other);
    bool Subtract(const TInfoUnit& iu);
    bool Subtract(const TInfoUnitSet& ius);
    bool IntersectWith(const TInfoUnitConstraintSet& other);
    TInfoUnitConstraintSet Complement() const;

private:
    bool AllExcept_ = false;
    TInfoUnitSet Units_;
};

struct TPlanNameConstraints {
    void Clear();

    bool AddForbidden(const TInfoUnitConstraintSet& forbidden);

    const TInfoUnitConstraintSet& GetForbidden() const;

    TInfoUnitConstraintSet Forbidden;
};

struct TAliasCandidate {
    TInfoUnit IU;
    i32 Priority = 0;
};

// Names required to exist by a contract that alias rewriting must not touch.
// Hard: the root output contract; these names can never be renamed away.
// Soft: produced-name contracts inside the plan (aggregate keys, UnionAll
//       columns) that only their dedicated push rules may rename.
// Recomputed together with plan aliases; valid only while aliases are.
struct TPinnedNames {
    TInfoUnitSet Hard;
    TInfoUnitSet Soft;
};

struct TPlanAliases {
    using TCandidates = TVector<TAliasCandidate>;
    using TAliasMap = THashMap<TInfoUnit, TCandidates, TInfoUnit::THashFunction>;
};

/**
 * Global plan properties
 */
struct TPlanProps {
    TStageGraph StageGraph;
    int InternalVarIdx = 1;
    TSubplans Subplans;
    bool PgSyntax = false;
    std::optional<TPinnedNames> PinnedNames;
};

}
}
