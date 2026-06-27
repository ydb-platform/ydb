#pragma once

#include "kqp_simple_operator.h"
#include "kqp_info_unit.h"
#include "kqp_stage_graph.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt.h>

#include <utility>

namespace NKikimr {
namespace NKqp {

using namespace NYql;

class IOperator;

enum ESubplanType : ui32 { EXPR, IN_SUBPLAN, EXISTS };

struct TPlanProps;

struct TSubplanEntry {
    TIntrusivePtr<ISimpleOperator> Plan;
    TVector<TInfoUnit> Tuple;
    ESubplanType Type;
    TInfoUnit IU;
    TVector<TInfoUnit> DependentIUs;
};

struct TSubplans {

    void Add(const TInfoUnit& iu, const TSubplanEntry& entry) {
        OrderedList.push_back(iu);
        PlanMap.insert({iu, entry});
    }

    void Replace(const TInfoUnit& iu, TIntrusivePtr<ISimpleOperator> op) {
        auto entry = PlanMap.at(iu);
        entry.Plan = op;
        PlanMap.erase(iu);
        PlanMap.insert({iu, entry});
    }

    TVector<TSubplanEntry> Get() {
        TVector<TSubplanEntry> result;
        for (const auto& iu : OrderedList) {
            result.push_back(PlanMap.at(iu));
        }
        return result;
    }

    void Remove(const TInfoUnit& iu) {
        std::erase(OrderedList, iu);
        PlanMap.erase(iu);
    }

    bool RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx);

    THashMap<TInfoUnit, TSubplanEntry, TInfoUnit::THashFunction> PlanMap;
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

    bool Add(const TInfoUnit& iu);
    bool Add(const TInfoUnitSet& ius);
    bool Add(const TInfoUnitConstraintSet& other);
    bool Remove(const TInfoUnit& iu);
    bool Remove(const TInfoUnitSet& ius);
    bool Intersect(const TInfoUnitConstraintSet& other);
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
};

}
}
