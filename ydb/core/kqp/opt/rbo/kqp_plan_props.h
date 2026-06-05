#pragma once

#include "kqp_simple_operator.h"
#include "kqp_info_unit.h"
#include "kqp_stage_graph.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt.h>

#include <cstdint>

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

struct TPlanEdgeKey {
    IOperator* Parent = nullptr;
    ui32 ChildIdx = 0;
    IOperator* Child = nullptr;

    bool operator==(const TPlanEdgeKey& other) const {
        return Parent == other.Parent && ChildIdx == other.ChildIdx && Child == other.Child;
    }

    struct THashFunction {
        size_t operator()(const TPlanEdgeKey& key) const {
            size_t hash = reinterpret_cast<std::uintptr_t>(key.Parent);
            hash ^= reinterpret_cast<std::uintptr_t>(key.Child) + 0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);
            hash ^= static_cast<size_t>(key.ChildIdx) + 0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);
            return hash;
        }
    };
};

struct TPlanNameConstraints {
    void Clear();

    bool AddForbiddenOut(IOperator* parent, ui32 childIdx, IOperator* child, const TInfoUnit& iu);
    bool AddForbiddenOut(IOperator* parent, ui32 childIdx, IOperator* child, const TInfoUnitSet& ius);

    const TInfoUnitSet& GetForbiddenOut(IOperator* parent, ui32 childIdx, IOperator* child) const;
    const TInfoUnitSet& GetForbiddenOut(IOperator* parent, ui32 childIdx) const;
    const TInfoUnitSet& GetForbiddenOutForSingleConsumer(IOperator* op) const;
    bool IsForbiddenAtOutput(IOperator* op, const TInfoUnit& iu) const;

    THashMap<TPlanEdgeKey, TInfoUnitSet, TPlanEdgeKey::THashFunction> ForbiddenOut;
};

struct TAliasCandidate {
    TInfoUnit IU;
    i32 Priority = 0;
};

struct TPlanAliases {
    using TCandidates = TVector<TAliasCandidate>;
    using TAliasMap = THashMap<TInfoUnit, TCandidates, TInfoUnit::THashFunction>;

    void Clear() {
        AliasesAtOutput.clear();
    }

    const TCandidates* GetAliases(IOperator* op, const TInfoUnit& iu) const {
        const auto opIt = AliasesAtOutput.find(op);
        if (opIt == AliasesAtOutput.end()) {
            return nullptr;
        }

        const auto aliasIt = opIt->second.find(iu);
        return aliasIt == opIt->second.end() ? nullptr : &aliasIt->second;
    }

    THashMap<IOperator*, TAliasMap> AliasesAtOutput;
};

/**
 * Global plan properties
 */
struct TPlanProps {
    TStageGraph StageGraph;
    THashMap<IOperator*, TInfoUnitSet> LiveOut;
    TPlanNameConstraints NameConstraints;
    TPlanAliases Aliases;
    int InternalVarIdx = 1;
    TSubplans Subplans;
    bool PgSyntax = false;
};

}
}
