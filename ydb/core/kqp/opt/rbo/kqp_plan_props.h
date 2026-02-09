#pragma once

#include "kqp_info_unit.h"
#include "kqp_stage_graph.h"
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;

class IOperator;

enum ESubplanType : ui32 { EXPR, IN_SUBPLAN, EXISTS };

struct TSubplanEntry {
    std::shared_ptr<IOperator> Plan;
    TVector<TInfoUnit> Tuple;
    ESubplanType Type;
    TInfoUnit IU;
};

struct TSubplans {

    void Add(TInfoUnit iu, TSubplanEntry entry) {
        OrderedList.push_back(iu);
        PlanMap.insert({iu, entry});
    }

    void Replace(TInfoUnit iu, std::shared_ptr<IOperator> op) {
        auto entry = PlanMap.at(iu);
        entry.Plan = op;
        PlanMap.erase(iu);
        PlanMap.insert({iu, entry});
    }

    TVector<TSubplanEntry> Get() {
        TVector<TSubplanEntry> result;
        for (auto iu : OrderedList) {
            result.push_back(PlanMap.at(iu));
        }
        return result;
    }

    void Remove(TInfoUnit iu) {
        std::erase(OrderedList, iu);
        PlanMap.erase(iu);
    }

    THashMap<TInfoUnit, TSubplanEntry, TInfoUnit::THashFunction> PlanMap;
    TVector<TInfoUnit> OrderedList;
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