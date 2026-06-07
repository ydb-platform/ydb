#pragma once

#include "kqp_info_unit.h"
#include "kqp_rbo_context.h"
#include "kqp_plan_props.h"

#include <algorithm>

namespace NKikimr {
namespace NKqp {

using namespace NYql;

class IOperator;

const TInfoUnitSet& EmptyInfoUnitSet();
bool ContainsInfoUnit(const TVector<TInfoUnit>& units, const TInfoUnit& unit);
bool AddInfoUnit(TInfoUnitSet& target, const TInfoUnit& iu);
bool AddInfoUnits(TInfoUnitSet& target, const TVector<TInfoUnit>& ius);
bool AddInfoUnits(TInfoUnitSet& target, const TInfoUnitSet& ius);
TInfoUnitSet MakeInfoUnitSet(const TVector<TInfoUnit>& ius);

bool IsGeneratedIgnoreIU(const TInfoUnit& iu);
TInfoUnit MakeGeneratedIgnoreIU(TPlanProps& props);
TVector<TInfoUnit> GetSubplanResultIUs(const TIntrusivePtr<IOperator>& op);

TVector<TInfoUnit> IUSetDiff(TVector<TInfoUnit> left, TVector<TInfoUnit> right);
TVector<TInfoUnit> IUSetIntersect(TVector<TInfoUnit> left, TVector<TInfoUnit> right);
bool IUIsSubset(TVector<TInfoUnit> left, TVector<TInfoUnit> right);

template <class T> void AddUnique(TVector<T>& toAdd, TVector<T>& target) {
    for (const auto & e : toAdd) {
        if (std::find(target.begin(), target.end(), e) == target.end()) {
            target.push_back(e);
        }
    }
}
}
}
