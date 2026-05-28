#pragma once

#include "kqp_info_unit.h"
#include "kqp_rbo_context.h"
#include "kqp_plan_props.h"

namespace NKikimr {
namespace NKqp {

using namespace NYql;

class IOperator;

bool IsGeneratedIgnoreIU(const TInfoUnit& iu);
TVector<TInfoUnit> GetSubplanResultIUs(const TIntrusivePtr<IOperator>& op);

TVector<TInfoUnit> IUSetDiff(TVector<TInfoUnit> left, TVector<TInfoUnit> right);
TVector<TInfoUnit> IUSetIntersect(TVector<TInfoUnit> left, TVector<TInfoUnit> right);
template <class T> void AddUnique(TVector<T>& toAdd, TVector<T>& target) {
    for (const auto & e : toAdd) {
        if (std::find(target.begin(), target.end(), e) == target.end()) {
            target.push_back(e);
        }
    }
}
}
}
