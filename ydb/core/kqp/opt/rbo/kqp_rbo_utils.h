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

bool HasOutputConflicts(const TVector<TInfoUnit>& outputIUs);
bool CanExposeOutput(IOperator* op, const TVector<TInfoUnit>& outputIUs, const TPlanProps& props);
bool CanExposeOutput(const TIntrusivePtr<IOperator>& op, const TVector<TInfoUnit>& outputIUs, const TPlanProps& props);
bool CanExposeToParents(IOperator* op, const TPlanProps& props);
bool CanReplaceInParents(
    const TIntrusivePtr<IOperator>& oldOp,
    const TIntrusivePtr<IOperator>& replacement,
    const TPlanProps& props);

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
