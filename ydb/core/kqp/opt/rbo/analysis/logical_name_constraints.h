#pragma once

#include <ydb/core/kqp/opt/rbo/kqp_plan_props.h>

namespace NKikimr {
namespace NKqp {

class IOperator;
class TOpRoot;

bool HasOutputConflicts(const TVector<TInfoUnit>& outputIUs);
bool CanExposeOutput(IOperator* op, const TVector<TInfoUnit>& outputIUs, const TPlanProps& props);
bool CanExposeOutput(const TIntrusivePtr<IOperator>& op, const TVector<TInfoUnit>& outputIUs, const TPlanProps& props);
bool CanExposeToParents(IOperator* op, const TPlanProps& props);
bool CanReplaceInParents(
    const TIntrusivePtr<IOperator>& oldOp,
    const TIntrusivePtr<IOperator>& replacement,
    const TPlanProps& props);

void ComputePlanNameConstraints(TOpRoot& root);

} // namespace NKqp
} // namespace NKikimr
