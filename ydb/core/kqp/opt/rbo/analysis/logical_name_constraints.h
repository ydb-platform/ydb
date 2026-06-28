#pragma once

#include <ydb/core/kqp/opt/rbo/kqp_plan_props.h>

namespace NKikimr {
namespace NKqp {

class IOperator;
class TOpRoot;

void ComputePlanNameConstraints(TOpRoot& root);

const TInfoUnitConstraintSet& GetForbidden(IOperator* op);
bool ContainsForbidden(const TVector<TInfoUnit>& output, const TInfoUnitConstraintSet& forbidden);

} // namespace NKqp
} // namespace NKikimr
