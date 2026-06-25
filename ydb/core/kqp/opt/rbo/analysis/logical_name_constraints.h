#pragma once

#include <ydb/core/kqp/opt/rbo/kqp_plan_props.h>

namespace NKikimr {
namespace NKqp {

class IOperator;
class TOpRoot;

void ComputePlanNameConstraints(TOpRoot& root);

const TInfoUnitSet& GetForbidden(const TPlanProps& props, IOperator* from, IOperator* to);
TInfoUnitSet GetForbidden(const TPlanProps& props, IOperator* op);

} // namespace NKqp
} // namespace NKikimr
