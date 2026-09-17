#pragma once

#include <ydb/core/kqp/opt/rbo/kqp_operator.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_context.h>

namespace NKikimr {
namespace NKqp {

// Creates a domain projection based on free variables from right side.
TIntrusivePtr<TOpAggregate> MakeDomainProjection(const TIntrusivePtr<IOperator>& input, const TVector<TInfoUnit>& columns, TPositionHandle pos);
// Checks whether we have a free variables.
bool HasFreeCorrelation(const TIntrusivePtr<IOperator>& op, const TVector<TInfoUnit>& correlatedColumns);
bool IsNullableIU(const TIntrusivePtr<IOperator>& input, const TInfoUnit& iu);
// Here we want to support semantics where null == null.
TVector<std::pair<TInfoUnit, TInfoUnit>> MakeNullSafeJoinKeys(TIntrusivePtr<IOperator>& leftInput, TIntrusivePtr<IOperator>& rightInput,
                                                              const TVector<std::pair<TInfoUnit, TInfoUnit>>& joinKeys, TPositionHandle pos, TRBOContext& ctx,
                                                              TPlanProps& props, TInfoUnitSet& usedIUs);
} // namespace NKqp
} // namespace NKikimr
