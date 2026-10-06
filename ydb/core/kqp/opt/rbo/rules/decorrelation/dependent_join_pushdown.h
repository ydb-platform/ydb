#pragma once

#include <ydb/core/kqp/opt/rbo/kqp_operator.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_context.h>

namespace NKikimr {
namespace NKqp {

// Creates a domain projection based on free variables from right side.
TIntrusivePtr<TOpAggregate> MakeDomainProjection(const TIntrusivePtr<IOperator>& input, const TUnorderedIUs& columns, TPositionHandle pos);
// Checks whether we have a free variables.
bool HasFreeCorrelation(const TIntrusivePtr<IOperator>& op, const TUnorderedIUs& correlatedColumns);
bool IsNullableIU(const TIntrusivePtr<IOperator>& input, TInfoUnitId iu, TExprContext& ctx);
// Here we want to support semantics where null == null.
TJoinIUs MakeNullSafeJoinKeys(TIntrusivePtr<IOperator>& leftInput, TIntrusivePtr<IOperator>& rightInput,
                              const TJoinIUs& joinKeys, TPositionHandle pos, TRBOContext& ctx,
                              TPlanProps& props);

// Caller IDs -> fresh domain IDs. The caller keeps the primary Replicate port.
struct TSubplanDomain {
    TIntrusivePtr<IOperator> Input;
    TPairedIUs Keys;

    // Capture definitions and body/result IDs stay fixed; only capture sources
    // change to the domain port. Nested scopes have globally distinct local IDs.
    TIntrusivePtr<IOperator> Bind(TIntrusivePtr<IOperator> body, TPositionHandle pos) &&;
};

TSubplanDomain MakeSubplanDomain(TIntrusivePtr<IOperator>& caller, const TUnorderedIUs& parameters,
    TPositionHandle pos, TPlanProps& props);
} // namespace NKqp
} // namespace NKikimr
