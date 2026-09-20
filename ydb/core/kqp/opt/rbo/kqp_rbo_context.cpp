#include "kqp_rbo_context.h"

#include <ydb/core/kqp/provider/yql_kikimr_settings.h>

namespace NKikimr::NKqp {

TRBOContext::TRBOContext(NOpt::TKqpOptimizeContext& kqpCtx, NYql::TExprContext& ctx, NYql::TTypeAnnotationContext& typeCtx,
    NYql::IGraphTransformer& typeAnnTransformer, const NMiniKQL::IFunctionRegistry& funcRegistry)
    : KqpCtx(kqpCtx)
    , ExprCtx(ctx)
    , TypeCtx(typeCtx)
    , TypeAnnTransformer(typeAnnTransformer)
    , FuncRegistry(funcRegistry)
    , CBOCtx(kqpCtx, 
        kqpCtx.Config->CostBasedOptimizationLevel.Get().GetOrElse(kqpCtx.Config->GetDefaultCostBasedOptimizationLevel()), 
        kqpCtx.Config->UseBlockHashJoin.Get().GetOrElse(false))
    , ExecutionJson(std::nullopt)
    , ExplainJson(std::nullopt)
{}

} // namespace NKikimr::NKqp
