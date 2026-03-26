#pragma once

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt.h>
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/type_ann/type_ann_core.h>
#include <ydb/core/kqp/opt/logical/kqp_opt_cbo.h>

namespace NKikimr {
namespace NKqp {

using namespace NOpt;

class TRBOContext {
public:
    TRBOContext(TKqpOptimizeContext& kqpCtx, NYql::TExprContext& ctx, NYql::TTypeAnnotationContext& typeCtx, NYql::IGraphTransformer& typeAnnTransformer,
                NYql::IGraphTransformer& peepholeTypeAnnTransformer, const NMiniKQL::IFunctionRegistry& funcRegistry)
        : KqpCtx(kqpCtx)
        , ExprCtx(ctx)
        , TypeCtx(typeCtx)
        , TypeAnnTransformer(typeAnnTransformer)
        , PeepholeTypeAnnTransformer(peepholeTypeAnnTransformer)
        , FuncRegistry(funcRegistry)
        , CBOCtx(
              TKqpProviderContext(kqpCtx, kqpCtx.Config->CostBasedOptimizationLevel.Get().GetOrElse(kqpCtx.Config->GetDefaultCostBasedOptimizationLevel()))) {
    }

    TKqpOptimizeContext& KqpCtx;
    NYql::TExprContext& ExprCtx;
    NYql::TTypeAnnotationContext& TypeCtx;
    NYql::IGraphTransformer& TypeAnnTransformer;
    NYql::IGraphTransformer& PeepholeTypeAnnTransformer;
    const NMiniKQL::IFunctionRegistry& FuncRegistry;
    TKqpProviderContext CBOCtx;
};

} // namespace NKqp
}