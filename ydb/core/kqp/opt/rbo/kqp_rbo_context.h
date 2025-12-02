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
    TRBOContext(TKqpOptimizeContext &kqpCtx, 
        NYql::TExprContext &ctx, 
        NYql::TTypeAnnotationContext &typeCtx, 
        TAutoPtr<NYql::IGraphTransformer> typeAnnTransformer,
        const NMiniKQL::IFunctionRegistry& funcRegistry) : 
        KqpCtx(kqpCtx),
        ExprCtx(ctx), 
        TypeCtx(typeCtx), 
        TypeAnnTransformer(typeAnnTransformer),
        FuncRegistry(funcRegistry),
        CBOCtx(TKqpProviderContext(kqpCtx, kqpCtx.Config->CostBasedOptimizationLevel.Get().GetOrElse(kqpCtx.Config->DefaultCostBasedOptimizationLevel))) 
        {}

    TKqpOptimizeContext & KqpCtx;
    NYql::TExprContext & ExprCtx;
    NYql::TTypeAnnotationContext & TypeCtx;
    TAutoPtr<NYql::IGraphTransformer> TypeAnnTransformer;
    const NMiniKQL::IFunctionRegistry& FuncRegistry;
    TKqpProviderContext CBOCtx;
};

}
}