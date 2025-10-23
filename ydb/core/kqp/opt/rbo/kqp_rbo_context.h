#pragma once

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt.h>
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/type_ann/type_ann_core.h>

namespace NKikimr {
namespace NKqp {

using namespace NOpt;

class TRBOContext {
public:
    TRBOContext(const TKqpOptimizeContext &kqpCtx, NYql::TExprContext &ctx, NYql::TTypeAnnotationContext &typeCtx, TAutoPtr<NYql::IGraphTransformer> typeAnnTransformer) : KqpCtx(kqpCtx),
        ExprCtx(ctx), TypeCtx(typeCtx), TypeAnnTransformer(typeAnnTransformer) {}

    const TKqpOptimizeContext & KqpCtx;
    NYql::TExprContext & ExprCtx;
    NYql::TTypeAnnotationContext & TypeCtx;
    TAutoPtr<NYql::IGraphTransformer> TypeAnnTransformer;
};

}
}