#pragma once

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt.h>
#include <yql/essentials/ast/yql_expr.h>

namespace NKikimr {
namespace NKqp {

using namespace NOpt;

class TRBOContext {
public:
    TRBOContext(const TKqpOptimizeContext &kqpCtx, NYql::TExprContext &ctx, NYql::TTypeAnnotationContext &typeCtx) : KqpCtx(kqpCtx),
        ExprCtx(ctx), TypeCtx(typeCtx) {}

    const TKqpOptimizeContext & KqpCtx;
    NYql::TExprContext & ExprCtx;
    NYql::TTypeAnnotationContext & TypeCtx;
};

}
}