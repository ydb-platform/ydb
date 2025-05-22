#pragma once

#include "type_ann_core.h"

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/ast/yql_expr_types.h>
#include <yql/essentials/core/yql_expr_optimize.h>

namespace NYql {

enum class ETypeCheckMode {
    Single,
    Initial,
    Repeat
};

TAutoPtr<IGraphTransformer> CreateTypeAnnotationTransformer(
    TAutoPtr<IGraphTransformer> callableTransformer, TTypeAnnotationContext& types,
    ETypeCheckMode mode = ETypeCheckMode::Single);

TAutoPtr<IGraphTransformer> CreateFullTypeAnnotationTransformer(bool instant, bool wholeProgram, TTypeAnnotationContext& types);

bool SyncAnnotateTypes(
        TExprNode::TPtr& root, TExprContext& ctx, bool wholeProgram,
        TTypeAnnotationContext& typeAnnotationContext);

bool InstantAnnotateTypes(
        TExprNode::TPtr& root, TExprContext& ctx, bool wholeProgram,
        TTypeAnnotationContext& typeAnnotationContext);

TExprNode::TPtr ParseAndAnnotate(
        const TStringBuf& str,
        TExprContext& exprCtx, bool instant, bool wholeProgram,
        TTypeAnnotationContext& typeAnnotationContext);

void CheckFatalTypeError(IGraphTransformer::TStatus status);

}
