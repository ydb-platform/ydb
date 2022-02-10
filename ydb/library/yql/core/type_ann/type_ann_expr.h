#pragma once

#include "type_ann_core.h"

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/ast/yql_expr_types.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>

namespace NYql {

TAutoPtr<IGraphTransformer> CreateTypeAnnotationTransformer(
        TAutoPtr<IGraphTransformer> callableTransformer, TTypeAnnotationContext& types);

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

}
