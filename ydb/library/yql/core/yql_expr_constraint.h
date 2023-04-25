#pragma once

#include <ydb/library/yql/core/yql_type_annotation.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/ast/yql_expr.h>

#include <util/generic/ptr.h>

#include <initializer_list>

namespace NYql {

TAutoPtr<IGraphTransformer> CreateConstraintTransformer(TTypeAnnotationContext& types, bool instantOnly = false, bool subGraph = false, bool disableCheck = false);
TAutoPtr<IGraphTransformer> CreateDefCallableConstraintTransformer();

IGraphTransformer::TStatus UpdateLambdaConstraints(const TExprNode& lambda);
IGraphTransformer::TStatus UpdateLambdaConstraints(TExprNode::TPtr& lambda, TExprContext& ctx, const TArrayRef<const TConstraintNode::TListType>& constraints);
IGraphTransformer::TStatus UpdateAllChildLambdasConstraints(const TExprNode& node);

}
