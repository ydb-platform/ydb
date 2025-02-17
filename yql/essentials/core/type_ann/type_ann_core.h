#pragma once

#include <yql/essentials/core/yql_data_provider.h>
#include <yql/essentials/core/yql_type_annotation.h>

#include <array>

namespace NYql {

IGraphTransformer::TStatus ValidateDataSource(const TExprNode::TPtr& node, TExprContext& ctx, const TTypeAnnotationContext& types);
IGraphTransformer::TStatus ValidateDataSink(const TExprNode::TPtr& node, TExprContext& ctx, const TTypeAnnotationContext& types);
IGraphTransformer::TStatus ValidateProviders(const TExprNode::TPtr& node, TExprNode::TPtr& output, TExprContext& ctx, const TTypeAnnotationContext& types);

TAutoPtr<IGraphTransformer> CreateIntentDeterminationTransformer(const TTypeAnnotationContext& types);

TAutoPtr<IGraphTransformer> CreateExtCallableTypeAnnotationTransformer(TTypeAnnotationContext& types,
    bool instantOnly = false);

const THashSet<TString>& GetBuiltinFunctions();

} // namespace NYql
