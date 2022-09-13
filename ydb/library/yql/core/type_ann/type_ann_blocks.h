#pragma once

#include "type_ann_impl.h"

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/ast/yql_expr_types.h>

namespace NYql {
namespace NTypeAnnImpl {

    IGraphTransformer::TStatus AsScalarWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus BlockAddWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);

} // namespace NTypeAnnImpl
} // namespace NYql
