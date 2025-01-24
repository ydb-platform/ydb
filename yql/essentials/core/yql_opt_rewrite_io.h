#pragma once

#include "yql_data_provider.h"
#include "yql_type_annotation.h"

#include <yql/essentials/ast/yql_expr.h>

namespace NYql {

IGraphTransformer::TStatus RewriteIO(const TExprNode::TPtr& input, TExprNode::TPtr& output, const TTypeAnnotationContext& types, TExprContext& ctx);

}
