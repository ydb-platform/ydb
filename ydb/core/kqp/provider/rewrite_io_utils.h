#pragma once

#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>
#include <ydb/library/yql/ast/yql_expr.h>

namespace NYql {

TExprNode::TPtr RewriteReadFromView(
    const TExprNode::TPtr& node,
    TExprContext& ctx,
    const NSQLTranslation::TTranslationSettings& outerSettings,
    IModuleResolver::TPtr moduleResolver,
    const TViewPersistedData& viewData
);

}
