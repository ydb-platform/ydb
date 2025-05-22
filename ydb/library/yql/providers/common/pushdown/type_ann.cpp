#include "predicate_node.h"

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>

namespace NYql::NPushdown {

IGraphTransformer::TStatus AnnotateFilterPredicate(const TExprNode::TPtr& input, size_t childIndex, const TStructExprType* itemType, TExprContext& ctx) {
    if (childIndex >= input->ChildrenSize()) {
        return IGraphTransformer::TStatus::Error;
    }

    auto& filterLambda = input->ChildRef(childIndex);
    if (!EnsureLambda(*filterLambda, ctx)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!UpdateLambdaAllArgumentsTypes(filterLambda, {itemType}, ctx)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (const auto* filterLambdaType = filterLambda->GetTypeAnn()) {
        if (filterLambdaType->GetKind() != ETypeAnnotationKind::Data) {
            return IGraphTransformer::TStatus::Error;
        }
        const TDataExprType* dataExprType = static_cast<const TDataExprType*>(filterLambdaType);
        if (dataExprType->GetSlot() != EDataSlot::Bool) {
            return IGraphTransformer::TStatus::Error;
        }
    } else {
        return IGraphTransformer::TStatus::Repeat;
    }
    return IGraphTransformer::TStatus::Ok;
}

} // namespace NYql::NPushdown
