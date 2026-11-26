#pragma once

#include "type_ann_impl.h"

namespace NYql::NTypeAnnImpl {

struct TInput;

TExprNode::TPtr GetYqlFlattenSetting(const TExprNode& from);

const TStructExprType* GetYqlFlattenRowType(const TExprNode& setting);

TMaybe<IGraphTransformer::TStatus> AnnotateYqlFromFlatten(
    const TExprNode::TPtr& input,
    TExprNode::TPtr& output,
    const TExprNode& fromOption,
    const TExprNode& source,
    TInput& sourceType,
    TExtContext& ctx);

} // namespace NYql::NTypeAnnImpl
