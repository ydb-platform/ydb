#pragma once

#include "dq_opt.h"

#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/yql_expr_optimize.h>

namespace NYql::NDq {

IGraphTransformer::TStatus DqReplicateStageMultiOutput(TExprNode::TPtr input, TExprNode::TPtr& output,
    TExprContext& ctx);

IGraphTransformer::TStatus DqExtractPrecomputeToStageInput(const TExprNode::TPtr& input, TExprNode::TPtr& output,
    TExprContext& ctx);

} // namespace NYql::NDq
