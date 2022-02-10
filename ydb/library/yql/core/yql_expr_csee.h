#pragma once

#include "yql_graph_transformer.h"
#include "yql_type_annotation.h" 

namespace NYql {

IGraphTransformer::TStatus EliminateCommonSubExpressions(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx, 
    bool forSubGraph, const TColumnOrderStorage& coStore); 
IGraphTransformer::TStatus UpdateCompletness(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx);

// Calculate order between two given nodes. Must be used only after CSEE pass or UpdateCompletness.
// 0 may not mean equality of nodes because we cannot distinguish order of external arguments in some cases.
int CompareNodes(const TExprNode& left, const TExprNode& right);

}
