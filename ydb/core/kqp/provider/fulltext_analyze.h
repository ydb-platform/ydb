#pragma once

#include "yql_kikimr_provider.h"

namespace NYql {

TExprNode::TPtr BuildFulltextAnalyze(const TKikimrTableDescription& table, const TExprNode::TPtr& inputRow,
    const TIndexDescription& index, TPositionHandle pos, TExprContext& ctx);

} // namespace NYql
