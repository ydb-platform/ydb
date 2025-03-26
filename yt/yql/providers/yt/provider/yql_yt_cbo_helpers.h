#pragma once

#include "yql_yt_join_impl.h"
#include "yql_yt_provider_context.h"

namespace NYql {

IGraphTransformer::TStatus CollectCboStats(TYtJoinNodeOp& op, const TYtState::TPtr& state, TExprContext& ctx);

IGraphTransformer::TStatus PopulateJoinStrategySizeInfo(TRelSizeInfo& outLeft, TRelSizeInfo& outRight, const TYtState::TPtr& state, TExprContext& ctx, TYtJoinNodeOp* op);

TVector<TString> JoinLeafLabels(TExprNode::TPtr label);

}  // namespace NYql
