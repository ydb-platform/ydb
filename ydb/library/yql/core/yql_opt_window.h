#pragma once
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>

namespace NYql {

TExprNode::TPtr ExpandCalcOverWindow(const TExprNode::TPtr& node, TExprContext& ctx);

TExprNodeList ExtractCalcsOverWindow(const TExprNode::TPtr& node, TExprContext& ctx); 
TExprNode::TPtr RebuildCalcOverWindowGroup(TPositionHandle pos, const TExprNode::TPtr& input, const TExprNodeList& calcs, TExprContext& ctx); 
 
struct TWindowFrameSettings { 
    TMaybe<i32> First; 
    TMaybe<i32> Last; 
    bool NeverEmpty = true; 
    bool IsCompact = false; 
}; 
 
bool ParseWindowFrameSettings(const TExprNode& node, TWindowFrameSettings& settings, TExprContext& ctx); 
 
// Lambda(input: Stream/List<T>) -> Stream/List<Tuple<T, SessionKey, SessionState, ....>> 
// input is assumed to be partitioned by partitionKeySelector 
TExprNode::TPtr ZipWithSessionParamsLambda(TPositionHandle pos, const TExprNode::TPtr& partitionKeySelector, 
    const TExprNode::TPtr& sessionKeySelector, const TExprNode::TPtr& sessionInit, 
    const TExprNode::TPtr& sessionUpdate, TExprContext& ctx); 
 
// input should be List/Stream of structs + see above 
TExprNode::TPtr AddSessionParamsMemberLambda(TPositionHandle pos, 
    TStringBuf sessionStartMemberName, TStringBuf sessionParamsMemberName, 
    const TExprNode::TPtr& partitionKeySelector, 
    const TExprNode::TPtr& sessionKeySelector, const TExprNode::TPtr& sessionInit, 
    const TExprNode::TPtr& sessionUpdate, TExprContext& ctx); 
}
