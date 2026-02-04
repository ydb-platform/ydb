#pragma once

#include "yql_window_frame_settings.h"

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>

namespace NYql {

struct TTypeAnnotationContext;
TExprNode::TPtr ExpandCalcOverWindow(const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& types);

TExprNodeList ExtractCalcsOverWindow(const TExprNode::TPtr& node, TExprContext& ctx);
TExprNode::TPtr RebuildCalcOverWindowGroup(TPositionHandle pos, const TExprNode::TPtr& input, const TExprNodeList& calcs, TExprContext& ctx);

using NNodes::TCoWinOnBase;
using NNodes::TCoFrameBound;

struct TSessionWindowParams {
    TSessionWindowParams()
        : Traits(nullptr)
        , Key(nullptr)
        , KeyType(nullptr)
        , ParamsType(nullptr)
        , Init(nullptr)
        , Update(nullptr)
        , SortTraits(nullptr)
    {}

    void Reset();

    TExprNode::TPtr Traits;
    TExprNode::TPtr Key;
    const TTypeAnnotationNode* KeyType;
    const TTypeAnnotationNode* ParamsType;
    TExprNode::TPtr Init;
    TExprNode::TPtr Update;
    TExprNode::TPtr SortTraits;
};

struct TSortParams {
    TExprNode::TPtr Key;
    TExprNode::TPtr Order;
};

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

// input should be List/Stream of structs + see above
TExprNode::TPtr AddSessionParamsMemberLambda(TPositionHandle pos,
    TStringBuf sessionStartMemberName, const TExprNode::TPtr& partitionKeySelector,
    const TSessionWindowParams& sessionWindowParams, TExprContext& ctx);

}
