#pragma once
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>

namespace NYql {

TExprNode::TPtr ExpandCalcOverWindow(const TExprNode::TPtr& node, TExprContext& ctx);

TExprNodeList ExtractCalcsOverWindow(const TExprNode::TPtr& node, TExprContext& ctx);
TExprNode::TPtr RebuildCalcOverWindowGroup(TPositionHandle pos, const TExprNode::TPtr& input, const TExprNodeList& calcs, TExprContext& ctx);

enum EFrameType {
    FrameByRows,
    FrameByRange,
    FrameByGroups,
};

using NNodes::TCoWinOnBase;
using NNodes::TCoFrameBound;

bool IsUnbounded(const NNodes::TCoFrameBound& bound);
bool IsCurrentRow(const NNodes::TCoFrameBound& bound);

class TWindowFrameSettings {
public:
    static TWindowFrameSettings Parse(const TExprNode& node, TExprContext& ctx);
    static TMaybe<TWindowFrameSettings> TryParse(const TExprNode& node, TExprContext& ctx);

    // This two functions can only be used for FrameByRows or FrameByGroups
    TMaybe<i32> GetFirstOffset() const;
    TMaybe<i32> GetLastOffset() const;

    TCoFrameBound GetFirst(bool& isPreceding) const;
    TCoFrameBound GetLast(bool& isPreceding) const;

    bool IsNonEmpty() const { return NeverEmpty; }
    bool IsCompact() const { return Compact; }
    EFrameType GetFrameType() const { return Type; }
private:
    EFrameType Type = FrameByRows;
    TExprNode::TPtr First;
    TMaybe<i32> FirstOffset;
    TExprNode::TPtr Last;
    TMaybe<i32> LastOffset;
    bool NeverEmpty = true;
    bool Compact = false;
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
}
