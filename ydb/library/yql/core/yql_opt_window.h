#pragma once
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>

namespace NYql {

struct TTypeAnnotationContext;
TExprNode::TPtr ExpandCalcOverWindow(const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& types);

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

    TCoFrameBound GetFirst() const;
    TCoFrameBound GetLast() const;

    bool IsNonEmpty() const { return NeverEmpty; }
    bool IsCompact() const { return Compact; }
    EFrameType GetFrameType() const { return Type; }
private:
    EFrameType Type = FrameByRows;
    TExprNode::TPtr First;
    TMaybe<i32> FirstOffset;
    TExprNode::TPtr Last;
    TMaybe<i32> LastOffset;
    bool NeverEmpty = false;
    bool Compact = false;
};

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
