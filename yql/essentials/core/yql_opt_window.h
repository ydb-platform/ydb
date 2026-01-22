#pragma once

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/sql_types/window_number_and_direction.h>
#include <yql/essentials/core/sql_types/sort_order.h>

#include <util/generic/overloaded.h>

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

enum class EFrameBoundsType: ui8 {
    EMPTY,
    LAGGING,
    CURRENT,
    LEADING,
    FULL,
    GENERIC,
};

enum class EFrameBoundsNewType: ui8 {
    EMPTY,
    INCREMENTAL,
    FULL,
    GENERIC,
};

class TWindowFrameSettings {
public:
    using TRowFrame = std::pair<TMaybe<i32>, TMaybe<i32>>;

    class TRangeFrame {
    public:
        using ESortOrder = NYql::ESortOrder;
        using TBoundType = NYql::NWindow::TNumberAndDirection<TString>;

        TRangeFrame(std::pair<TBoundType, TBoundType> frame, bool isNumeric, ESortOrder sortOrder, const TString& boundsCallable)
            : Frame_(frame)
            , IsNumeric_(isNumeric)
            , SortOrder_(sortOrder)
            , BoundsCallable_(boundsCallable)
        {
        }

        const TBoundType& GetFirst() const {
            return Frame_.first;
        }
        const TBoundType& GetLast() const {
            return Frame_.second;
        }

        bool IsNumeric() const {
            return IsNumeric_;
        }

        ESortOrder GetSortOrder() const {
            return SortOrder_;
        }

        TStringBuf BoundsCallable() const {
            return BoundsCallable_;
        }

    private:
        std::pair<TBoundType, TBoundType> Frame_;
        bool IsNumeric_;
        ESortOrder SortOrder_;
        TString BoundsCallable_;
    };

    using TGroupsFrame = std::monostate;

    using TFrame = std::variant<TRowFrame, TRangeFrame, TGroupsFrame>;

    TWindowFrameSettings(const TFrame& frameBounds, bool neverEmpty, bool compact, bool isAlwaysEmpty);

    static TWindowFrameSettings Parse(const TExprNode& node, TExprContext& ctx);
    static TMaybe<TWindowFrameSettings> TryParse(const TExprNode& node, TExprContext& ctx, bool& isUniversal);

    bool IsNonEmpty() const {
        return NeverEmpty_;
    }

    bool IsCompact() const {
        return Compact_;
    }

    bool IsAlwaysEmpty() const {
        return IsAlwaysEmpty_;
    }

    EFrameType GetFrameType() const;

    bool IsFullPartition() const;

    const TRowFrame& GetRowFrame() const {
        YQL_ENSURE(GetFrameType() == FrameByRows);
        return std::get<TRowFrame>(FrameBounds_);
    }

    const TRangeFrame& GetRangeFrame() const {
        YQL_ENSURE(GetFrameType() == FrameByRange);
        return std::get<TRangeFrame>(FrameBounds_);
    }

    const TGroupsFrame& GetGroupsFrame() const {
        YQL_ENSURE(GetFrameType() == FrameByGroups);
        return std::get<TGroupsFrame>(FrameBounds_);
    }

    bool IsLeftInf() const;
    bool IsRightInf() const;

    bool IsLeftCurrent() const;
    bool IsRightCurrent() const;

private:
    TFrame FrameBounds_;

    bool NeverEmpty_ = false;
    bool Compact_ = false;
    bool IsAlwaysEmpty_ = false;
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
