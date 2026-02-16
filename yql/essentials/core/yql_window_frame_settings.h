#pragma once

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/sql_types/window_frames_collector_params.h>
#include <yql/essentials/core/sql_types/window_number_and_direction.h>
#include <yql/essentials/core/sql_types/sort_order.h>
#include <yql/essentials/core/yql_expr_node_core_win_frame_collector_bounds.h>

namespace NYql {

enum EFrameType {
    FrameByRows,
    FrameByRange,
    FrameByGroups,
};

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
        using TBoundType = NYql::NWindow::TNumberAndDirection<TExprNode::TPtr>;

        TRangeFrame(std::pair<TBoundType, TBoundType> frame, bool isNumeric, ESortOrder sortOrder, bool isRightCurrentRow)
            : Frame_(frame)
            , IsNumeric_(isNumeric)
            , SortOrder_(sortOrder)
            , IsRightCurrentRow_(isRightCurrentRow)
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

        bool IsRightCurrentRow() const {
            return IsRightCurrentRow_;
        }

    private:
        std::pair<TBoundType, TBoundType> Frame_;
        bool IsNumeric_;
        ESortOrder SortOrder_;
        bool IsRightCurrentRow_;
    };

    using TGroupsFrame = std::monostate;

    using TFrame = std::variant<TRowFrame, TRangeFrame, TGroupsFrame>;

    TWindowFrameSettings(const TFrame& frameBounds, bool neverEmpty, bool compact, bool isAlwaysEmpty);

    static TWindowFrameSettings Parse(const TExprNode& node, TExprContext& ctx);
    static TMaybe<TWindowFrameSettings> TryParse(const TExprNode& node, TExprContext& ctx, bool& isUniversal);
    static TExprNode::TPtr GetSortSpec(const TExprNode& node, TExprContext& ctx);

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

    bool IsRightCurrent() const;

private:
    TFrame FrameBounds_;

    bool NeverEmpty_ = false;
    bool Compact_ = false;
    bool IsAlwaysEmpty_ = false;
};

using TExprNodeNumberAndDirection = TWindowFrameSettings::TRangeFrame::TBoundType;
using TExprNodeCoreWinFrameCollectorBounds = NWindow::TExprNodeCoreWinFrameCollectorBounds;
using TExprNodeCoreWinFrameCollectorParams = NWindow::TCoreWinFramesCollectorParams<TWindowFrameSettings::TRangeFrame::TBoundType::TNumberType>;

inline constexpr TStringBuf SortedColumnMemberName = "_yql_sorted_column";

bool CheckRowFrameIsAlwaysEmpty(const TWindowFrameSettings::TRowFrame& frame);

EFrameBoundsNewType GetFrameTypeNew(const TWindowFrameSettings& frameSettings);

} // namespace NYql
