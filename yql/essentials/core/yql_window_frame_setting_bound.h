#pragma once

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_node_transform.h>
#include <yql/essentials/core/sql_types/window_number_and_direction.h>

#include <util/generic/hash.h>

#include <utility>

namespace NYql {

class TWindowFrameSettingWithOffset {
public:
    TWindowFrameSettingWithOffset(TExprNode::TPtr frameBound,
                                  TMaybe<TNodeTransform> columnCast,
                                  TMaybe<TNodeTransform> boundCast,
                                  TMaybe<ui32> procId)
        : FrameBound_(std::move(frameBound))
        , ColumnCast_(std::move(columnCast))
        , BoundCast_(std::move(boundCast))
        , ProcId_(std::move(procId))
    {
    }

    const TExprNode::TPtr& GetFrameBound() const {
        return FrameBound_;
    }

    const TMaybe<TNodeTransform>& GetColumnCast() const {
        return ColumnCast_;
    }

    const TMaybe<TNodeTransform>& GetBoundCast() const {
        return BoundCast_;
    }

    const TMaybe<ui32>& GetProcId() const {
        return ProcId_;
    }

private:
    TExprNode::TPtr FrameBound_;
    TExprNode::TPtr OriginalFrameBound_;
    TMaybe<TNodeTransform> ColumnCast_;
    TMaybe<TNodeTransform> BoundCast_;
    TMaybe<ui32> ProcId_;
};

template <typename TValueHasher = THash<TExprNode::TPtr>>
class TWindowFrameSettingOriginalBoundHash {
public:
    explicit TWindowFrameSettingOriginalBoundHash(TValueHasher hasher = TValueHasher{})
        : Hasher_(std::move(hasher))
    {
    }

    size_t operator()(const TWindowFrameSettingWithOffset& value) const {
        return Hasher_(value.GetFrameBound());
    }

private:
    TValueHasher Hasher_;
};

template <typename TValueComparator = TEqualTo<TExprNode::TPtr>>
class TWindowFrameSettingOriginalBoundComparator {
public:
    explicit TWindowFrameSettingOriginalBoundComparator(TValueComparator comparator = TValueComparator{})
        : Comparator_(comparator)
    {
    }

    bool operator()(const TWindowFrameSettingWithOffset& left, const TWindowFrameSettingWithOffset& right) const {
        return Comparator_(left.GetFrameBound(), right.GetFrameBound());
    }

private:
    TValueComparator Comparator_;
};

using TWindowFrameSettingBound = NWindow::TNumberAndDirection<TWindowFrameSettingWithOffset>;

} // namespace NYql
