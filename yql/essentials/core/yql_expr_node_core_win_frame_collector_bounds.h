#pragma once

#include <yql/essentials/core/sql_types/window_frame_bounds.h>
#include <yql/essentials/ast/yql_expr.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/generic/overloaded.h>

#include <utility>

namespace NYql::NWindow {

class TExprNodeCoreWinFrameCollectorBounds: private TCoreWinFrameCollectorBounds<TExprNode::TPtr> {
    using TBase = TCoreWinFrameCollectorBounds<TExprNode::TPtr>;

public:
    using THandle = typename TBase::THandle;
    using TRangeFrame = TInputRangeWindowFrame<TExprNode::TPtr>;
    using TRangeBound = TInputRange<TExprNode::TPtr>;

    // Key type for cache lookups using TInputRange/TInputRangeWindowFrame with string pairs.
    // This allows reusing existing hash functions TNumberAndDirectionHash and TWindowFrameHash.
    using TStringPair = std::pair<TString, TString>;
    using TBoundKey = TInputRange<TStringPair>;
    using TFrameKey = TInputRangeWindowFrame<TStringPair>;

    explicit TExprNodeCoreWinFrameCollectorBounds(bool dedup)
        : TBase()
        , Dedup_(dedup)
    {
    }

    THandle AddRange(const TRangeFrame& range) {
        if (Dedup_) {
            TFrameKey key = MakeFrameKey(range);
            if (auto* ptr = RangeIntervalsCache_.FindPtr(key)) {
                return *ptr;
            }
            THandle handle = TBase::AddRange(range);
            RangeIntervalsCache_.emplace(key, handle);
            return handle;
        }
        return TBase::AddRange(range);
    }

    THandle AddRow(const TInputRowWindowFrame& row) {
        if (Dedup_) {
            if (auto* ptr = RowIntervalsCache_.FindPtr(row)) {
                return *ptr;
            }
            THandle handle = TBase::AddRow(row);
            RowIntervalsCache_.emplace(row, handle);
            return handle;
        }
        return TBase::AddRow(row);
    }

    THandle AddRangeIncremental(const TRangeBound& delta) {
        if (Dedup_) {
            TBoundKey key = GetKey(delta);
            if (auto* ptr = RangeIncrementalsCache_.FindPtr(key)) {
                return *ptr;
            }
            THandle handle = TBase::AddRangeIncremental(delta);
            RangeIncrementalsCache_.emplace(key, handle);
            return handle;
        }
        return TBase::AddRangeIncremental(delta);
    }

    THandle AddRowIncremental(const TInputRow& delta) {
        if (Dedup_) {
            if (auto* ptr = RowIncrementalsCache_.FindPtr(delta)) {
                return *ptr;
            }
            THandle handle = TBase::AddRowIncremental(delta);
            RowIncrementalsCache_.emplace(delta, handle);
            return handle;
        }
        return TBase::AddRowIncremental(delta);
    }

    // Expose read-only accessors from base class.
    using TBase::Empty;
    using TBase::RangeIncrementals;
    using TBase::RangeIntervals;
    using TBase::RowIncrementals;
    using TBase::RowIntervals;

    // Convert to base class (returns const reference to underlying TCoreWinFrameCollectorBounds).
    const TBase& AsBase() const {
        return static_cast<const TBase&>(*this);
    }

private:
    static TBoundKey GetKey(const TRangeBound& bound) {
        return std::visit(TOverloaded{
            [&](const typename TRangeBound::TUnbounded&) {
                return TBoundKey::Inf(bound.GetDirection());
            },
            [](const typename TRangeBound::TZero&) {
                return TBoundKey::Zero();
            },
            [&](const TExprNode::TPtr& node) {
                Y_ENSURE(node->IsCallable({"Int8", "Uint8",
                                           "Int16", "Uint16",
                                           "Int32", "Uint32",
                                           "Int64", "Uint64",
                                           "Float", "Double",
                                           "Interval", "Interval64"}), "Unexpected callable type: " << node->Content());
                YQL_ENSURE(node->ChildrenSize() == 1, "Expected only one child.");
                YQL_ENSURE(node->Child(0)->IsAtom(), "Expected only one child.");
                TStringPair value{TString(node->Content()), TString(node->Child(0)->Content())};
                return TBoundKey(std::move(value), bound.GetDirection());
            }
        }, bound.GetValue());
    }

    static TFrameKey MakeFrameKey(const TRangeFrame& frame) {
        return TFrameKey(GetKey(frame.Min()), GetKey(frame.Max()));
    }

    bool Dedup_;

    THashMap<TFrameKey, THandle> RangeIntervalsCache_;
    THashMap<TBoundKey, THandle> RangeIncrementalsCache_;
    THashMap<TInputRowWindowFrame, THandle> RowIntervalsCache_;
    THashMap<TInputRow, THandle> RowIncrementalsCache_;
};

} // namespace NYql::NWindow
