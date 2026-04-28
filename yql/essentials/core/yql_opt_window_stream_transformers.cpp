#include "yql_opt_window_stream_transformers.h"

#include <yql/essentials/core/sql_types/sort_order.h>
#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/core/yql_window_frame_settings.h>

namespace NYql {

namespace {

constexpr TStringBuf SortedColumnMemberName = "_yql_sorted_column";

auto ForEachRangeFrame(const TExprNode::TPtr& frames, TExprContext& ctx, std::function<void(const TWindowFrameSettings::TRangeFrame&)> func) {
    for (auto& winOn : frames->ChildrenList()) {
        if (NNodes::TCoWinOnRange::Match(winOn.Get())) {
            func(TWindowFrameSettings::Parse(*winOn, ctx).GetRangeFrame());
        }
    }
}

ESortOrder ExtractAndVerifyRangeSortOrder(const TExprNode::TPtr& frames, TExprContext& ctx) {
    TMaybe<ESortOrder> sortOrder;
    ForEachRangeFrame(frames, ctx, [&](const TWindowFrameSettings::TRangeFrame& rangeFrame) {
        if (!sortOrder) {
            sortOrder = rangeFrame.GetSortOrder();
        } else {
            YQL_ENSURE(*sortOrder == rangeFrame.GetSortOrder(), "All Range frames must have the same SortOrder");
        }
    });
    return sortOrder.GetOrElse(ESortOrder::Unimportant);
}

TMaybe<const TTypeAnnotationNode*> GetSortedColumnType(const TExprNode::TPtr& sortTraits) {
    if (!sortTraits || sortTraits->IsCallable("Void")) {
        return {};
    }
    YQL_ENSURE(sortTraits->IsCallable("SortTraits"));

    auto sortKeyLambda = sortTraits->ChildPtr(2);
    YQL_ENSURE(sortKeyLambda->IsLambda());
    const TTypeAnnotationNode* sortKeyType = sortKeyLambda->GetTypeAnn();
    YQL_ENSURE(sortKeyType);
    return sortKeyType;
}

TMaybe<std::pair<TExprNode::TPtr, TNodeTransform>> GetSortedBoundNodeKeyForDedup(const TWindowFrameSettingBound& bound) {
    if (!bound.IsFinite()) {
        return {};
    }
    if (!bound.GetUnderlyingValue().GetColumnCast()){
        return {};
    }
    return std::make_pair(bound.GetUnderlyingValue().GetFrameBound(), *bound.GetUnderlyingValue().GetColumnCast());
};

} // namespace

TWindowSortedColumnPusher::TWindowSortedColumnPusher(const TExprNode::TPtr& sortTraits, TExprContext& ctx, const TExprNode::TPtr& frames)
    : Ctx_(ctx)
{
    SortOrder_ = ExtractAndVerifyRangeSortOrder(frames, ctx);
    SortColumnType_ = GetSortedColumnType(sortTraits);
    ForEachRangeFrame(frames, ctx, [&](const TWindowFrameSettings::TRangeFrame& rangeFrame) {
        auto boundForDedup = {GetSortedBoundNodeKeyForDedup(rangeFrame.GetFirst()), GetSortedBoundNodeKeyForDedup(rangeFrame.GetLast())};
        for (auto boundNodeAndTransform : boundForDedup) {
            if (boundNodeAndTransform) {
                // Insert with deduplication for the same functions.
                MappedSortedColumnsToNames_.emplace(
                    boundNodeAndTransform->first,
                    std::make_pair(boundNodeAndTransform->second, TString(TStringBuilder() << SortedColumnMemberName << "_" << MappedSortedColumnsToNames_.size() + 1)));
            }
        }
    });
}

std::pair<TStringBuf, TStringBuf> TWindowSortedColumnPusher::GetRangeSortedColumnNames(const TWindowFrameSettings::TRangeFrame& rangeFrame) const {
    YQL_ENSURE(ShouldAddSortedColumn(), "SortOrder is unimportant, but range frame is provided");
    auto getSortedColumnName = [this](const TMaybe<std::pair<TExprNode::TPtr, TNodeTransform>>& boundNodeAndTransform) -> TStringBuf {
        if (boundNodeAndTransform) {
            auto it = MappedSortedColumnsToNames_.find(boundNodeAndTransform->first);
            YQL_ENSURE(it != MappedSortedColumnsToNames_.end(), "Sorted column cast function not found.");
            const auto& [boundNode, transformAndColumnName] = *it;
            const auto& [transform, columnName] = transformAndColumnName;
            return columnName;
        }
        return SortedColumnMemberName;
    };

    return {getSortedColumnName(GetSortedBoundNodeKeyForDedup(rangeFrame.GetFirst())),
            getSortedColumnName(GetSortedBoundNodeKeyForDedup(rangeFrame.GetLast()))};
}

TVector<TStringBuf> TWindowSortedColumnPusher::GetAllSortedColumnNames() const {
    TVector<TStringBuf> sortedColumns;
    if (!ShouldAddSortedColumn()) {
        YQL_ENSURE(MappedSortedColumnsToNames_.empty(), "No sort column type is provided, but extrasorted columns are provided");
        return sortedColumns;
    }
    sortedColumns.reserve(MappedSortedColumnsToNames_.size() + 1);
    sortedColumns.push_back(SortedColumnMemberName);

    std::transform(MappedSortedColumnsToNames_.begin(), MappedSortedColumnsToNames_.end(), std::back_inserter(sortedColumns), [](const auto& item) {
        const auto& [keyNode, transformAndColumnName] = item;
        const auto& [transform, columnName] = transformAndColumnName;
        return columnName;
    });

    std::sort(sortedColumns.begin(), sortedColumns.end(), [](const auto& a, const auto& b) {
        return a < b;
    });
    return sortedColumns;
}

bool TWindowSortedColumnPusher::ShouldAddSortedColumn() const {
    return SortOrder() != ESortOrder::Unimportant;
}

ESortOrder TWindowSortedColumnPusher::SortOrder() const {
    return SortOrder_;
}

TExprNode::TPtr TWindowSortedColumnPusher::GetStreamWithSortedColumns(TExprNode::TPtr stream, TExprNode::TPtr sortKeySelector) const {
    if (!ShouldAddSortedColumn()) {
        return stream;
    }
    return AddSortedColumnsToStream(stream, sortKeySelector);
}

TExprNode::TPtr TWindowSortedColumnPusher::AddSortedColumnsToStream(TExprNode::TPtr stream, TExprNode::TPtr sortKeySelector) const {
    auto pos = stream->Pos();
    // If sortKeySelector is Void, nothing to do.
    if (sortKeySelector->IsCallable("Void")) {
        return stream;
    }
    // clang-format off
    // Add sorted column to the input stream using Map.
    auto rowArg = Ctx_.NewArgument(pos, "row");
    auto addMemberBody = Ctx_.Builder(pos)
        .Callable("AddMember")
            .Add(0, rowArg)
            .Atom(1, SortedColumnMemberName)
            .Apply(2, Ctx_.DeepCopyLambda(*sortKeySelector))
                .With(0, rowArg)
            .Seal()
        .Seal()
        .Build();

    if (!MappedSortedColumnsToNames_.empty()) {
        auto makePrefixStruct = [this, pos](const TExprNode::TPtr& structNode) {
            return Ctx_.Builder(pos)
                .List()
                    .Atom(0, "")
                    .Add(1, structNode)
                .Seal()
                .Build();
        };

        auto newFieldsAsStruct = Ctx_.Builder(pos)
            .Callable("AsStruct")
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    ui32 i = 0U;
                    for (const auto& [keyNode, transformAndColumnName] : MappedSortedColumnsToNames_) {
                        const auto& [transform, columnName] = transformAndColumnName;
                        auto sortedColumnValue = Ctx_.Builder(pos)
                            .Callable("Member")
                                .Add(0, addMemberBody)
                                .Atom(1, SortedColumnMemberName)
                            .Seal()
                            .Build();
                        auto memberValue = transform(sortedColumnValue, Ctx_);
                        parent.List(i)
                            .Atom(0, columnName)
                            .Add(1, memberValue)
                        .Seal();
                        ++i;
                    }
                    return parent;
                })
            .Seal()
            .Build();

        addMemberBody = Ctx_.NewCallable(pos, "FlattenMembers", TExprNode::TListType{
            makePrefixStruct(addMemberBody),
            makePrefixStruct(newFieldsAsStruct)
        });
    }

    auto addMemberLambda = Ctx_.NewLambda(pos, Ctx_.NewArguments(pos, {rowArg}), std::move(addMemberBody));

    auto streamWithSortedColumn = Ctx_.Builder(pos)
        .Callable("OrderedMap")
            .Add(0, stream)
            .Add(1, addMemberLambda)
        .Seal()
        .Build();

    return streamWithSortedColumn;
    // clang-format on
}

TExprNode::TPtr TWindowSortedColumnPusher::ClearStreamFromSortedColumns(TExprNode::TPtr stream) const {
    if (!ShouldAddSortedColumn()) {
        return stream;
    }
    auto pos = stream->Pos();
    auto rowArg = Ctx_.NewArgument(pos, "row");
    auto body = RemoveMembers(pos, rowArg, GetAllSortedColumnNames(), Ctx_);
    auto lambda = Ctx_.NewLambda(pos, Ctx_.NewArguments(pos, {rowArg}), std::move(body));
    // clang-format off
    return Ctx_.Builder(pos)
        .Callable("OrderedMap")
            .Add(0, stream)
            .Add(1, lambda)
        .Seal()
        .Build();
    // clang-format on
}
} // namespace NYql
