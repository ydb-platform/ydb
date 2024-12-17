#include "mkql_match_recognize_rows_formatter.h"

#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

namespace NKikimr::NMiniKQL::NMatchRecognize {

namespace {

class TOneRowFormatter final : public IRowsFormatter {
public:
    explicit TOneRowFormatter(const TState& state) : IRowsFormatter(state) {}

    NUdf::TUnboxedValue GetFirstMatchRow(
        TComputationContext& ctx,
        const TSparseList& rows,
        const NUdf::TUnboxedValue& partitionKey,
        const TNfaTransitionGraph& graph,
        const TNfa::TMatch& match) {
        Match_ = match;
        const auto result = DoGetMatchRow(ctx, rows, partitionKey, graph);
        IRowsFormatter::Clear();
        return result;
    }

    NUdf::TUnboxedValue GetOtherMatchRow(
        TComputationContext& ctx,
        const TSparseList& rows,
        const NUdf::TUnboxedValue& partitionKey,
        const TNfaTransitionGraph& graph) {
        return NUdf::TUnboxedValue{};
    }
};

class TAllRowsFormatter final : public IRowsFormatter {
public:
    explicit TAllRowsFormatter(const IRowsFormatter::TState& state) : IRowsFormatter(state) {}

    NUdf::TUnboxedValue GetFirstMatchRow(
        TComputationContext& ctx,
        const TSparseList& rows,
        const NUdf::TUnboxedValue& partitionKey,
        const TNfaTransitionGraph& graph,
        const TNfa::TMatch& match) {
        Match_ = match;
        CurrentRowIndex_ = Match_.BeginIndex;
        for (const auto& matchedVar : Match_.Vars) {
            for (const auto& range : matchedVar) {
                ToIndexToMatchRangeLookup_.emplace(range.To(), range);
            }
        }
        return GetMatchRow(ctx, rows, partitionKey, graph);
    }

    NUdf::TUnboxedValue GetOtherMatchRow(
        TComputationContext& ctx,
        const TSparseList& rows,
        const NUdf::TUnboxedValue& partitionKey,
        const TNfaTransitionGraph& graph) {
        return GetMatchRow(ctx, rows, partitionKey, graph);
    }

private:
    NUdf::TUnboxedValue GetMatchRow(TComputationContext& ctx, const TSparseList& rows, const NUdf::TUnboxedValue& partitionKey, const TNfaTransitionGraph& graph) {
        while (CurrentRowIndex_ <= Match_.EndIndex) {
            if (auto iter = ToIndexToMatchRangeLookup_.lower_bound(CurrentRowIndex_);
                iter == ToIndexToMatchRangeLookup_.end()) {
                MKQL_ENSURE(false, "Internal logic error");
            } else if (auto transition = std::get_if<TMatchedVarTransition>(&graph.Transitions.at(iter->second.NfaIndex()));
                !transition) {
                MKQL_ENSURE(false, "Internal logic error");
            } else if (transition->ExcludeFromOutput) {
                ++CurrentRowIndex_;
            } else {
                break;
            }
        }
        if (CurrentRowIndex_ > Match_.EndIndex) {
            return NUdf::TUnboxedValue{};
        }
        const auto result = DoGetMatchRow(ctx, rows, partitionKey, graph);
        ++CurrentRowIndex_;
        if (CurrentRowIndex_ == Match_.EndIndex) {
            Clear();
        }
        return result;
    }

    void Clear() {
        IRowsFormatter::Clear();
        ToIndexToMatchRangeLookup_.clear();
    }

    TMap<size_t, const TSparseList::TRange&> ToIndexToMatchRangeLookup_;
};

} // anonymous namespace

IRowsFormatter::IRowsFormatter(const TState& state) : State_(state) {}

TOutputColumnOrder IRowsFormatter::GetOutputColumnOrder(
    TRuntimeNode outputColumnOrder) {
    TOutputColumnOrder result;
    auto list = AS_VALUE(TListLiteral, outputColumnOrder);
    TConstArrayRef<TRuntimeNode> items(list->GetItems(), list->GetItemsCount());
    for (auto item : items) {
        const auto entry = AS_VALUE(TStructLiteral, item);
        result.emplace_back(
            AS_VALUE(TDataLiteral, entry->GetValue(0))->AsValue().Get<ui32>(),
            static_cast<EOutputColumnSource>(AS_VALUE(TDataLiteral, entry->GetValue(1))->AsValue().Get<i32>())
        );
    }
    return result;
}

NUdf::TUnboxedValue IRowsFormatter::DoGetMatchRow(TComputationContext& ctx, const TSparseList& rows, const NUdf::TUnboxedValue& partitionKey, const TNfaTransitionGraph& graph) {
    NUdf::TUnboxedValue *itemsPtr = nullptr;
    const auto result = State_.Cache->NewArray(ctx, State_.OutputColumnOrder.size(), itemsPtr);
    for (const auto& columnEntry: State_.OutputColumnOrder) {
        switch(columnEntry.SourceType) {
            case EOutputColumnSource::PartitionKey:
                *itemsPtr++ = partitionKey.GetElement(columnEntry.Index);
                break;
            case EOutputColumnSource::Measure:
                *itemsPtr++ = State_.Measures[columnEntry.Index]->GetValue(ctx);
                break;
            case EOutputColumnSource::Other:
                *itemsPtr++ = rows.Get(CurrentRowIndex_).GetElement(columnEntry.Index);
                break;
        }
    }
    return result;
}

std::unique_ptr<IRowsFormatter> IRowsFormatter::Create(const IRowsFormatter::TState& state) {
    switch (state.RowsPerMatch) {
        case ERowsPerMatch::OneRow:
            return std::unique_ptr<IRowsFormatter>(new TOneRowFormatter(state));
        case ERowsPerMatch::AllRows:
            return std::unique_ptr<IRowsFormatter>(new TAllRowsFormatter(state));
    }
}

} //namespace NKikimr::NMiniKQL::NMatchRecognize
