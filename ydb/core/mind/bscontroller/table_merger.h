#pragma once

#include "defs.h"
#include "diff.h"

namespace NKikimr {

    using NTabletFlatExecutor::TTransactionContext;

    // Base table merger
    template<typename Table, typename TParam, typename T>
    class TTableMerger;

    template<typename Table, typename TParam, typename TKey, typename TRow>
    class TTableMerger<Table, TParam, TMap<TKey, TRow>>
    {
        using TContainer = TMap<TKey, TRow>;
        using TItem = typename TContainer::value_type;

        const TContainer *Original;
        const TContainer *Changed;
        TParam Param;

    public:
        template<typename T>
        TTableMerger(const TContainer *original, const TContainer *changed, T&& param)
            : Original(original)
            , Changed(changed)
            , Param(std::forward<T>(param))
        {}

        void operator()(TTransactionContext &txc) const;
    };

    template<typename Table, typename TParam, typename TKey>
    class TTableMerger<Table, TParam, TSet<TKey>>
    {
        using TContainer = TSet<TKey>;
        using TItem = typename TContainer::value_type;

        const TContainer *Original;
        const TContainer *Changed;
        TParam Param;

    public:
        template<typename T>
        TTableMerger(const TContainer *original, const TContainer *changed, T&& param)
            : Original(original)
            , Changed(changed)
            , Param(std::forward<T>(param))
        {}

        void operator()(TTransactionContext &txc) const;
    };

    template<typename Table, typename TParam, typename T>
    TTableMerger<Table, TParam, T> MakeTableMerger(const T *original, const T *changed, TParam&& param) {
        return {original, changed, std::forward<TParam>(param)};
    }

    template<typename Table, typename TParam, typename TKey, typename TRow>
    void TTableMerger<Table, TParam, TMap<TKey, TRow>>::operator()(TTransactionContext &txc) const {
        for (auto row : Diff(Original, Changed)) {
            auto callback = [&](auto *adapter) {
                auto [prev, cur] = row;
                if (!cur) {
                    // the row has been deleted
                    adapter->IssueEraseRow(txc, prev->first);
                } else if (!prev || !adapter->Equals(prev->second, cur->second)) {
                    // the row has been added or modified
                    adapter->IssueUpdateRow(txc, cur->first, cur->second);
                }
                auto processInline = [&](const auto (TRow::*cell), const auto *table) {
                    auto [prev, cur] = row;
                    using TCell = std::remove_reference_t<decltype(std::declval<TRow>().*cell)>;
                    const TCell *prevInline = prev ? &(prev->second.*cell) : nullptr;
                    const TCell *curInline = cur ? &(cur->second.*cell) : nullptr;
                    auto merger = MakeTableMerger<std::remove_pointer_t<decltype(table)>>(prevInline, curInline, Param);
                    merger(txc);
                };
                adapter->ForEachInlineTable(std::move(processInline));
            };
            TRow::Apply(Param, std::move(callback));
        }
    }

    template<typename Table, typename TParam, typename TKey>
    void TTableMerger<Table, TParam, TSet<TKey>>::operator ()(TTransactionContext &txc) const {
        for (auto [prev, cur] : Diff(Original, Changed)) {
            // pick the operation we want -- either insert row, or erase row
            const TKey *key = nullptr;
            NTable::ERowOp op;
            if (!cur) {
                key = prev;
                op = NTable::ERowOp::Erase;
            } else if (!prev) {
                key = cur;
                op = NTable::ERowOp::Upsert;
            }

            if (key) {
                auto x = NTableAdapter::WrapTuple(*key);
                auto keyTuple = NTableAdapter::PrepareKeyTuple<Table>(&x);
                TStackVec<TRawTypeValue, std::tuple_size<decltype(keyTuple)>::value> keyForTable;
                NTableAdapter::MapKey<Table>(&keyTuple, keyForTable);
                txc.DB.Update(Table::TableId, op, keyForTable, {});
            }
        }
    }

} // NKikimr
