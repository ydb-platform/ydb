#pragma once

#include <ydb/core/scheme/scheme_tabledefs.h>

namespace NKikimr {
namespace NSysView {

template <size_t N, typename TKey, typename TField>
void SetField(TKey&, TField);

template <size_t N, typename TKey, typename TField>
void ConvertField(TKey& key, const TConstArrayRef<TCell>& cells) {
    if (cells.size() > N) {
        if (!cells[N].IsNull()) {
            SetField<N>(key, cells[N].AsValue<TField>());
        } else {
            SetField<N>(key, Min<TField>());
        }
    } else {
        SetField<N>(key, Max<TField>());
    }
}

template <size_t N, typename TKey>
void ConvertKey(TKey&, const TConstArrayRef<TCell>&) {
}

template <size_t N, typename TKey, typename TField, typename... TFields>
void ConvertKey(TKey& key, const TConstArrayRef<TCell>& cells) {
    ConvertField<N, TKey, TField>(key, cells);
    ConvertKey<N + 1, TKey, TFields...>(key, cells);
}

template <typename TRange, typename... TFields>
void ConvertKeyRange(TRange& protoRange, const TSerializedTableRange& tableRange) {
    ConvertKey<0, decltype(*protoRange.MutableFrom()), TFields...>(
        *protoRange.MutableFrom(), tableRange.From.GetCells());

    protoRange.SetInclusiveFrom(tableRange.FromInclusive);

    ConvertKey<0, decltype(*protoRange.MutableTo()), TFields...>(
        *protoRange.MutableTo(), tableRange.To.GetCells());

    protoRange.SetInclusiveTo(tableRange.To.GetCells().empty() ? true : tableRange.ToInclusive);
}

} // NSysView
} // NKikimr
