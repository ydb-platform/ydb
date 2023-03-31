#pragma once

#include "change_collector_base.h"

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>

namespace NKikimr {
namespace NDataShard {

class TCachedTagsBuilder;

class TAsyncIndexChangeCollector: public TBaseChangeCollector {
    friend class TCachedTagsBuilder;

    struct TCachedTags {
        explicit TCachedTags(
                TVector<NTable::TTag>&& columns,
                const std::pair<ui32, ui32>& indexRange)
            : Columns(std::move(columns))
            , IndexColumns(&Columns.at(indexRange.first), indexRange.second + 1)
        {
        }

        TVector<NTable::TTag> Columns; // Index + Data
        TArrayRef<NTable::TTag> IndexColumns;
    };

    auto CacheTags(const TTableId& tableId) const;
    TArrayRef<NTable::TTag> GetTagsToSelect(const TTableId& tableId, NTable::ERowOp rop) const;

    void FillKeyFromRowState(NTable::TTag tag, NTable::TPos pos, const NTable::TRowState& rowState, NScheme::TTypeInfo type);
    void FillKeyFromKey(NTable::TTag tag, NTable::TPos pos, TArrayRef<const TRawTypeValue> key);
    void FillKeyFromUpdate(NTable::TTag tag, NTable::TPos pos, TArrayRef<const NTable::TUpdateOp> updates);
    void FillKeyWithNull(NTable::TTag tag, NScheme::TTypeInfo type);
    void FillDataFromRowState(NTable::TTag tag, NTable::TPos pos, const NTable::TRowState& rowState, NScheme::TTypeInfo type);
    void FillDataFromUpdate(NTable::TTag tag, NTable::TPos pos, TArrayRef<const NTable::TUpdateOp> updates);
    void FillDataWithNull(NTable::TTag tag, NScheme::TTypeInfo type);

    void Persist(const TTableId& tableId, const TPathId& pathId, NTable::ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const NTable::TTag> keyTags,
        TArrayRef<const NTable::TUpdateOp> updates);

    void Clear();

public:
    using TBaseChangeCollector::TBaseChangeCollector;

    void OnRestart() override;
    bool NeedToReadKeys() const override;

    bool Collect(const TTableId& tableId, NTable::ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const NTable::TUpdateOp> updates) override;

private:
    mutable THashMap<TTableId, TCachedTags> CachedTags;

    // reused between Collect() calls, cleared after every Clear() call
    THashSet<NTable::TTag> TagsSeen;
    TVector<NTable::TTag> IndexKeyTags;
    TVector<TRawTypeValue> IndexKeyVals;
    TVector<NTable::TUpdateOp> IndexDataVals;

}; // TAsyncIndexChangeCollector

} // NDataShard
} // NKikimr
