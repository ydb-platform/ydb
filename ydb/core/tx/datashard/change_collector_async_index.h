#pragma once

#include "change_collector_base.h"
#include "change_collector_helpers.h"

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
                THashMap<NTable::TTag, NTable::TPos>&& keyTagToPos,
                TVector<NTable::TTag>&& columns,
                const std::pair<ui32, ui32>& indexRange)
            : KeyTagToPos(std::move(keyTagToPos))
            , Columns(std::move(columns))
            , IndexColumns(&Columns.at(indexRange.first), indexRange.second + 1)
        {
        }

        THashMap<NTable::TTag, NTable::TPos> KeyTagToPos;
        TVector<NTable::TTag> Columns; // Index + Data
        TArrayRef<NTable::TTag> IndexColumns;
    };

    auto CacheTags(const TTableId& tableId) const;
    const THashMap<NTable::TTag, NTable::TPos>& GetKeyTagToPos(const TTableId& tableId) const;
    TArrayRef<NTable::TTag> GetTagsToSelect(const TTableId& tableId, NTable::ERowOp rop) const;

    void FillKeyFromRowState(NTable::TTag tag, NTable::TPos pos, const NTable::TRowState& rowState, NScheme::TTypeId type);
    void FillKeyFromKey(NTable::TTag tag, NTable::TPos pos, TArrayRef<const TRawTypeValue> key);
    void FillKeyFromUpdate(NTable::TTag tag, NTable::TPos pos, TArrayRef<const NTable::TUpdateOp> updates);
    void FillKeyWithNull(NTable::TTag tag, NScheme::TTypeId type);
    void FillDataFromUpdate(NTable::TTag tag, NTable::TPos pos, TArrayRef<const NTable::TUpdateOp> updates);
    void FillDataWithNull(NTable::TTag tag, NScheme::TTypeId type);

    void Persist(const TTableId& tableId, const TPathId& pathId, NTable::ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const NTable::TTag> keyTags,
        TArrayRef<const NTable::TUpdateOp> updates);

    void Clear();

public:
    using TBaseChangeCollector::TBaseChangeCollector;

    bool NeedToReadKeys() const override;
    void SetReadVersion(const TRowVersion& readVersion) override;

    bool Collect(const TTableId& tableId, NTable::ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const NTable::TUpdateOp> updates) override;

    void Reset() override;

private:
    TRowVersion ReadVersion;

    mutable THashMap<TTableId, TCachedTags> CachedTags;
    TRowsCache RowsCache;

    // reused between Collect() calls, cleared after every Clear() call
    THashSet<NTable::TTag> TagsSeen;
    TVector<NTable::TTag> IndexKeyTags;
    TVector<TRawTypeValue> IndexKeyVals;
    TVector<NTable::TUpdateOp> IndexDataVals;

}; // TAsyncIndexChangeCollector

} // NDataShard
} // NKikimr
