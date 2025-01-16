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

    void AddValue(TVector<NTable::TUpdateOp>& out, const NTable::TUpdateOp& update);
    void AddRawValue(TVector<NTable::TUpdateOp>& out, NTable::TTag tag, const TRawTypeValue& value);
    void AddCellValue(TVector<NTable::TUpdateOp>& out, NTable::TTag tag, const TCell& cell, const NScheme::TTypeInfo& type);
    void AddNullValue(TVector<NTable::TUpdateOp>& out, NTable::TTag tag, const NScheme::TTypeInfo& type);

    void Persist(const TTableId& tableId, const TPathId& pathId, NTable::ERowOp rop,
        TArrayRef<const NTable::TUpdateOp> key, TArrayRef<const NTable::TUpdateOp> data);
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
    THashSet<NTable::TTag> KeyTagsSeen;
    TVector<NTable::TUpdateOp> KeyVals;
    TVector<NTable::TUpdateOp> DataVals;

}; // TAsyncIndexChangeCollector

} // NDataShard
} // NKikimr
