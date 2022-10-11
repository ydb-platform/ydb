#pragma once

#include <ydb/core/tablet_flat/flat_database.h>

#include <util/generic/hash.h>

namespace NKikimr {
namespace NDataShard {

class TRowsCache {
    struct TRow {
        NTable::EReady Ready;
        NTable::ERowOp Rop;
        THashMap<NTable::TTag, TString> Cells;
    };

    const TRow* CacheRow(ui32 tid, NTable::EReady ready,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const NTable::TTag> tags, const NTable::TRowState& rowState);

    const TRow* FindCachedRow(ui32 tid, TArrayRef<const TRawTypeValue> key) const;

public:
    NTable::EReady SelectRow(NTable::TDatabase& db, ui32 tid,
        TArrayRef<const TRawTypeValue> key, const THashMap<NTable::TTag, NTable::TPos>& keyTagToPos,
        TArrayRef<const NTable::TTag> tags, NTable::TRowState& rowState, const TRowVersion& readVersion);

    void UpdateCachedRow(ui32 tid, NTable::ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const NTable::TUpdateOp> updates);

    void Reset();

private:
    THashMap<ui32, THashMap<TString, TRow>> Rows;

}; // TRowsCache

} // NDataShard
} // NKikimr
