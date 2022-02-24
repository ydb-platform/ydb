#pragma once

#include "change_collector_base.h"
#include "change_collector_helpers.h"
#include "datashard_user_table.h"

namespace NKikimr {
namespace NDataShard {

class TCdcStreamChangeCollector: public TBaseChangeCollector {
    TMaybe<NTable::TRowState> GetCurrentState(ui32 tid, TArrayRef<const TRawTypeValue> key,
        TArrayRef<const NTable::TTag> keyTags, TArrayRef<const NTable::TTag> valueTags);
    static NTable::TRowState PatchState(const NTable::TRowState& oldState, NTable::ERowOp rop,
        const THashMap<NTable::TTag, NTable::TPos>& tagToPos, const THashMap<NTable::TTag, NTable::TUpdateOp>& updates);

    void Persist(const TTableId& tableId, const TPathId& pathId, NTable::ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const NTable::TTag> keyTags, TArrayRef<const NTable::TUpdateOp> updates);
    void Persist(const TTableId& tableId, const TPathId& pathId, NTable::ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const NTable::TTag> keyTags,
        const NTable::TRowState* oldState, const NTable::TRowState* newState, TArrayRef<const NTable::TTag> valueTags);

public:
    using TBaseChangeCollector::TBaseChangeCollector;

    bool NeedToReadKeys() const override;
    void SetReadVersion(const TRowVersion& readVersion) override;

    bool Collect(const TTableId& tableId, NTable::ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const NTable::TUpdateOp> updates) override;

private:
    TRowVersion ReadVersion;

    mutable TMaybe<bool> CachedNeedToReadKeys;
    TRowsCache RowsCache;

}; // TCdcStreamChangeCollector

} // NDataShard
} // NKikimr
