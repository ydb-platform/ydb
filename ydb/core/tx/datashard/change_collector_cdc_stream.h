#pragma once

#include "change_collector_base.h"
#include "datashard_user_table.h"

namespace NACLib {
    class TUserContext;
}

namespace NKikimr {
namespace NDataShard {

class TCdcStreamChangeCollector: public TBaseChangeCollector {
    struct TValueSchema {
        TUserTable::TCPtr Table;
        ui64 Version = 0;
        TVector<NTable::TTag> Tags;
        TVector<NScheme::TTypeInfo> Types;
    };

    const TValueSchema& GetValueSchema(const TUserTable::TCPtr& table);

    TMaybe<NTable::TRowState> GetState(const TTableId& tableId, TArrayRef<const TRawTypeValue> key,
        TArrayRef<const NTable::TTag> valueTags, NTable::TSelectStats& stats, const TMaybe<TRowVersion>& snapshot = {});
    TMaybe<NTable::TRowState> GetState(const TTableId& tableId, TArrayRef<const TRawTypeValue> key,
        TArrayRef<const NTable::TTag> valueTags, const TMaybe<TRowVersion>& snapshot = {});
    static NTable::TRowState PatchState(const NTable::TRowState& oldState, NTable::ERowOp rop,
        const THashMap<NTable::TTag, NTable::TPos>& tagToPos, const THashMap<NTable::TTag, NTable::TUpdateOp>& updates);

    void Persist(const TTableId& tableId, const TPathId& pathId, NTable::ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const NTable::TTag> keyTags, TArrayRef<const NTable::TUpdateOp> updates,
        TIntrusivePtr<NACLib::TUserContext> userCtx);
    void Persist(const TTableId& tableId, const TPathId& pathId, NTable::ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const NTable::TTag> keyTags,
        const NTable::TRowState* oldState, const NTable::TRowState* newState, TArrayRef<const NTable::TTag> valueTags,
        TIntrusivePtr<NACLib::TUserContext> userCtx);

public:
    using TBaseChangeCollector::TBaseChangeCollector;

    void OnRestart() override;
    bool NeedToReadKeys() const override;
    bool NeedHistoricalPrecharge(const TTableId& tableId, TArrayRef<const TRawTypeValue> key) const override;

    bool Collect(const TTableId& tableId, NTable::ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const NTable::TUpdateOp> updates,
        TIntrusivePtr<NACLib::TUserContext> userCtx) override;

private:
    mutable TMaybe<bool> CachedNeedToReadKeys;
    TMaybe<TValueSchema> CachedValueSchema;

}; // TCdcStreamChangeCollector

} // NDataShard
} // NKikimr
