#pragma once
#include "datashard_impl.h"

namespace NKikimr::NDataShard {

class TDataShardLocksDb
    : public ILocksDb
{
public:
    TDataShardLocksDb(TDataShard& self, TTransactionContext& txc)
        : Self(self)
        , DB(txc.DB)
    { }

    bool HasChanges() const {
        return HasChanges_;
    }

    bool Load(TVector<TLockRow>& rows) override;

    bool MayAddLock(ui64 lockId) override;

    // Persist adding/removing a lock info
    void PersistAddLock(ui64 lockId, ui32 lockNodeId, ui32 generation, ui64 counter, ui64 createTs, ui64 flags = 0) override;
    void PersistLockCounter(ui64 lockId, ui64 counter) override;
    void PersistRemoveLock(ui64 lockId) override;

    // Persist adding/removing info on locked ranges
    void PersistAddRange(ui64 lockId, ui64 rangeId, const TPathId& tableId, ui64 flags = 0, const TString& data = {}) override;
    void PersistRangeFlags(ui64 lockId, ui64 rangeId, ui64 flags) override;
    void PersistRemoveRange(ui64 lockId, ui64 rangeId) override;

    // Persist a conflict, i.e. this lock must break some other lock on commit
    void PersistAddConflict(ui64 lockId, ui64 otherLockId) override;
    void PersistRemoveConflict(ui64 lockId, ui64 otherLockId) override;

private:
    TDataShard& Self;
    NTable::TDatabase& DB;
    bool HasChanges_ = false;
};

} // namespace NKikimr::NDataShard
