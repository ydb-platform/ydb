#pragma once
#include <ydb/core/tx/locks/locks_db.h>

#include "datashard_impl.h"

namespace NKikimr::NDataShard {

class TDataShardLocksDb : public NLocks::TShardLocksDb<TDataShard, TDataShard::Schema> {
private:
    using TBase = NLocks::TShardLocksDb<TDataShard, TDataShard::Schema>;

public:
    using TSchema = TDataShard::Schema;
    using TBase::TBase;

    void PersistLockCounter(ui64 lockId, ui64 counter) override;
    void PersistRemoveLock(ui64 lockId) override;
    bool MayAddLock(ui64 lockId) override;
    // Activation adds logical state to an existing TrackingOnly object. Unlike
    // a new id, it may already have harmless references in borrowed parts.
    bool MayActivateLock(ui64 lockId) override;
};

} // namespace NKikimr::NDataShard
