#pragma once
#include <ydb/core/tx/locks/locks_db.h>

#include "datashard_impl.h"

namespace NKikimr::NDataShard {

class TDataShardLocksDb : public NLocks::TShardLocksDb<TDataShard, TDataShard::Schema> {
private:
    using TBase = NLocks::TShardLocksDb<TDataShard, TDataShard::Schema>;

public:
    using TBase::TBase;

    void PersistRemoveLock(ui64 lockId) override;
    bool MayAddLock(ui64 lockId) override;
};

} // namespace NKikimr::NDataShard
