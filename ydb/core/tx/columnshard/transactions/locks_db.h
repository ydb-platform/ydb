#pragma once
#include <ydb/core/tx/locks/locks_db.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>


namespace NKikimr::NColumnShard {

class TColumnShardLocksDb : public NLocks::TShardLocksDb<TColumnShard, NColumnShard::Schema> {
private:
    using TBase = NLocks::TShardLocksDb<TColumnShard, NColumnShard::Schema>;

public:
    using TBase::TBase;

    void PersistRemoveLock(ui64 lockId) override {
        NIceDb::TNiceDb db(DB);
        db.Table<NColumnShard::Schema::Locks>().Key(lockId).Delete();
        HasChanges_ = true;
    }

    bool MayAddLock(ui64) override {
        return true;
    }

};

}
