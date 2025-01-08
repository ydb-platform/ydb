#pragma once
#include "abstract.h"

#include <ydb/core/tx/columnshard/data_locks/locks/abstract.h>

namespace NKikimr::NColumnShard {
class TColumnShard;
}

namespace NKikimr::NOlap {

class IChangeAction {
private:
    virtual std::shared_ptr<NDataLocks::ILock> DoBuildDataLock(const TString& id, const NDataLocks::ELockCategory lockCategory) const = 0;
    virtual void DoApplyOnExecute(
        NColumnShard::TColumnShard* self, TWriteIndexContext& context, const TDataAccessorsResult& fetchedDataAccessors) = 0;
    virtual void DoApplyOnComplete(
        NColumnShard::TColumnShard* self, TWriteIndexCompleteContext& context, const TDataAccessorsResult& fetchedDataAccessors) = 0;

public:
    IChangeAction() = default;

    std::shared_ptr<NDataLocks::ILock> BuildDataLock(const TString& id, const NDataLocks::ELockCategory lockCategory) const {
        return DoBuildDataLock(id, lockCategory);
    }

    void ApplyOnExecute(NColumnShard::TColumnShard* self, TWriteIndexContext& context, const TDataAccessorsResult& fetchedDataAccessors) {
        return DoApplyOnExecute(self, context, fetchedDataAccessors);
    }

    void ApplyOnComplete(
        NColumnShard::TColumnShard* self, TWriteIndexCompleteContext& context, const TDataAccessorsResult& fetchedDataAccessors) {
        return DoApplyOnComplete(self, context, fetchedDataAccessors);
    }
};

}   // namespace NKikimr::NOlap
