#pragma once
#include "changes.h"

#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

namespace NKikimr::NOlap {

class TMovePortionsChange: public IChangeAction {
private:
    THashMap<TPortionAddress, std::shared_ptr<const TPortionInfo>> Portions;
    YDB_READONLY_DEF(std::optional<ui64>, TargetCompactionLevel);

    virtual std::shared_ptr<NDataLocks::ILock> DoBuildDataLock(const TString& id, const NDataLocks::ELockCategory lockCategory) const override;
    virtual void DoApplyOnExecute(
        NColumnShard::TColumnShard* self, TWriteIndexContext& context, const TDataAccessorsResult& fetchedDataAccessor) override;
    virtual void DoApplyOnComplete(
        NColumnShard::TColumnShard* self, TWriteIndexCompleteContext& context, const TDataAccessorsResult& fetchedDataAccessor) override;

public:
    const THashMap<TPortionAddress, TPortionInfo::TConstPtr>& GetPortionsToRemove() const {
        return Portions;
    }

    ui32 GetSize() const {
        return Portions.size();
    }

    bool HasPortions() const {
        return Portions.size();
    }

    void AddPortions(const std::vector<std::shared_ptr<TPortionInfo>>& portions) {
        for (auto&& i : portions) {
            AFL_VERIFY(i);
            AFL_VERIFY(Portions.emplace(i->GetAddress(), i).second)("portion_id", i->GetPortionId());
        }
    }

    void SetTargetCompactionLevel(const ui64 level) {
        TargetCompactionLevel = level;
    }
};

}   // namespace NKikimr::NOlap
