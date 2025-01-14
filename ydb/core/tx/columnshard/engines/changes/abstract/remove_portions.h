#pragma once

#include "changes.h"

namespace NKikimr::NOlap {

class TRemovePortionsChange: public IChangeAction {
private:
    THashMap<TPortionAddress, std::shared_ptr<const TPortionInfo>> Portions;

    virtual std::shared_ptr<NDataLocks::ILock> DoBuildDataLock(const TString& id, const NDataLocks::ELockCategory lockCategory) const override;
    virtual void DoApplyOnExecute(
        NColumnShard::TColumnShard* self, TWriteIndexContext& context, const TDataAccessorsResult& fetchedDataAccessors) override;
    virtual void DoApplyOnComplete(
        NColumnShard::TColumnShard* self, TWriteIndexCompleteContext& context, const TDataAccessorsResult& fetchedDataAccessors) override;

public:
    const THashMap<TPortionAddress, std::shared_ptr<const TPortionInfo>>& GetPortions() const {
        return Portions;
    }

    THashSet<ui64> GetPortionIds() const {
        THashSet<ui64> result;
        for (auto&& i : Portions) {
            result.emplace(i.first.GetPortionId());
        }
        return result;
    }

    const THashMap<TPortionAddress, TPortionInfo::TConstPtr>& GetPortionsToRemove() const {
        return Portions;
    }

    ui32 GetSize() const {
        return Portions.size();
    }

    bool HasPortions() const {
        return Portions.size();
    }

    bool AddPortion(const TPortionInfo::TConstPtr& info) {
        AFL_VERIFY(!info->HasRemoveSnapshot());
        return Portions.emplace(info->GetAddress(), info).second;
    }
};

}   // namespace NKikimr::NOlap
