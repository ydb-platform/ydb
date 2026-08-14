#pragma once

#include <ydb/core/tx/columnshard/engines/storage/actualizer/abstract/abstract.h>
#include <ydb/core/tx/columnshard/engines/storage/actualizer/common/address.h>

#include <util/generic/hash_set.h>

namespace NKikimr::NOlap {
class TVersionedIndex;
}

namespace NKikimr::NOlap::NActualizer {

class TMoveDataActualizer: public IActualizer {
private:
    const TVersionedIndex& VersionedIndex;

    // Portion IDs captured at Refresh time; only these are eligible for moving.
    // New portions written after the move started get new IDs and are not eligible.
    THashSet<ui64> InitialPortionIds;

    // RWAddress -> set<portionId>
    THashMap<TRWAddress, THashSet<ui64>> PortionsToMove;
    // portionId -> RWAddress (inverse index)
    THashMap<ui64, TRWAddress> PortionAddress;

protected:
    virtual void DoAddPortion(const TPortionInfo& info, const TAddExternalContext& context) override;
    virtual void DoRemovePortion(const ui64 portionId) override;
    virtual void DoExtractTasks(
        TTieringProcessContext& tasksContext, const TExternalTasksContext& externalContext, TInternalTasksContext& internalContext) override;

public:
    ui64 GetPortionsToMoveCount() const {
        ui64 count = 0;
        for (auto& [addr, ids] : PortionsToMove) {
            count += ids.size();
        }
        return count;
    }

    void Refresh(const TAddExternalContext& externalContext);

    explicit TMoveDataActualizer(const TVersionedIndex& versionedIndex)
        : VersionedIndex(versionedIndex)
    {
    }
};

}   // namespace NKikimr::NOlap::NActualizer
