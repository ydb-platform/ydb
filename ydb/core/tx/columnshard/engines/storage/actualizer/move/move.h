#pragma once

#include <ydb/core/tx/columnshard/engines/column_engine.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/storage/actualizer/abstract/abstract.h>
#include <ydb/core/tx/columnshard/engines/storage/actualizer/common/address.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

namespace NKikimr::NOlap {
class TPortionDataAccessor;
}

namespace NKikimr::NOlap::NActualizer {

// Actualizer that rewrites blob portions residing in specified BS groups into
// currently-active groups.  The group filter is applied asynchronously once
// the portion accessor (with real BlobIds) has been loaded.
class TMoveDataActualizer: public IActualizer {
private:
    const THashSet<ui32> TargetGroups;
    const TVersionedIndex& VersionedIndex;
    // Snapshot of portionIds captured at Refresh(): prevents newly-written portions
    // (which get fresh IDs) from re-entering the work queue infinitely.
    THashSet<ui64> InitialPortionIds;
    // Portions waiting for accessor-load so we can check their DsGroup.
    THashSet<ui64> PendingPortionIds;
    // Portions confirmed to have blobs in TargetGroups; ready to be rewritten.
    THashMap<TRWAddress, THashSet<ui64>> PortionsToMove;
    THashMap<ui64, TRWAddress> PortionAddress;

    // Remove from PortionsToMove/PortionAddress only; keeps InitialPortionIds intact
    // so the portion can re-enter PendingPortionIds if the change is aborted.
    void RemoveFromActiveQueue(ui64 portionId);

protected:
    virtual void DoAddPortion(const TPortionInfo& info, const TAddExternalContext& context) override;
    virtual void DoRemovePortion(const ui64 portionId) override;
    virtual void DoExtractTasks(
        TTieringProcessContext& tasksContext, const TExternalTasksContext& externalContext, TInternalTasksContext& internalContext) override;

public:
    void ActualizePortionInfo(const TPortionDataAccessor& accessor);

    // Test helpers — exercise internal state without a full TTieringProcessContext.
    void SimulateTaskSubmissionForTest(ui64 portionId) {
        RemoveFromActiveQueue(portionId);
    }

    bool IsInInitialPortionIds(ui64 portionId) const {
        return InitialPortionIds.contains(portionId);
    }

    bool IsInPendingPortionIds(ui64 portionId) const {
        return PendingPortionIds.contains(portionId);
    }

    bool IsInPortionsToMove(ui64 portionId) const {
        return PortionAddress.contains(portionId);
    }

    void AddToInitialAndPendingForTest(ui64 portionId) {
        InitialPortionIds.emplace(portionId);
        PendingPortionIds.emplace(portionId);
    }

    // Directly confirm a pending portion into PortionsToMove using a placeholder address.
    void ConfirmPortionForTest(ui64 portionId) {
        PendingPortionIds.erase(portionId);
        TRWAddress addr({ IStoragesManager::DefaultStorageId }, { IStoragesManager::DefaultStorageId });
        PortionsToMove[addr].emplace(portionId);
        PortionAddress.emplace(portionId, std::move(addr));
    }

    std::vector<TCSMetadataRequest> BuildMoveDataMetadataRequests(
        const THashMap<ui64, TPortionInfo::TPtr>& portions, const std::shared_ptr<TMoveDataActualizer>& self) const;

    ui64 GetMoveDataPortionsCount() const;

    void Refresh(const TAddExternalContext& externalContext);

    TMoveDataActualizer(const THashSet<ui32>& targetGroups, const TVersionedIndex& versionedIndex)
        : TargetGroups(targetGroups)
        , VersionedIndex(versionedIndex)
    {
    }
};

}   // namespace NKikimr::NOlap::NActualizer
