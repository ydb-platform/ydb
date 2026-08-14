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
    const THashSet<ui32> TargetGroups;   // groups whose blobs must be rewritten
    const TVersionedIndex& VersionedIndex;
    // Snapshot of portionIds captured at Refresh(): prevents rewritten portions
    // (which get fresh IDs) from re-entering the work queue.
    THashSet<ui64> InitialPortionIds;
    // Portions waiting for accessor-load so we can check their DsGroup.
    THashSet<ui64> PendingPortionIds;
    // Portions confirmed to have blobs in TargetGroups; ready to be rewritten.
    THashMap<TRWAddress, THashSet<ui64>> PortionsToMove;
    THashMap<ui64, TRWAddress> PortionAddress;

protected:
    virtual void DoAddPortion(const TPortionInfo& info, const TAddExternalContext& context) override;
    virtual void DoRemovePortion(const ui64 portionId) override;
    virtual void DoExtractTasks(
        TTieringProcessContext& tasksContext, const TExternalTasksContext& externalContext, TInternalTasksContext& internalContext) override;

public:
    // Called from IMetadataAccessorResultProcessor when accessor data arrives.
    void ActualizePortionInfo(const TPortionDataAccessor& accessor);

    // Build accessor-load requests for all pending portions.
    std::vector<TCSMetadataRequest> BuildMoveDataMetadataRequests(
        const THashMap<ui64, TPortionInfo::TPtr>& portions, const std::shared_ptr<TMoveDataActualizer>& self);

    // Total work remaining: pending validation + confirmed-but-not-yet-rewritten.
    ui64 GetPortionsToMoveCount() const;

    // Reset: capture a new snapshot of all default-tier portion IDs.
    void Refresh(const TAddExternalContext& externalContext);

    TMoveDataActualizer(const THashSet<ui32>& targetGroups, const TVersionedIndex& versionedIndex)
        : TargetGroups(targetGroups)
        , VersionedIndex(versionedIndex)
    {
    }
};

}   // namespace NKikimr::NOlap::NActualizer
