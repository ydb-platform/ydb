#pragma once

#include <ydb/core/tx/columnshard/engines/column_engine.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/storage/actualizer/abstract/abstract.h>
#include <ydb/core/tx/columnshard/engines/storage/actualizer/common/address.h>
#include <ydb/core/tx/columnshard/engines/storage/actualizer/move/queue_sizes.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

namespace NKikimr::NOlap {
class TPortionDataAccessor;
class TWrittenPortionInfo;
}   // namespace NKikimr::NOlap

namespace NKikimr::NOlap::NActualizer {

// Rewrites portions out of the given BS groups; the filter needs a loaded accessor for BlobIds.
class TMoveDataActualizer: public IActualizer {
private:
    const THashSet<ui32> TargetGroups;
    const TVersionedIndex& VersionedIndex;
    // Fixed when seeded: Handle(TEvMoveData) rejects live groups, so a portion created afterwards cannot hold a target blob.
    THashSet<ui64> InitialPortionIds;
    // Portions waiting for accessor-load so we can check their DsGroup.
    THashSet<ui64> PendingPortionIds;
    // Portions confirmed to have blobs in TargetGroups; ready to be rewritten.
    THashMap<TRWAddress, THashSet<ui64>> PortionsToMove;
    THashMap<ui64, TRWAddress> PortionAddress;
    // Still counted: old blobs reach the delete queues only on commit, so the gate must wait.
    THashSet<ui64> InFlightPortionIds;
    // Seeded portions that left the index; the gate waits until cleanup erases them from the granule.
    THashSet<ui64> RetiredPortionIds;
    // Uncommitted at the session start: a commit hands them to the normal path, an abort drops them.
    THashSet<ui64> UncommittedPortionIds;
    // Uncommitted portions with blobs in TargetGroups, held until the write commits or aborts.
    THashSet<ui64> UncommittedOnTarget;
    ui64 RejectedPortions = 0;

    // Keeps InitialPortionIds intact so an aborted change can re-enter PendingPortionIds.
    void RemoveFromActiveQueue(ui64 portionId);

protected:
    // Needed only for bookkeeping of the seeded set (a move finishing, failing, or a seeded write committing); nothing new is adopted, since new portions land in active groups.
    virtual void DoAddPortion(const TPortionInfo& info, const TAddExternalContext& context) override;
    virtual void DoRemovePortion(const ui64 portionId) override;
    virtual void DoExtractTasks(
        TTieringProcessContext& tasksContext, const TExternalTasksContext& externalContext, TInternalTasksContext& internalContext) override;

public:
    // Split out to be testable without a TPortionDataAccessor, which needs arrow-backed metadata.
    static bool HasBlobInGroups(const std::vector<TUnifiedBlobId>& blobIds, const THashSet<ui32>& groups);

    void ActualizePortionInfo(const TPortionDataAccessor& accessor);

    // Asks for every pending portion: the caller runs it only while no move request is in flight.
    std::vector<TCSMetadataRequest> BuildMoveDataMetadataRequests(const THashMap<ui64, TPortionInfo::TPtr>& portions,
        const THashMap<ui64, std::shared_ptr<TWrittenPortionInfo>>& uncommitted, const std::shared_ptr<TMoveDataActualizer>& self);

    // Drop cleaned-up retired ids; a retired id still in the granule's maps awaits cleanup.
    TMoveDataQueueSizes GetMoveDataQueueSizes(
        const THashMap<ui64, TPortionInfo::TPtr>& portions, const THashMap<ui64, std::shared_ptr<TWrittenPortionInfo>>& uncommitted);

    // Once, right after construction: a new target set gets a new actualizer.
    void Seed(const TAddExternalContext& externalContext, const THashMap<ui64, std::shared_ptr<TWrittenPortionInfo>>& uncommitted);

    TMoveDataActualizer(const THashSet<ui32>& targetGroups, const TVersionedIndex& versionedIndex)
        : TargetGroups(targetGroups)
        , VersionedIndex(versionedIndex)
    {
    }
};

}   // namespace NKikimr::NOlap::NActualizer
