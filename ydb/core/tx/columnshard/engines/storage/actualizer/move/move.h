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
}

namespace NKikimr::NOlap::NActualizer {

// Rewrites portions out of the given BS groups; the filter needs a loaded accessor for BlobIds.
class TMoveDataActualizer: public IActualizer {
private:
    const THashSet<ui32> TargetGroups;
    const TVersionedIndex& VersionedIndex;
    // Extended until AdmissionDeadline: new portions still land in the doomed group; unbounded, it never converges.
    THashSet<ui64> InitialPortionIds;
    TInstant AdmissionDeadline;
    // Portions waiting for accessor-load so we can check their DsGroup.
    THashSet<ui64> PendingPortionIds;
    // Portions confirmed to have blobs in TargetGroups; ready to be rewritten.
    THashMap<TRWAddress, THashSet<ui64>> PortionsToMove;
    THashMap<ui64, TRWAddress> PortionAddress;
    // Still counted: old blobs reach the delete queues only on commit, so the gate must wait.
    THashSet<ui64> InFlightPortionIds;
    // Pending portions with an unanswered accessor request; the expiry re-asks for a request that got lost.
    THashMap<ui64, TInstant> RequestedAt;
    ui64 RejectedPortions = 0;

    // Keeps InitialPortionIds intact so an aborted change can re-enter PendingPortionIds.
    void RemoveFromActiveQueue(ui64 portionId);

protected:
    virtual void DoAddPortion(const TPortionInfo& info, const TAddExternalContext& context) override;
    virtual void DoRemovePortion(const ui64 portionId) override;
    virtual void DoExtractTasks(
        TTieringProcessContext& tasksContext, const TExternalTasksContext& externalContext, TInternalTasksContext& internalContext) override;

public:
    // Split out to be testable without a TPortionDataAccessor, which needs arrow-backed metadata.
    static bool HasBlobInGroups(const std::vector<TUnifiedBlobId>& blobIds, const THashSet<ui32>& groups);

    void ActualizePortionInfo(const TPortionDataAccessor& accessor);

    // Called for every reply, errors included: a portion the reply could not resolve stays pending and is asked for again.
    void OnMetadataRequestAnswered(const std::vector<ui64>& portionIds);

protected:
    // Protected test helpers: unit tests subclass to reach them, production code cannot.
    void SimulateTaskSubmissionForTest(ui64 portionId) {
        RemoveFromActiveQueue(portionId);
        InFlightPortionIds.emplace(portionId);
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
        InFlightPortionIds.erase(portionId);
        PendingPortionIds.emplace(portionId);
    }

    void ConfirmPortionForTest(ui64 portionId) {
        PendingPortionIds.erase(portionId);
        TRWAddress addr({ IStoragesManager::DefaultStorageId }, { IStoragesManager::DefaultStorageId });
        PortionsToMove[addr].emplace(portionId);
        PortionAddress.emplace(portionId, std::move(addr));
    }

public:
    std::vector<TCSMetadataRequest> BuildMoveDataMetadataRequests(
        const THashMap<ui64, TPortionInfo::TPtr>& portions, const std::shared_ptr<TMoveDataActualizer>& self, const TInstant now);

    TMoveDataQueueSizes GetMoveDataQueueSizes() const;

    ui64 GetMoveDataPortionsCount() const {
        return GetMoveDataQueueSizes().GetTotal();
    }

    static constexpr TDuration AdmissionWindow = TDuration::Minutes(10);
    static constexpr TDuration MetadataRequestExpiry = TDuration::Minutes(5);

    void Refresh(const TAddExternalContext& externalContext);

    TMoveDataActualizer(const THashSet<ui32>& targetGroups, const TVersionedIndex& versionedIndex)
        : TargetGroups(targetGroups)
        , VersionedIndex(versionedIndex)
    {
    }
};

}   // namespace NKikimr::NOlap::NActualizer
