#pragma once

#include "portion_queue.h"

#include <ydb/core/tx/columnshard/engines/column_engine.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/storage/actualizer/abstract/abstract.h>
#include <ydb/core/tx/columnshard/engines/storage/actualizer/common/queue_sizes.h>
#include <ydb/core/tx/columnshard/engines/storage/actualizer/move/metadata.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

#include <utility>

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
    TPortionQueue Queue;
    bool PortionQueueInitialized = false;
    static constexpr size_t PortionsPerPass = 1024;

protected:
    // Progress is read from the seeded portions; normal index notifications do not change session membership.
    void DoAddPortion(const TPortionInfo&, const TAddExternalContext&) override {
    }

    void DoRemovePortion(ui64) override {
    }

    virtual void DoExtractTasks(
        TTieringProcessContext& tasksContext, const TExternalTasksContext& externalContext, TInternalTasksContext& internalContext) override;

public:
    static bool HasBlobInGroups(const std::vector<TUnifiedBlobId>& blobIds, const THashSet<ui32>& groups);

    void ActualizePortionInfo(const TPortionDataAccessor& accessor);

    // Resume a bounded metadata pass after the preceding requests finish.
    std::vector<TCSMetadataRequest> BuildMoveDataMetadataRequests(const THashMap<ui64, TPortionInfo::TPtr>& portions,
        const TWrittenPortionsById& uncommitted, const std::shared_ptr<TMoveDataActualizer>& self,
        TMoveDataMetadataBudget& budget);

    // Update a bounded batch from current portion state, including commits, retirement, and cleanup.
    size_t UpdatePortionStates(
        const THashMap<ui64, TPortionInfo::TPtr>& portions, const TWrittenPortionsById& uncommitted, size_t limit = PortionsPerPass);

    TMoveDataQueueSizes GetMoveDataQueueSizes() const;

    // Once, right after construction: a new target set gets a new actualizer.
    void InitializePortionQueue(const TAddExternalContext& externalContext, const TWrittenPortionsById& uncommitted);

    TMoveDataActualizer(const TMoveDataActualizer&) = delete;
    TMoveDataActualizer& operator=(const TMoveDataActualizer&) = delete;

    TMoveDataActualizer(THashSet<ui32> targetGroups, const TVersionedIndex& versionedIndex)
        : TargetGroups(std::move(targetGroups))
        , VersionedIndex(versionedIndex)
    {
    }
};

}   // namespace NKikimr::NOlap::NActualizer
