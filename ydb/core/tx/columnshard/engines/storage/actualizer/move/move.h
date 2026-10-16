#pragma once

#include <ydb/core/tx/columnshard/engines/column_engine.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/storage/actualizer/abstract/abstract.h>
#include <ydb/core/tx/columnshard/engines/storage/actualizer/move/queue_sizes.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/map.h>

#include <array>

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
    enum class EPortionState {
        Pending,
        Ready,
        InFlight,
        Uncommitted,
        Retired,
        Count,
    };
    using TPortions = TMap<ui64, EPortionState>;
    // Seeded once. New writes cannot use the inactive target groups; rejected and cleaned-up ids never return.
    TPortions Portions;
    std::array<ui64, static_cast<size_t>(EPortionState::Count)> StateCounts = {};
    ui64 RejectedPortions = 0;
    // Stable cursors bound each pass; erased candidates do not leave empty hash buckets to scan.
    // ErasePortion advances either cursor if it points at the erased entry.
    TPortions::iterator RefreshPosition;
    TPortions::iterator RewritePosition;
    static constexpr size_t PortionsPerPass = 1024;

    TPortions::iterator NextPortion(TPortions::iterator& position);
    void SetState(EPortionState& state, EPortionState next);
    void ErasePortion(TPortions::iterator it);

protected:
    // Progress is read from the seeded portions; normal index notifications do not change session membership.
    void DoAddPortion(const TPortionInfo&, const TAddExternalContext&) override {
    }

    void DoRemovePortion(ui64) override {
    }

    virtual void DoExtractTasks(
        TTieringProcessContext& tasksContext, const TExternalTasksContext& externalContext, TInternalTasksContext& internalContext) override;

public:
    // Split out to be testable without a TPortionDataAccessor, which needs arrow-backed metadata.
    static bool HasBlobInGroups(const std::vector<TUnifiedBlobId>& blobIds, const THashSet<ui32>& groups);

    void ActualizePortionInfo(const TPortionDataAccessor& accessor);

    // Asks for every pending portion: the caller runs it only while no move request is in flight.
    std::vector<TCSMetadataRequest> BuildMoveDataMetadataRequests(const THashMap<ui64, TPortionInfo::TPtr>& portions,
        const THashMap<ui64, std::shared_ptr<TWrittenPortionInfo>>& uncommitted, const std::shared_ptr<TMoveDataActualizer>& self);

    // Reconcile a bounded batch with the current catalogue, including commits, retirements, and cleanup.
    void RefreshPortions(
        const THashMap<ui64, TPortionInfo::TPtr>& portions, const THashMap<ui64, std::shared_ptr<TWrittenPortionInfo>>& uncommitted);

    TMoveDataQueueSizes GetMoveDataQueueSizes() const;

    // Once, right after construction: a new target set gets a new actualizer.
    void Seed(const TAddExternalContext& externalContext, const THashMap<ui64, std::shared_ptr<TWrittenPortionInfo>>& uncommitted);

    TMoveDataActualizer(const THashSet<ui32>& targetGroups, const TVersionedIndex& versionedIndex)
        : TargetGroups(targetGroups)
        , VersionedIndex(versionedIndex)
    {
    }
};

}   // namespace NKikimr::NOlap::NActualizer
