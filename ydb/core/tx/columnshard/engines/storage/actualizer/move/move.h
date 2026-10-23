#pragma once

#include <ydb/core/tx/columnshard/engines/column_engine.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/storage/actualizer/abstract/abstract.h>
#include <ydb/core/tx/columnshard/engines/storage/actualizer/common/queue_sizes.h>
#include <ydb/core/tx/columnshard/engines/storage/actualizer/move/metadata.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

#include <array>
#include <iterator>
#include <list>
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
    enum class EPortionState {
        Pending,   // Blob groups have not been classified yet.
        Ready,
        RewriteStarted,   // A target-group rewrite was scheduled; retry after abort, wait for retirement after success.
        Uncommitted,
        Retired,
        Finished,
        Rejected,
        Count,
    };
    using TPortions = std::list<std::pair<ui64, EPortionState>>;
    // Seeded once: new writes cannot use inactive target groups. Terminal candidates are erased, never readopted.
    TPortions Portions;
    THashMap<ui64, TPortions::iterator> PortionsById;
    bool Seeded = false;
    std::array<ui64, static_cast<size_t>(EPortionState::Count)> StateCounts = {};
    // List traversal stays constant-time per candidate after erasure; ErasePortion repairs all cursors.
    TPortions::iterator StateScanPosition = Portions.end();
    TPortions::iterator RewriteScanPosition = Portions.end();
    TPortions::iterator MetadataScanPosition = Portions.end();
    static constexpr size_t PortionsPerPass = 1024;

    // Owns empty-list checks, the pass bound, wraparound and terminal erasure; callers only process valid entries.
    template <class TVisitor>
    void ScanPortions(TPortions::iterator& position, size_t limit, TVisitor&& visitor) {
        const size_t count = Min(limit, Portions.size());
        for (size_t i = 0; i < count; ++i) {
            if (position == Portions.end()) {
                position = Portions.begin();
                AFL_VERIFY(position != Portions.end());
            }
            const auto current = position++;
            const bool keepScanning = visitor(current->first, current->second);
            if (current->second == EPortionState::Finished || current->second == EPortionState::Rejected) {
                ErasePortion(current);
            }
            if (!keepScanning) {
                return;
            }
        }
    }

    void SetState(EPortionState& state, EPortionState next);
    void ErasePortion(TPortions::iterator portion);

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

    // Resume a bounded metadata pass after the preceding requests finish.
    std::vector<TCSMetadataRequest> BuildMoveDataMetadataRequests(const THashMap<ui64, TPortionInfo::TPtr>& portions,
        const THashMap<ui64, std::shared_ptr<TWrittenPortionInfo>>& uncommitted, const std::shared_ptr<TMoveDataActualizer>& self,
        TMoveDataMetadataBudget& budget);

    // Update a bounded batch from current portion state, including commits, retirement, and cleanup.
    void UpdatePortionStates(
        const THashMap<ui64, TPortionInfo::TPtr>& portions, const THashMap<ui64, std::shared_ptr<TWrittenPortionInfo>>& uncommitted);

    TMoveDataQueueSizes GetMoveDataQueueSizes() const;

    // Once, right after construction: a new target set gets a new actualizer.
    void Seed(const TAddExternalContext& externalContext, const THashMap<ui64, std::shared_ptr<TWrittenPortionInfo>>& uncommitted);

    TMoveDataActualizer(const TMoveDataActualizer&) = delete;
    TMoveDataActualizer& operator=(const TMoveDataActualizer&) = delete;

    TMoveDataActualizer(THashSet<ui32> targetGroups, const TVersionedIndex& versionedIndex)
        : TargetGroups(std::move(targetGroups))
        , VersionedIndex(versionedIndex)
    {
    }
};

}   // namespace NKikimr::NOlap::NActualizer
