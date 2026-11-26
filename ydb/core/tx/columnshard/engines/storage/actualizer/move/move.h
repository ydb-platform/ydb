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

    class TPortionQueue {
        using TPortions = std::list<std::pair<ui64, EPortionState>>;
        TPortions Portions;
        THashMap<ui64, TPortions::iterator> PortionsById;
        std::array<ui64, static_cast<size_t>(EPortionState::Count)> StateCounts = {};
        TPortions::iterator StateUpdateCursor = Portions.end();
        TPortions::iterator RewriteCursor = Portions.end();
        TPortions::iterator MetadataCursor = Portions.end();

        ui64& Count(EPortionState state);
        const ui64& Count(EPortionState state) const;
        void ErasePortion(TPortions::iterator portion);

    public:
        void Add(ui64 portionId);
        void SetState(EPortionState& state, EPortionState next);
        TMoveDataQueueSizes GetSizes() const;

        bool HasPending() const {
            return Count(EPortionState::Pending) != 0;
        }

        template <class TClassifier>
        void ClassifyPending(ui64 portionId, TClassifier&& classify) {
            const auto found = PortionsById.find(portionId);
            if (found == PortionsById.end()) {
                return;
            }
            const auto entry = found->second;
            if (entry->second == EPortionState::Pending) {
                SetState(entry->second, classify());
                if (entry->second == EPortionState::Rejected) {
                    ErasePortion(entry);
                }
            }
        }

        template <class TVisitor>
        size_t ScanForStateUpdate(size_t limit, TVisitor&& visitor) {
            return Scan(StateUpdateCursor, limit, visitor);
        }

        template <class TVisitor>
        size_t ScanForRewrite(size_t limit, TVisitor&& visitor) {
            if (Count(EPortionState::Ready) == 0 && Count(EPortionState::RewriteStarted) == 0) {
                return 0;
            }
            return Scan(RewriteCursor, limit, visitor);
        }

        template <class TVisitor>
        size_t ScanForMetadata(size_t limit, TVisitor&& visitor) {
            return Scan(MetadataCursor, limit, visitor);
        }

    private:
        template <class TVisitor>
        size_t Scan(TPortions::iterator& position, size_t limit, TVisitor&& visitor) {
            const size_t count = Min(limit, Portions.size());
            for (size_t i = 0; i < count; ++i) {
                if (position == Portions.end()) {
                    position = Portions.begin();
                }
                const auto current = position++;
                const bool keepScanning = visitor(current->first, current->second);
                if (current->second == EPortionState::Finished || current->second == EPortionState::Rejected) {
                    ErasePortion(current);
                }
                if (!keepScanning) {
                    return i + 1;
                }
            }
            return count;
        }
    };

    TPortionQueue Queue;
    bool Seeded = false;
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
    void Seed(const TAddExternalContext& externalContext, const TWrittenPortionsById& uncommitted);

    TMoveDataActualizer(const TMoveDataActualizer&) = delete;
    TMoveDataActualizer& operator=(const TMoveDataActualizer&) = delete;

    TMoveDataActualizer(THashSet<ui32> targetGroups, const TVersionedIndex& versionedIndex)
        : TargetGroups(std::move(targetGroups))
        , VersionedIndex(versionedIndex)
    {
    }
};

}   // namespace NKikimr::NOlap::NActualizer
