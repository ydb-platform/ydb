#pragma once

#include <ydb/core/tx/columnshard/engines/storage/actualizer/common/queue_sizes.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>

#include <array>
#include <list>
#include <utility>

namespace NKikimr::NOlap::NActualizer {

class TPortionQueue {
public:
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

private:
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
    TPortionQueue() = default;
    TPortionQueue(const TPortionQueue&) = delete;
    TPortionQueue& operator=(const TPortionQueue&) = delete;

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

}   // namespace NKikimr::NOlap::NActualizer
