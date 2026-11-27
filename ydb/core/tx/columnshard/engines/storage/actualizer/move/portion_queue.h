#pragma once

#include <ydb/core/tx/columnshard/engines/storage/actualizer/common/queue_sizes.h>

#include <util/generic/algorithm.h>

#include <array>
#include <list>
#include <utility>

namespace NKikimr::NOlap::NActualizer {

class TPortionQueue {
public:
    enum class EPortionState {
        Ready,
        RewriteStarted,   // A target-group rewrite was scheduled; retry after abort, wait for retirement after success.
        Uncommitted,
        Finished,
        Count,
    };

private:
    using TPortions = std::list<std::pair<ui64, EPortionState>>;
    TPortions Portions;
    std::array<ui64, static_cast<size_t>(EPortionState::Count)> StateCounts = {};
    TPortions::iterator StateUpdateCursor = Portions.end();
    TPortions::iterator RewriteCursor = Portions.end();

    ui64& Count(EPortionState state);
    const ui64& Count(EPortionState state) const;
    void ErasePortion(TPortions::iterator portion);

public:
    TPortionQueue() = default;
    TPortionQueue(const TPortionQueue&) = delete;
    TPortionQueue& operator=(const TPortionQueue&) = delete;

    void Add(ui64 portionId, EPortionState state);
    void SetState(EPortionState& state, EPortionState next);
    TMoveDataQueueSizes GetSizes() const;

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
            if (current->second == EPortionState::Finished) {
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
