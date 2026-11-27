#include "portion_queue.h"

#include <ydb/library/actors/core/log.h>

#include <iterator>

namespace NKikimr::NOlap::NActualizer {

ui64& TPortionQueue::Count(EPortionState state) {
    return const_cast<ui64&>(std::as_const(*this).Count(state));
}

const ui64& TPortionQueue::Count(EPortionState state) const {
    return StateCounts[static_cast<size_t>(state)];
}

void TPortionQueue::SetState(EPortionState& state, EPortionState next) {
    AFL_VERIFY(Count(state));
    --Count(state);
    ++Count(next);
    state = next;
}

void TPortionQueue::ErasePortion(TPortions::iterator portion) {
    AFL_VERIFY(portion->second == EPortionState::Finished);
    const auto next = std::next(portion);
    for (auto* cursor : { &StateUpdateCursor, &RewriteCursor }) {
        if (*cursor == portion) {
            *cursor = next;
        }
    }
    Portions.erase(portion);
    // Terminal counters are cumulative; only active states count towards the completion gate.
}

TMoveDataQueueSizes TPortionQueue::GetSizes() const {
    return TMoveDataQueueSizes{
        .ConfirmedToMove = Count(EPortionState::Ready),
        .RewriteStarted = Count(EPortionState::RewriteStarted),
        .Uncommitted = Count(EPortionState::Uncommitted),
    };
}

void TPortionQueue::Add(ui64 portionId, EPortionState state) {
    Portions.emplace_back(portionId, state);
    ++Count(state);
}

}   // namespace NKikimr::NOlap::NActualizer
