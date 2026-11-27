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
    AFL_VERIFY(portion->second == EPortionState::Finished || portion->second == EPortionState::Rejected);
    const auto next = std::next(portion);
    for (auto* cursor : { &StateUpdateCursor, &RewriteCursor, &MetadataCursor }) {
        if (*cursor == portion) {
            *cursor = next;
        }
    }
    PortionsById.erase(portion->first);
    Portions.erase(portion);
    // Terminal counters are cumulative; only active states count towards the completion gate.
}

TMoveDataQueueSizes TPortionQueue::GetSizes() const {
    return TMoveDataQueueSizes{
        .Pending = Count(EPortionState::Pending),
        .ConfirmedToMove = Count(EPortionState::Ready),
        .RewriteStarted = Count(EPortionState::RewriteStarted),
        .Uncommitted = Count(EPortionState::Uncommitted),
        .Retired = Count(EPortionState::Retired),
        .Rejected = Count(EPortionState::Rejected),
    };
}

void TPortionQueue::Add(ui64 portionId) {
    const auto entry = Portions.emplace(Portions.end(), portionId, EPortionState::Pending);
    AFL_VERIFY(PortionsById.emplace(portionId, entry).second);
    ++Count(EPortionState::Pending);
}

}   // namespace NKikimr::NOlap::NActualizer
