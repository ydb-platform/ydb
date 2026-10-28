#include "dq_input_ready.h"

#include <util/system/yassert.h>

namespace NYql::NDq {

TDqInputReadySet::TDqInputReadySet(ui32 size)
    : Marked(size)
    , States(size, ESlotState::Idle)
{
    for (auto& marked : Marked) {
        marked.store(false, std::memory_order_relaxed);
    }
    Queue.reserve(size);
    Taken.reserve(size);
}

void TDqInputReadySet::Mark(ui32 slot) {
    Y_DEBUG_ABORT_UNLESS(slot < Marked.size());
    // loaded first: an input marks itself on every push, and it is marked already most of the time
    if (Marked[slot].load() || Marked[slot].exchange(true)) {
        return;
    }
    {
        std::lock_guard lock(Mutex);
        Queue.push_back(slot);
    }
    // after the slot is queued: a Collect which finds Pending set finds the slot, or a later one does
    Pending.store(true);
}

void TDqInputReadySet::Collect() {
    if (!Pending.load() || !Pending.exchange(false)) {
        return;
    }
    Taken.clear();
    {
        std::lock_guard lock(Mutex);
        Taken.swap(Queue);
    }
    for (auto slot : Taken) {
        if (States[slot] == ESlotState::Idle) {
            States[slot] = ESlotState::Ready;
            Ready.push_back(slot);
        }
    }
}

ui32 TDqInputReadySet::Next() {
    Y_DEBUG_ABORT_UNLESS(!Ready.empty());
    auto slot = Ready.front();
    Ready.pop_front();
    // a store, seq_cst, followed by the load of the state of the input: see the class comment
    Marked[slot].store(false);
    return slot;
}

void TDqInputReadySet::Keep(ui32 slot) {
    Y_DEBUG_ABORT_UNLESS(States[slot] == ESlotState::Ready);
    Ready.push_back(slot);
}

void TDqInputReadySet::Release(ui32 slot) {
    Y_DEBUG_ABORT_UNLESS(States[slot] == ESlotState::Ready);
    States[slot] = ESlotState::Idle;
}

void TDqInputReadySet::Retire(ui32 slot) {
    Y_DEBUG_ABORT_UNLESS(States[slot] == ESlotState::Ready);
    States[slot] = ESlotState::Retired;
}

} // namespace NYql::NDq
