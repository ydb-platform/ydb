#include "dq_input_ready.h"

#include <util/system/yassert.h>

namespace NYql::NDq {

TDqInputReadySet::TDqInputReadySet(ui32 size)
    : Marked(size)
{
    Queue.reserve(size);
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
    // after the slot is queued: a Take which finds Pending set finds the slot, or a later one does
    Pending.store(true);
}

void TDqInputReadySet::Take(std::vector<ui32>& slots) {
    if (!Pending.load() || !Pending.exchange(false)) {
        return;
    }
    std::lock_guard lock(Mutex);
    slots.insert(slots.end(), Queue.begin(), Queue.end());
    Queue.clear();
}

void TDqInputReadySet::Clear(ui32 slot) {
    Y_DEBUG_ABORT_UNLESS(slot < Marked.size());
    // a store, seq_cst, which may be followed by a load of the state of the input: see the class comment
    Marked[slot].store(false);
}

} // namespace NYql::NDq
