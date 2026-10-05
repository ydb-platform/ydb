#pragma once

#include <util/system/types.h>

#include <atomic>
#include <deque>
#include <memory>
#include <mutex>
#include <vector>

namespace NYql::NDq {

// Tells a union of inputs which of them may have something for it, so that it does not poll every input to find
// most of them empty. An input supports it with IDqInput::BindReadySet and marks its slot whenever it may have
// become non-empty or finished; the union visits the inputs the set hands out.
//
// The producers mark from any thread. The consumer (the union, single threaded) collects the marks into a queue
// of ready slots and visits them in turn:
//     set.Collect();
//     for (auto count = set.ReadyCount(); count > 0; --count) {
//         auto slot = set.Next();          // the mark is cleared here, before the input is looked at
//         if (/* the input had data */) { set.Keep(slot); ... }
//         else if (/* finished */) { set.Retire(slot); }
//         else { set.Release(slot); }      // found empty: comes back when marked again
//     }
//
// Next clears the mark before the input is looked at, and a producer makes its data visible before it marks, both
// seq_cst: either the consumer sees the data, or the producer sees the mark cleared and marks the slot again. Marks
// tell which inputs to look at, never whether to wake the consumer up: that stays with the inputs, which a union
// looks at until one reports itself empty (and so asks to be woken up).
class TDqInputReadySet {
public:
    explicit TDqInputReadySet(ui32 size);

    // any thread
    void Mark(ui32 slot);

    // The consumer only
    // moves the slots marked since the last call into the ready queue, unless there already or retired
    void Collect();
    size_t ReadyCount() const {
        return Ready.size();
    }
    // takes the slot at the front of the ready queue, and clears its mark: the input may be looked at now
    ui32 Next();
    // the input had data: back to the tail of the ready queue, as it may have more
    void Keep(ui32 slot);
    // the input was found empty: out of the ready queue until marked again
    void Release(ui32 slot);
    // the input finished: its marks are ignored from now on
    void Retire(ui32 slot);

    ui32 Size() const {
        return Marked.size();
    }

private:
    enum class ESlotState : ui8 {
        Idle,       // waits for a mark
        Ready,      // in Ready, or taken by Next and not given back yet
        Retired,
    };

    // producers
    std::vector<std::atomic<bool>> Marked;
    // set once a slot is queued, so that Collect takes the Mutex only when there is something to take
    std::atomic<bool> Pending = false;
    std::mutex Mutex;
    std::vector<ui32> Queue;

    // the consumer
    std::vector<ESlotState> States;
    std::deque<ui32> Ready;
    std::vector<ui32> Taken;
};

// What an input keeps to mark itself, empty when the input is not bound to a union
struct TDqInputReadyHook {
    std::shared_ptr<TDqInputReadySet> Set;
    ui32 Slot = 0;

    void Mark() const {
        if (Set) {
            Set->Mark(Slot);
        }
    }

    explicit operator bool() const {
        return static_cast<bool>(Set);
    }
};

} // namespace NYql::NDq
