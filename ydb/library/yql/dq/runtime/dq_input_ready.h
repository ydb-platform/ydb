#pragma once

#include <util/system/types.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <vector>

namespace NYql::NDq {

// Tells a union of inputs which of them may have something for it, so that it does not poll every input to find
// most of them empty. An input opts in with IDqInput::BindReadySet and marks its slot whenever it may have
// become non-empty or finished; the union visits the marked inputs only.
//
// The consumer clears the mark of a slot before it looks at the input, and the producer makes its data visible
// before it marks, both seq_cst: either the consumer sees the data, or the producer sees the mark cleared and
// marks the slot again. Marks tell which inputs to look at, never whether to wake the consumer up: that stays
// with the inputs, which a union looks at until one reports itself empty (and so asks to be woken up).
class TDqInputReadySet {
public:
    explicit TDqInputReadySet(ui32 size);

    // any thread
    void Mark(ui32 slot);

    // the consumer only: appends the slots marked since the last call
    void Take(std::vector<ui32>& slots);
    // the consumer only: before it looks at the input
    void Clear(ui32 slot);

    ui32 Size() const {
        return Marked.size();
    }

private:
    std::vector<std::atomic<bool>> Marked;
    // set once a slot is queued, so that the consumer takes the Mutex only when there is something to take
    std::atomic<bool> Pending = false;
    std::mutex Mutex;
    std::vector<ui32> Queue;
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
