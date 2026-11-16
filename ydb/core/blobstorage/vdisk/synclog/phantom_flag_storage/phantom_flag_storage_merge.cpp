#include "phantom_flag_storage_merge.h"

#include <util/system/yassert.h>

#include <algorithm>
#include <functional>
#include <utility>

namespace NKikimr::NSyncLog {

std::span<TLogoBlobID> SortPhantomFlagKeys(std::span<TLogoBlobID> keys) {
    if (keys.size() < 2) {
        return keys;
    }
    std::sort(keys.begin(), keys.end());
    const auto uniqueEnd = std::unique(keys.begin(), keys.end());
    return keys.first(static_cast<size_t>(uniqueEnd - keys.begin()));
}

TPhantomFlagRunMerger::TPhantomFlagRunMerger(size_t inputCount)
    : Inputs(inputCount)
{
    Heap.reserve(inputCount);
}

bool TPhantomFlagRunMerger::TInputHead::operator>(const TInputHead& other) const {
    return other.Key < Key;
}

std::optional<size_t> TPhantomFlagRunMerger::GetInputToRefill() const {
    if (NextInitialInput < Inputs.size()) {
        return NextInitialInput;
    }
    return RefillInput;
}

void TPhantomFlagRunMerger::SetInputBlock(size_t inputIndex, std::vector<TLogoBlobID> keys, bool eof) {
    Y_ABORT_UNLESS(GetInputToRefill() == inputIndex);
    if (keys.empty() && !eof) {
        return;
    }
    if (NextInitialInput < Inputs.size()) {
        ++NextInitialInput;
    } else {
        RefillInput.reset();
    }

    if (!keys.empty()) {
        Heap.push_back({keys.front(), inputIndex});
        std::push_heap(Heap.begin(), Heap.end(), std::greater<>{});
        Inputs[inputIndex] = {.Keys = std::move(keys), .Eof = eof};
    }
}

TPhantomFlagRunMerger::TProgress TPhantomFlagRunMerger::Merge(std::span<TLogoBlobID> output) {
    TProgress progress;
    if (NextInitialInput < Inputs.size()) {
        progress.InputToRefill = NextInitialInput;
        return progress;
    }
    // An unread key can precede or match the heap minimum.
    while (!RefillInput && !Heap.empty() && progress.Written < output.size()) {
        std::pop_heap(Heap.begin(), Heap.end(), std::greater<>{});
        const TInputHead head = Heap.back();
        Heap.pop_back();
        if (LastEmittedKey != head.Key) {
            output[progress.Written++] = head.Key;
            LastEmittedKey = head.Key;
        }

        TInputBlock& block = Inputs[head.InputIndex];
        ++block.Offset;
        if (block.Offset < block.Keys.size()) {
            Heap.push_back({block.Keys[block.Offset], head.InputIndex});
            std::push_heap(Heap.begin(), Heap.end(), std::greater<>{});
        } else {
            if (!block.Eof) {
                RefillInput = head.InputIndex;
            }
            block = TInputBlock{};
        }
    }
    progress.InputToRefill = RefillInput;
    progress.Finished = !RefillInput && Heap.empty();
    return progress;
}

} // namespace NKikimr::NSyncLog
