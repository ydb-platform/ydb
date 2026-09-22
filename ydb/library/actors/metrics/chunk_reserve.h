#pragma once

#include <util/generic/vector.h>
#include <util/system/yassert.h>

#include <atomic>

namespace NActors {
    struct TChunk;

    // One manager produces, one line writer consumes. The consumer role may
    // transfer to the manager only after the writer publishes Status=Closed.
    class TChunkReserve {
    public:
        explicit TChunkReserve(ui32 capacity)
            : Slots(capacity)
        {
            Y_ABORT_UNLESS(capacity);
        }

        bool Full() const noexcept {
            return Write.load(std::memory_order_relaxed) - Read.load(std::memory_order_acquire) == Slots.size();
        }

        bool TryPush(TChunk* chunk) noexcept {
            const ui64 write = Write.load(std::memory_order_relaxed);
            if (write - Read.load(std::memory_order_acquire) == Slots.size()) {
                return false;
            }
            Slots[WriteSlot] = chunk;
            WriteSlot = (WriteSlot + 1) % Slots.size();
            Write.store(write + 1, std::memory_order_release);
            return true;
        }

        TChunk* TryPop() noexcept {
            const ui64 read = Read.load(std::memory_order_relaxed);
            if (read == Write.load(std::memory_order_acquire)) {
                return nullptr;
            }
            TChunk* chunk = Slots[ReadSlot];
            ReadSlot = (ReadSlot + 1) % Slots.size();
            Read.store(read + 1, std::memory_order_release);
            return chunk;
        }

    private:
        TVector<TChunk*> Slots;
        // Separate slot cursors also work when the unsigned counters wrap and
        // capacity is not a power of two.
        size_t WriteSlot = 0;
        std::atomic<ui64> Write = 0;
        size_t ReadSlot = 0;
        std::atomic<ui64> Read = 0;
    };
} // namespace NActors
