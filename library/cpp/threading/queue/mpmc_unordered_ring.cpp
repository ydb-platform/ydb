#include "mpmc_unordered_ring.h"

namespace NThreading {
    TMPMCUnorderedRing::TMPMCUnorderedRing(size_t size) {
        Y_ABORT_UNLESS(size > 0);
        RingSize = size;
        RingBuffer.Reset(new void*[size]);
        memset(&RingBuffer[0], 0, sizeof(void*) * size);
    }

    bool TMPMCUnorderedRing::Push(void* msg, ui16 retryCount) noexcept {
        if (retryCount == 0) {
            StubbornPush(msg);
            return true;
        }
        for (ui16 itry = retryCount; itry-- > 0;) {
            if (WeakPush(msg)) {
                return true;
            }
        }
        return false;
    }

    bool TMPMCUnorderedRing::WeakPush(void* msg) noexcept {
        auto pawl = AtomicIncrement(WritePawl);
        if (pawl - AtomicGet(ReadFront) >= RingSize) {
            // Queue is full
            AtomicDecrement(WritePawl);
            return false;
        }

        auto writeSlot = AtomicGetAndIncrement(WriteFront);
        if (AtomicCas(&RingBuffer[writeSlot % RingSize], msg, nullptr)) {
            return true;
        }
        // slot is occupied for some reason, retry
        return false;
    }

    void* TMPMCUnorderedRing::Pop() noexcept {
        ui64 readSlot;

        for (ui16 itry = MAX_POP_TRIES; itry-- > 0;) {
            auto pawl = AtomicIncrement(ReadPawl);
            if (pawl > AtomicGet(WriteFront)) {
                // Queue is empty
                AtomicDecrement(ReadPawl);
                return nullptr;
            }

            readSlot = AtomicGetAndIncrement(ReadFront);

            auto msg = AtomicSwap(&RingBuffer[readSlot % RingSize], nullptr);
            if (msg != nullptr) {
                return msg;
            }
        }

        /* got no message in the slot, let's try to rollback readfront */
        AtomicCas(&ReadFront, readSlot - 1, readSlot);
        return nullptr;
    }

    void* TMPMCUnorderedRing::UnsafeScanningPop(ui64* last) noexcept {
        for (; *last < RingSize;) {
            auto msg = AtomicSwap(&RingBuffer[*last], nullptr);
            ++*last;
            if (msg != nullptr) {
                return msg;
            }
        }
        return nullptr;
    }
}
