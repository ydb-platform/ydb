#pragma once

/*
  It's not a general purpose queue.
  No order guarantee, but it mostly ordered.
  Items may stuck in almost empty queue.
  Use UnsafeScanningPop to pop all stuck items.
  Almost wait-free for producers and consumers.
 */

#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/generic/ptr.h>

namespace NThreading {
    struct TMPMCUnorderedRing {
    public:
        static constexpr ui16 MAX_PUSH_TRIES = 4;
        static constexpr ui16 MAX_POP_TRIES = 4;

        TMPMCUnorderedRing(size_t size);

        bool Push(void* msg, ui16 retryCount = MAX_PUSH_TRIES) noexcept;
        void StubbornPush(void* msg) {
            while (!WeakPush(msg)) {
            }
        }

        void* Pop() noexcept;

        void* UnsafeScanningPop(ui64* last) noexcept;

    private:
        bool WeakPush(void* msg) noexcept;

        size_t RingSize;
        TArrayPtr<void*> RingBuffer;
        ui64 WritePawl = 0;
        ui64 WriteFront = 0;
        ui64 ReadPawl = 0;
        ui64 ReadFront = 0;
    };
}
