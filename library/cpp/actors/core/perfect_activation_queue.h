#pragma once

#include "defs.h"

namespace NActors {

    class TPerfectActivationQueue {
        using TItem = ui32;

        alignas(64) std::atomic_uint64_t ReadIndex = 0;
        alignas(64) std::atomic_uint64_t WriteIndex = 0;

        TItem *QueueData;

    public:
        static constexpr ui32 Concurrency = 1;

    public:
        TPerfectActivationQueue();
        ~TPerfectActivationQueue();
        void Push(ui32 value);
        ui32 Pop();
        void PushBulk(ui32 *values, size_t count);

    private:
        static ui32 ConvertIndex(ui32 index);
    };

} // NActors
