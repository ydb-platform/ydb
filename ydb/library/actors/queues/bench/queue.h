#pragma once

#include "defs.h"

#include <ydb/library/actors/queues/mpmc_ring_queue.h>


namespace NActors::NQueueBench {

    struct IQueue {
        virtual ~IQueue() = default;
        virtual bool TryPush(ui32 value) = 0;
        virtual std::optional<ui32> TryPop() = 0;
    };

    template <ui32 SizeBits>
    using TTryPush = bool (TMPMCRingQueue<SizeBits>::*)(ui32 value);
    template <ui32 SizeBits>
    using TTryPop = std::optional<ui32> (TMPMCRingQueue<SizeBits>::*)();

    template <ui32 SizeBits, TTryPush<SizeBits> TryPushMethod, TTryPop<SizeBits> TryPopMethod>
    struct TMPMCQueueBase : IQueue {
        TMPMCRingQueue<SizeBits> *Queue;

        TMPMCQueueBase(TMPMCRingQueue<SizeBits> *queue)
            : Queue(queue)
        {}

        bool TryPush(ui32 value) final {
            return (Queue->*TryPushMethod)(value);
        }
        std::optional<ui32> TryPop() final {
            return (Queue->*TryPopMethod)();
        }
    };

    template <ui32 SizeBits>
    using TVerySlowQueue = TMPMCQueueBase<SizeBits, &TMPMCRingQueue<SizeBits>::TryPushSlow, &TMPMCRingQueue<SizeBits>::TryPopReallySlow>;

    template <ui32 SizeBits>
    using TSlowQueue = TMPMCQueueBase<SizeBits, &TMPMCRingQueue<SizeBits>::TryPushSlow, &TMPMCRingQueue<SizeBits>::TryPopSlow>;

    template <ui32 SizeBits>
    using TFastQueue = TMPMCQueueBase<SizeBits, &TMPMCRingQueue<SizeBits>::TryPush, &TMPMCRingQueue<SizeBits>::TryPopFast>;

    template <ui32 SizeBits>
    using TVeryFastQueue = TMPMCQueueBase<SizeBits, &TMPMCRingQueue<SizeBits>::TryPush, &TMPMCRingQueue<SizeBits>::TryPopReallyFast>;

    template <ui32 SizeBits>
    using TSingleQueue = TMPMCQueueBase<SizeBits, &TMPMCRingQueue<SizeBits>::TryPush, &TMPMCRingQueue<SizeBits>::TryPopSingleConsumer>;

    template <ui32 SizeBits>
    struct TAdaptiveQueue : IQueue {
        TMPMCRingQueue<SizeBits> *Queue;
        typename TMPMCRingQueue<SizeBits>::EPopMode State = TMPMCRingQueue<SizeBits>::EPopMode::ReallySlow;

        TAdaptiveQueue(TMPMCRingQueue<SizeBits> *queue)
            : Queue(queue)
        {}

        bool TryPush(ui32 value) final {
            return Queue->TryPush(value);
        }
        std::optional<ui32> TryPop() final {
            return Queue->TryPop(State);
        }
    };


}