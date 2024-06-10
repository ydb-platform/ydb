#pragma once

#include "defs.h"

#include <ydb/library/actors/queues/mpmc_ring_queue.h>


namespace NActors::NQueueBench {

    struct IQueue {
        virtual ~IQueue() = default;
        virtual bool TryPush(ui32 value) = 0;
        virtual std::optional<ui32> TryPop() = 0;
    };

    template <ui32 SizeBits, typename TStats>
    using TTryPush = bool (TMPMCRingQueue<SizeBits, TStats>::*)(ui32 value);
    template <ui32 SizeBits, typename TStats>
    using TTryPop = std::optional<ui32> (TMPMCRingQueue<SizeBits, TStats>::*)();

    template <ui32 SizeBits, typename TStats, TTryPush<SizeBits, TStats> TryPushMethod, TTryPop<SizeBits, TStats> TryPopMethod>
    struct TMPMCQueueBase : IQueue {
        TMPMCRingQueue<SizeBits, TStats> *Queue;

        TMPMCQueueBase(TMPMCRingQueue<SizeBits, TStats> *queue)
            : Queue(queue)
        {}

        bool TryPush(ui32 value) final {
            return (Queue->*TryPushMethod)(value);
        }
        std::optional<ui32> TryPop() final {
            return (Queue->*TryPopMethod)();
        }
    };

    template <ui32 SizeBits, typename TStats=void>
    using TVerySlowQueue = TMPMCQueueBase<SizeBits, TStats, &TMPMCRingQueue<SizeBits, TStats>::TryPushSlow, &TMPMCRingQueue<SizeBits, TStats>::TryPopReallySlow>;

    template <ui32 SizeBits, typename TStats=void>
    using TSlowQueue = TMPMCQueueBase<SizeBits, TStats, &TMPMCRingQueue<SizeBits, TStats>::TryPushSlow, &TMPMCRingQueue<SizeBits, TStats>::TryPopSlow>;

    template <ui32 SizeBits, typename TStats=void>
    using TFastQueue = TMPMCQueueBase<SizeBits, TStats, &TMPMCRingQueue<SizeBits, TStats>::TryPush, &TMPMCRingQueue<SizeBits, TStats>::TryPopFast>;

    template <ui32 SizeBits, typename TStats=void>
    using TVeryFastQueue = TMPMCQueueBase<SizeBits, TStats, &TMPMCRingQueue<SizeBits, TStats>::TryPush, &TMPMCRingQueue<SizeBits, TStats>::TryPopReallyFast>;

    template <ui32 SizeBits, typename TStats=void>
    using TSingleQueue = TMPMCQueueBase<SizeBits, TStats, &TMPMCRingQueue<SizeBits, TStats>::TryPush, &TMPMCRingQueue<SizeBits, TStats>::TryPopSingleConsumer>;

    template <ui32 SizeBits, typename TStats=void>
    struct TAdaptiveQueue : IQueue {
        TMPMCRingQueue<SizeBits, TStats> *Queue;
        typename TMPMCRingQueue<SizeBits, TStats>::EPopMode State = TMPMCRingQueue<SizeBits, TStats>::EPopMode::ReallySlow;

        TAdaptiveQueue(TMPMCRingQueue<SizeBits, TStats> *queue)
            : Queue(queue)
        {}

        bool TryPush(ui32 value) final {
            return Queue->TryPush(value);
        }
        std::optional<ui32> TryPop() final {
            return Queue->TryPop(State);
        }
    };


    template <ui32 SizeBits>
    using TMPMCRingQueueWithStats = TMPMCRingQueue<SizeBits, TMPMCRingQueueStats>;

    template <template <ui32, typename> typename TAdaptor>
    struct TAdaptorWithStats {
        template <ui32 SizeBits>
        using Type = TAdaptor<SizeBits, TMPMCRingQueueStats>;
    };


}