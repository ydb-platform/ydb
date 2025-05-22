#pragma once

#include "defs.h"

#include <ydb/library/actors/queues/mpmc_ring_queue.h>
#include <ydb/library/actors/queues/mpmc_ring_queue_v1.h>
#include <ydb/library/actors/queues/mpmc_ring_queue_v2.h>
#include <ydb/library/actors/queues/mpmc_ring_queue_v3.h>
#include <ydb/library/actors/queues/observer/observer.h>


namespace NActors::NQueueBench {

    struct IQueue {
        virtual ~IQueue() = default;
        virtual bool TryPush(ui32 value) = 0;
        virtual std::optional<ui32> TryPop() = 0;
    };

    template <ui32 SizeBits, typename TObserver>
    using TTryPush = bool (TMPMCRingQueue<SizeBits, TObserver>::*)(ui32 value);
    template <ui32 SizeBits, typename TObserver>
    using TTryPop = std::optional<ui32> (TMPMCRingQueue<SizeBits, TObserver>::*)();

    template <ui32 SizeBits, typename TObserver, TTryPush<SizeBits, TObserver> TryPushMethod, TTryPop<SizeBits, TObserver> TryPopMethod>
    struct TMPMCQueueBase : IQueue {
        TMPMCRingQueue<SizeBits, TObserver> *Queue;

        TMPMCQueueBase(TMPMCRingQueue<SizeBits, TObserver> *queue)
            : Queue(queue)
        {}

        bool TryPush(ui32 value) final {
            return (Queue->*TryPushMethod)(value);
        }
        std::optional<ui32> TryPop() final {
            return (Queue->*TryPopMethod)();
        }
    };

    template <ui32 SizeBits, typename TObserver=void>
    using TVerySlowQueue = TMPMCQueueBase<SizeBits, TObserver, &TMPMCRingQueue<SizeBits, TObserver>::TryPushSlow, &TMPMCRingQueue<SizeBits, TObserver>::TryPopReallySlow>;

    template <ui32 SizeBits, typename TObserver=void>
    using TSlowQueue = TMPMCQueueBase<SizeBits, TObserver, &TMPMCRingQueue<SizeBits, TObserver>::TryPushSlow, &TMPMCRingQueue<SizeBits, TObserver>::TryPopSlow>;

    template <ui32 SizeBits, typename TObserver=void>
    using TFastQueue = TMPMCQueueBase<SizeBits, TObserver, &TMPMCRingQueue<SizeBits, TObserver>::TryPush, &TMPMCRingQueue<SizeBits, TObserver>::TryPopFast>;

    template <ui32 SizeBits, typename TObserver=void>
    using TVeryFastQueue = TMPMCQueueBase<SizeBits, TObserver, &TMPMCRingQueue<SizeBits, TObserver>::TryPush, &TMPMCRingQueue<SizeBits, TObserver>::TryPopReallyFast>;

    template <ui32 SizeBits, typename TObserver=void>
    using TSingleQueue = TMPMCQueueBase<SizeBits, TObserver, &TMPMCRingQueue<SizeBits, TObserver>::TryPush, &TMPMCRingQueue<SizeBits, TObserver>::TryPopSingleConsumer>;

    template <ui32 SizeBits, typename TObserver=void>
    struct TAdaptiveQueue : IQueue {
        TMPMCRingQueue<SizeBits, TObserver> *Queue;
        typename TMPMCRingQueue<SizeBits, TObserver>::EPopMode State = TMPMCRingQueue<SizeBits, TObserver>::EPopMode::ReallySlow;

        TAdaptiveQueue(TMPMCRingQueue<SizeBits, TObserver> *queue)
            : Queue(queue)
        {}

        bool TryPush(ui32 value) final {
            return Queue->TryPush(value);
        }
        std::optional<ui32> TryPop() final {
            return Queue->TryPop(State);
        }
    };

    template <ui32 SizeBits, typename TObserver=void>
    struct TNotReallyAdaptiveQueue : IQueue {
        TMPMCRingQueue<SizeBits, TObserver> *Queue;
        typename TMPMCRingQueue<SizeBits, TObserver>::EPopMode State = TMPMCRingQueue<SizeBits, TObserver>::EPopMode::ReallySlow;

        TNotReallyAdaptiveQueue(TMPMCRingQueue<SizeBits, TObserver> *queue)
            : Queue(queue)
        {}

        bool TryPush(ui32 value) final {
            return Queue->TryPush(value);
        }
        std::optional<ui32> TryPop() final {
            if (State == TMPMCRingQueue<SizeBits, TObserver>::EPopMode::ReallySlow) {
                State = TMPMCRingQueue<SizeBits, TObserver>::EPopMode::Slow;
            }
            if (State == TMPMCRingQueue<SizeBits, TObserver>::EPopMode::ReallyFast) {
                State = TMPMCRingQueue<SizeBits, TObserver>::EPopMode::Fast;
            }
            return Queue->TryPop(State);
        }
    };

    template <typename TQueue>
    struct TIdAdaptor : IQueue {
        TQueue *Queue;

        TIdAdaptor(TQueue *queue)
            : Queue(queue)
        {}

        bool TryPush(ui32 value) final {
            return Queue->TryPush(value);
        }
        std::optional<ui32> TryPop() final {
            return Queue->TryPop();
        }
    };


    template <ui32 SizeBits>
    using TMPMCRingQueueWithStats = TMPMCRingQueue<SizeBits, TStatsObserver>;

    template <ui32 SizeBits>
    using TMPMCRingQueueV2WithStats = TMPMCRingQueueV2<SizeBits, TStatsObserver>;

    template <template <ui32, typename> typename TAdaptor>
    struct TAdaptorWithStats {
        template <ui32 SizeBits>
        using Type = TAdaptor<SizeBits, TStatsObserver>;
    };


}