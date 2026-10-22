#pragma once

#include <ydb/library/actors/core/event.h>

#include <memory>

namespace NActors {

    // Receives objects whose memory is no longer used by completed transmissions.
    class IEventDestructor {
    public:
        virtual ~IEventDestructor() = default;

        // Takes ownership and arranges destruction. The event or buffer may be null.
        // May be called concurrently by different engine workers.
        virtual void DestroyEvent(
            std::unique_ptr<IEventBase> event,
            TIntrusivePtr<TEventSerializedData> buffer) = 0;

        // Atomically takes the accumulated time spent in DestroyEvent, in CPU cycles,
        // including mutex waits. Concurrent callers receive disjoint deltas.
        // Excludes asynchronous destruction.
        virtual ui64 GetAndResetDestroyEventsCycles() = 0;
    };

}
