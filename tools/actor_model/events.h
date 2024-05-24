#include <library/cpp/actors/core/event_local.h>
#include <library/cpp/actors/core/events.h>

struct TEvents {
    enum Events {
        EvWriteValueRequest = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EventFinished
    };
    struct TEvWriteValueRequest : NActors::TEventLocal<TEvWriteValueRequest, EvWriteValueRequest> {
        int64_t value;
        explicit TEvWriteValueRequest(int64_t value) : value(value) {}
    };
    struct TEvDone : NActors::TEventLocal<TEvDone, EventFinished> {};
};