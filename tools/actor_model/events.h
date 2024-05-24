#include <library/cpp/actors/core/event_local.h>
#include <library/cpp/actors/core/events.h>

struct TEvents {
    enum {
        EvWriteValueRequest = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvDone,
    };
};

struct TEvWriteValueRequest : NActors::TEventLocal<TEvWriteValueRequest, TEvents::EvWriteValueRequest> {
    const int64_t Value;

    explicit TEvWriteValueRequest(int64_t value)
        : Value(value)
    {}
};

struct TEvDone : NActors::TEventLocal<TEvDone, TEvents::EvDone> {};