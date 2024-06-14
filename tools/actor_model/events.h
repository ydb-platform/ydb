#include <library/cpp/actors/core/event_local.h>
#include <library/cpp/actors/core/events.h>

struct TEvents {
    struct TEvWriteValueRequest : NActors::TEventLocal<TEvWriteValueRequest, NActors::TEvents::ES_PRIVATE> {
        int64_t Value;
        explicit TEvWriteValueRequest(int64_t value) : Value(value) {}
    };

    struct TEvDone : NActors::TEventLocal<TEvDone, NActors::TEvents::ES_PRIVATE> {};
};
