#include <library/cpp/actors/core/event_local.h>
#include <library/cpp/actors/core/events.h>

struct TEvents {
    enum EEvents {
        EvWriteValueRequest = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvDone,
        EvMaximumPrimeDevisor,
        EvEnd
    };

    struct TEvWriteValueRequest : NActors::TEventLocal<TEvWriteValueRequest, EvWriteValueRequest> {
        int64_t Value;
        TEvWriteValueRequest(int64_t value) : Value(value) {}
    };

    struct TEvDone : NActors::TEventLocal<TEvDone, EvDone> {};

    struct TEvMaximumPrimeDevisor : NActors::TEventLocal<TEvMaximumPrimeDevisor, EvMaximumPrimeDevisor> {
        int64_t Value;
        TEvMaximumPrimeDevisor(int64_t value) : Value(value) {}
    };
};
