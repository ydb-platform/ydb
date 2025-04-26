#include <library/cpp/actors/core/event_local.h>
#include <library/cpp/actors/core/events.h>

struct TEvents {
    struct LabEvents{
        enum EEvents {
            Done = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
            WriteValueRequest
        };
    };

    struct TEvDone: public NActors::TEventLocal<TEvDone, LabEvents::Done> {
        DEFINE_SIMPLE_LOCAL_EVENT(TEvDone, "LabEvents: Done");
    };

    struct TEvWriteValueRequest: public NActors::TEventLocal<TEvWriteValueRequest, LabEvents::WriteValueRequest> {
        int64_t value;

        explicit TEvWriteValueRequest(int64_t v) : value(v) {}
    };   
};
