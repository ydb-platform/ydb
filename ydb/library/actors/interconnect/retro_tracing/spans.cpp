#include "spans.h"

#include <cstring>

namespace NActors {

NRetroTracing::TRetroSpan* DeserializeInterconnectRetroSpan(ui32 type, ui32 size, const void* data) {
    Y_DEBUG_ABORT_UNLESS(NRetroTracing::TSpanTypeNamespace::Get(type) == NRetroTracing::TSpanTypeNamespace::INTERCONNECT);
    switch (type) {
#define SPAN_TYPE(TSpanType, typeId)                                \
        case TInterconnectRetroSpanType::typeId: {                  \
            TSpanType res;                                          \
            std::memcpy(reinterpret_cast<void*>(&res), data, size); \
            return new TSpanType(res);                              \
        }

        SPAN_TYPE(TPacketSpan, Packet);
        SPAN_TYPE(TDelayedEventSpan, DelayedEvent);

#undef SPAN_TYPE
        default:
            return nullptr;
    }
}

}  // namespace NActors
