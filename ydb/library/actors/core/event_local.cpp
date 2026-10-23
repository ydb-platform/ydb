#include "event_local.h"

namespace NActors::NDetail {

void AbortLocalEventSerialization(const std::type_info& eventTypeInfo, ui32 eventType) {
    Y_ABORT("Serialization of local event %s type %" PRIu32, TypeName(eventTypeInfo).data(), eventType);
}

} // namespace NActors::NDetail
