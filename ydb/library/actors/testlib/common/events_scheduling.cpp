#include "events_scheduling.h"

namespace NActors::NTests {

void TGlobalScheduledEvents::RegisterImpl(const ui32 eventType, const TEventSchedulingFeatures features) {
    TGuard<TMutex> g(Mutex);
    EventsForScheduling[eventType] = features;
}

bool TGlobalScheduledEvents::ContainsImpl(const ui32 eventType) const {
    return !!FeaturesImpl(eventType);
}

std::optional<TEventSchedulingFeatures> TGlobalScheduledEvents::FeaturesImpl(const ui32 eventType) const {
    TGuard<TMutex> g(Mutex);
    auto it = EventsForScheduling.find(eventType);
    if (it != EventsForScheduling.end()) {
        return it->second;
    }
    return {};
}

}
