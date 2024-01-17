#pragma once
#include <util/system/mutex.h>
#include <util/generic/hash_set.h>
#include <util/system/types.h>
#include <util/generic/singleton.h>

namespace NActors::NTests {

class TEventSchedulingFeatures {
private:
    bool UseSchedulingLimit = false;
public:
    TEventSchedulingFeatures& SetUseSchedulingLimit(const bool value = true) {
        UseSchedulingLimit = value;
        return *this;
    }
    bool GetUseSchedulingLimit() const {
        return UseSchedulingLimit;
    }
};

class TGlobalScheduledEvents {
private:
    TMutex Mutex;

    THashMap<ui32, TEventSchedulingFeatures> EventsForScheduling;

    void RegisterImpl(const ui32 eventType, const TEventSchedulingFeatures features);
    bool ContainsImpl(const ui32 eventType) const;
    std::optional<TEventSchedulingFeatures> FeaturesImpl(const ui32 eventType) const;
public:

    class TRegistrator {
    public:
        TRegistrator(const ui32 eventType, const TEventSchedulingFeatures features = TEventSchedulingFeatures()) {
            Singleton<TGlobalScheduledEvents>()->RegisterImpl(eventType, features);
        }
    };

    static bool Contains(const ui32 eventType) {
        return Singleton<TGlobalScheduledEvents>()->ContainsImpl(eventType);
    }

    static std::optional<TEventSchedulingFeatures> Features(const ui32 eventType) {
        return Singleton<TGlobalScheduledEvents>()->FeaturesImpl(eventType);
    }

    static void Register(const ui32 eventType, const TEventSchedulingFeatures features = TEventSchedulingFeatures()) {
        return Singleton<TGlobalScheduledEvents>()->RegisterImpl(eventType, features);
    }

};
}
