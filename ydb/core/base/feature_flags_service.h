#pragma once
#include "defs.h"
#include "events.h"

namespace NKikimr {

    struct TEvFeatureFlags {
        enum EEv {
            EvSubscribe = EventSpaceBegin(TKikimrEvents::ES_FEATURE_FLAGS),
            EvUnsubscribe,
            EvChanged,
            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_FEATURE_FLAGS),
            "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_FEATURE_FLAGS)");

        struct TEvSubscribe : public TEventLocal<TEvSubscribe, EvSubscribe> {
            TString Description;

            TEvSubscribe() = default;

            explicit TEvSubscribe(const TString& description)
                : Description(description)
            {}
        };

        struct TEvUnsubscribe : public TEventLocal<TEvUnsubscribe, EvUnsubscribe> {
            TEvUnsubscribe() = default;
        };

        struct TEvChanged : public TEventLocal<TEvChanged, EvChanged> {
            TEvChanged() = default;
        };
    };

    /**
     * Returns the service id that may be used to subscribe to feature flags
     * change notifications.
     */
    TActorId MakeFeatureFlagsServiceID();

} // namespace NKikimr
