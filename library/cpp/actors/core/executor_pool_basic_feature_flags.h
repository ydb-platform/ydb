#pragma once

#include "defs.h"

#include <optional>


namespace NActors::NFeatures {

    enum class EActorSystemOptimizationType {
        Common,
        LocalQueues,
    };

    struct TCommonFeatureFlags {
        static constexpr EActorSystemOptimizationType OptimizationType = EActorSystemOptimizationType::Common;

        static constexpr bool ProbeSpinCycles = false;
    };

    struct TLocalQueuesFeatureFlags {
        static constexpr EActorSystemOptimizationType OptimizationType = EActorSystemOptimizationType::LocalQueues;

        static constexpr ui16 MIN_LOCAL_QUEUE_SIZE = 0;
        static constexpr ui16 MAX_LOCAL_QUEUE_SIZE = 16;
        static constexpr std::optional<ui16> FIXED_LOCAL_QUEUE_SIZE = std::nullopt;

        static constexpr bool UseIfAllOtherThreadsAreSleeping = false;
        static constexpr bool UseOnMicroburst = false;
    };

    struct TSpinFeatureFlags {
        static constexpr bool DoNotSpinLower = false;
        static constexpr bool UsePseudoMovingWindow = true;

        static constexpr bool HotColdThreads = false;
        static constexpr bool CalcPerThread = false;
    };

    using TFeatureFlags = TCommonFeatureFlags;

    consteval bool IsCommon() {
        return TFeatureFlags::OptimizationType == EActorSystemOptimizationType::Common;
    }

    consteval bool IsLocalQueues() {
        return TFeatureFlags::OptimizationType == EActorSystemOptimizationType::LocalQueues;
    }

}
