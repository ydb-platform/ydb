#pragma once

#include <library/cpp/actors/core/actorsystem.h>
#include <memory>

struct TEvents {
    struct TEvDone : public NActors::TEventLocal<TEvDone, NActors::TEvents::ES_PRIVATE> {
        const int64_t MaxPrimeDivisor;

        explicit TEvDone(int64_t maxPrimeDivisor)
            : MaxPrimeDivisor(maxPrimeDivisor) {}
    };
};
