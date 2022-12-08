#pragma once

#include "defs.h"
#include "config.h"

namespace NActors {
    class IExecutorPool;

    // Pool cpu harmonizer
    class IHarmonizer {
      public:
        virtual ~IHarmonizer() {}
        virtual void Harmonize(ui64 ts) = 0;
        virtual void DeclareEmergency(ui64 ts) = 0;
        virtual void AddPool(IExecutorPool* pool) = 0;
        virtual void Enable(bool enable) = 0;
    };

    IHarmonizer* MakeHarmonizer(ui64 ts);
}
