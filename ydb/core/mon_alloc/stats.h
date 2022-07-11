#pragma once

#include <library/cpp/actors/core/defs.h>
#include <library/cpp/actors/core/actor.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr {
    struct IAllocStats {
        virtual ~IAllocStats() = default;
        virtual void Update() = 0;
    };

    NActors::IActor* CreateMemStatsCollector(
        ui32 intervalSec,
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters);


    struct IAllocState {
        virtual ~IAllocState() = default;
        virtual ui64 GetAllocatedMemoryEstimate() const = 0;
    };

    struct TAllocState {
        static std::unique_ptr<IAllocState> AllocState;

        static ui64 GetAllocatedMemoryEstimate();
        static double GetMemoryUsage();
    };
}
