#pragma once

#include <ydb/library/actors/core/defs.h>
#include <ydb/library/actors/core/actor.h>

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
        struct TState {
            /**
            * @brief  Number of bytes that the application is actively using to hold data
            * 
            * This is computed by the bytes requested from the OS minus any bytes that are held in caches
            */
            ui64 AllocatedMemory;

            /**
             * @brief Number of bytes that are held in caches
             */
            ui64 AllocatorCachesMemory;
        };

        virtual ~IAllocState() = default;

        
        virtual TState Get() const = 0;
    };

    struct TMemoryUsage {
        ui64 AnonRss;
        ui64 CGroupLimit;

        double Usage() const {
            return CGroupLimit ? static_cast<double>(AnonRss) / CGroupLimit : 0;
        }

        TString ToString() const {
            auto usage = Usage();
            if (usage) {
                return TStringBuilder() << "RSS usage " << usage * 100. << "% (" << AnonRss << " of " << CGroupLimit << " bytes)";
            } else {
                return TStringBuilder() << "RSS usage " << AnonRss << " bytes";
            }
        }
    };

    struct TAllocState {
        static std::unique_ptr<IAllocState> AllocState;

        static IAllocState::TState Get();
    };
}
