#pragma once

#include <ydb/core/base/counters.h>
#include <ydb/core/mon_alloc/stats.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

namespace NKikimr {

using TDynamicCountersPtr = TIntrusivePtr<::NMonitoring::TDynamicCounters>;
using TDynamicCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;

namespace {
    using namespace NActors;

    class TMemoryController : public TActorBootstrapped<TMemoryController> {
        public:
            static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
                return NKikimrServices::TActivity::MEMORY_CONTROLLER;
            }

            TMemoryController(TDuration interval, TDynamicCountersPtr counters)
                : Interval(interval)
                , Counters(counters)
            {}

            void Bootstrap(const TActorContext& ctx) {
                
                LOG_NOTICE_S(ctx, NKikimrServices::MEMORY_CONTROLLER, "Bootstrapped");

                Become(&TThis::StateWork);
                ctx.Schedule(Interval, new TEvents::TEvWakeup());
            }

        private:
            STFUNC(StateWork) {
                switch (ev->GetTypeRewrite()) {
                    CFunc(TEvents::TEvWakeup::EventType, HandleWakeup);
                    
                }
            }

            void HandleWakeup(const TActorContext& ctx) noexcept {
                std::optional<TMemoryUsage> memoryUsage = TAllocState::TryGetMemoryUsage();
                auto allocatedMemory = TAllocState::GetAllocatedMemoryEstimate();

                LOG_DEBUG_S(ctx, NKikimrServices::MEMORY_CONTROLLER, "Periodic memory stats"
                    << " AnonRss:" << memoryUsage->AnonRss << " CGroupLimit: " << memoryUsage->CGroupLimit << " AllocatedMemory: " << allocatedMemory);

                Counters->GetCounter("Stats/AnonRss")->Set(memoryUsage->AnonRss);
                Counters->GetCounter("Stats/CGroupLimit")->Set(memoryUsage->CGroupLimit);
                Counters->GetCounter("Stats/AllocatedMemory")->Set(allocatedMemory);

                ctx.Schedule(Interval, new TEvents::TEvWakeup());
            }

        private:
            const TDuration Interval;
            const TDynamicCountersPtr Counters;
        };
    }

    IActor* CreateMemoryController(ui32 intervalSec, TDynamicCountersPtr counters) {
        return new TMemoryController(TDuration::Seconds(intervalSec), 
            GetServiceCounters(counters, "utils")->GetSubgroup("component", "memory_controller"));
    }

}