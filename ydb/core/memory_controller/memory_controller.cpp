#include "memory_controller.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/base/memory_controller_iface.h>
#include <ydb/core/mon_alloc/stats.h>
#include <ydb/core/tablet_flat/shared_sausagecache.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

namespace NKikimr::NMemory {

namespace {
    using namespace NActors;

    class TMemoryController : public TActorBootstrapped<TMemoryController> {
    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::MEMORY_CONTROLLER;
        }

        TMemoryController(TDuration interval, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters)
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

            ui64 sharedCacheMemoryLimit = 0;
            if (memoryUsage->CGroupLimit) {
                // TODO: sum consumed
                // ui64 usedExternal = 0;
                // // we have: mem.Used = usedExternal + StatAllBytes
                // if (mem.Used > GetStatAllBytes()) {
                //     usedExternal = mem.Used - GetStatAllBytes();
                // }

                // we want: MemLimitBytes + externalUsage <= mem.SoftLimit
                // MemLimitBytes = mem.SoftLimit > usedExternal
                //     ? mem.SoftLimit - usedExternal
                //     : 1;

                sharedCacheMemoryLimit = static_cast<ui64>(0.75 * memoryUsage->CGroupLimit);
            }
            
            Send(MakeSharedPageCacheId(), new TEvMemoryLimit(sharedCacheMemoryLimit));

            ctx.Schedule(Interval, new TEvents::TEvWakeup());
        }

    private:
        const TDuration Interval;
        const TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    };
}

IActor* CreateMemoryController(TDuration interval, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters) {
    return new TMemoryController(interval, 
        GetServiceCounters(counters, "utils")->GetSubgroup("component", "memory_controller"));
}

}