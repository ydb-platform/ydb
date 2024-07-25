#include "stats.h"
#include "tcmalloc.h"

#include <ydb/core/base/counters.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/process_stats.h>
#include <library/cpp/lfalloc/dbg_info/dbg_info.h>
#include <library/cpp/malloc/api/malloc.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/datetime/base.h>

namespace NKikimr {
    using TDynamicCountersPtr = TIntrusivePtr<::NMonitoring::TDynamicCounters>;
    using TDynamicCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;

    namespace {
        class TLfAllocStats: public IAllocStats {
        private:
            TDynamicCountersPtr CounterGroup;

            TDynamicCounterPtr MmapCount;
            TDynamicCounterPtr MunmapCount;
            TDynamicCounterPtr SlowAllocCount;
            TDynamicCounterPtr DefragmentMemCount;

            TDynamicCounterPtr BytesSmall;
            TDynamicCounterPtr BytesLarge;
            TDynamicCounterPtr BytesSystem;
            TDynamicCounterPtr BytesMmapped;

            TDynamicCounterPtr ActiveBytesSmall;
            TDynamicCounterPtr ActiveBytesLarge;
            TDynamicCounterPtr ActiveBytesSystem;
            TDynamicCounterPtr ActiveBytesMmapped;

        public:
            TLfAllocStats(TDynamicCountersPtr group) {
                CounterGroup = group->GetSubgroup("component", "lfalloc");

                MmapCount = CounterGroup->GetCounter("MmapCount", true);
                MunmapCount = CounterGroup->GetCounter("MunmapCount", true);
                SlowAllocCount = CounterGroup->GetCounter("SlowAllocCount", true);
                DefragmentMemCount = CounterGroup->GetCounter("DefragmentMemCount", true);

                BytesSmall = CounterGroup->GetCounter("BytesSmall", true);
                BytesLarge = CounterGroup->GetCounter("BytesLarge", true);
                BytesSystem = CounterGroup->GetCounter("BytesSystem", true);
                BytesMmapped = CounterGroup->GetCounter("BytesMmapped", true);

                ActiveBytesSmall = CounterGroup->GetCounter("ActiveBytesSmall");
                ActiveBytesLarge = CounterGroup->GetCounter("ActiveBytesLarge");
                ActiveBytesSystem = CounterGroup->GetCounter("ActiveBytesSystem");
                ActiveBytesMmapped = CounterGroup->GetCounter("ActiveBytesMmapped");
            }

            void Update() override {
                using namespace NAllocDbg;

                *MmapCount = GetAllocationCounterFast(CT_MMAP_CNT);
                *MunmapCount = GetAllocationCounterFast(CT_MUNMAP_CNT);
                *SlowAllocCount = GetAllocationCounterFast(CT_SLOW_ALLOC_CNT);
                *DefragmentMemCount = GetAllocationCounterFast(CT_DEGRAGMENT_CNT);

                i64 systemAllocated = GetAllocationCounterFast(CT_SYSTEM_ALLOC);
                i64 systemFreed = GetAllocationCounterFast(CT_SYSTEM_FREE);
                i64 smallAllocated = GetAllocationCounterFast(CT_SMALL_ALLOC);
                i64 smallFreed = GetAllocationCounterFast(CT_SMALL_FREE);
                i64 largeAllocated = GetAllocationCounterFast(CT_LARGE_ALLOC);
                i64 largeFreed = GetAllocationCounterFast(CT_LARGE_FREE);
                i64 totalMmaped = GetAllocationCounterFast(CT_MMAP);
                i64 totalMunmaped = GetAllocationCounterFast(CT_MUNMAP);

                *BytesSystem = systemAllocated;
                *BytesSmall = smallAllocated;
                *BytesLarge = largeAllocated;
                *BytesMmapped = totalMmaped;

                systemAllocated -= systemFreed;
                smallAllocated -= smallFreed;
                largeAllocated -= largeFreed;
                totalMmaped -= totalMunmaped;

                *ActiveBytesSystem = systemAllocated > 0 ? systemAllocated : 0;
                *ActiveBytesSmall = smallAllocated > 0 ? smallAllocated : 0;
                *ActiveBytesLarge = largeAllocated > 0 ? largeAllocated : 0;
                *ActiveBytesMmapped = totalMmaped > 0 ? totalMmaped : 0;
            }
        };

        struct TFakeAllocStats: public IAllocStats {
            void Update() override {
            }
        };

        std::unique_ptr<IAllocStats> CreateAllocStats(TDynamicCountersPtr group) {
            const auto& info = NMalloc::MallocInfo();
            TStringBuf name(info.Name);

            std::unique_ptr<IAllocStats> stats;
            if (name.StartsWith("lf")) {
                stats = std::make_unique<TLfAllocStats>(std::move(group));
            } else if (name.StartsWith("tc")) {
                stats = std::move(CreateTcMallocStats(std::move(group)));
            }

            return stats ? std::move(stats) : std::make_unique<TFakeAllocStats>();
        }

        class TMemStatsCollector: public TActorBootstrapped<TMemStatsCollector> {
        private:
            const TDuration Interval;
            const std::unique_ptr<IAllocStats> AllocStats;

        public:
            static constexpr EActivityType ActorActivityType() {
                return EActivityType::ACTORLIB_STATS;
            }

            TMemStatsCollector(TDuration interval, std::unique_ptr<IAllocStats> allocStats)
                : Interval(interval)
                , AllocStats(std::move(allocStats))
            {
            }

            void Bootstrap(const TActorContext& ctx) {
                Become(&TThis::StateWork);
                ctx.Schedule(Interval, new TEvents::TEvWakeup());
            }

        private:
            STFUNC(StateWork) {
                switch (ev->GetTypeRewrite()) {
                    CFunc(TEvents::TSystem::Wakeup, HandleWakeup);
                }
            }

            void HandleWakeup(const TActorContext& ctx) noexcept {
                AllocStats->Update();
                ctx.Schedule(Interval, new TEvents::TEvWakeup());
            }
        };

        struct TLfAllocState: public IAllocState {
            ui64 GetAllocatedMemoryEstimate() const override {
                ui64 result = 0;
                result += NAllocDbg::GetAllocationCounterFast(NAllocDbg::CT_SYSTEM_ALLOC);
                result -= NAllocDbg::GetAllocationCounterFast(NAllocDbg::CT_SYSTEM_FREE);
                result += NAllocDbg::GetAllocationCounterFast(NAllocDbg::CT_SMALL_ALLOC);
                result -= NAllocDbg::GetAllocationCounterFast(NAllocDbg::CT_SMALL_FREE);
                result += NAllocDbg::GetAllocationCounterFast(NAllocDbg::CT_LARGE_ALLOC);
                result -= NAllocDbg::GetAllocationCounterFast(NAllocDbg::CT_LARGE_FREE);
                return result;
            }
        };

        struct TFakeAllocState: public IAllocState {
            ui64 GetAllocatedMemoryEstimate() const override {
                return 0;
            }
        };

        std::unique_ptr<IAllocState> CreateAllocState() {
            const auto& info = NMalloc::MallocInfo();
            TStringBuf name(info.Name);

            std::unique_ptr<IAllocState> state;
            if (name.StartsWith("lf")) {
                state = std::make_unique<TLfAllocState>();
            } else if (name.StartsWith("tc")) {
                state = std::move(CreateTcMallocState());
            }

            return state ? std::move(state) : std::make_unique<TFakeAllocState>();
        }
    }

    IActor* CreateMemStatsCollector(ui32 intervalSec, TDynamicCountersPtr counters) {
        return new TMemStatsCollector(
            TDuration::Seconds(intervalSec),
            CreateAllocStats(GetServiceCounters(counters, "utils")));
    }

    std::unique_ptr<IAllocState> TAllocState::AllocState = CreateAllocState();

    ui64 TAllocState::GetAllocatedMemoryEstimate() {
        return AllocState->GetAllocatedMemoryEstimate();
    }

}
