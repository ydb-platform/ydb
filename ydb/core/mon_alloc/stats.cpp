#include "stats.h"
#include "tcmalloc.h"

#include <ydb/core/base/counters.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/process_stats.h>
#include <library/cpp/lfalloc/dbg_info/dbg_info.h>
#include <library/cpp/malloc/api/malloc.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/ytalloc/api/ytalloc.h>

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

        class TYtAllocStats: public IAllocStats {
            struct TTimingEventCounters {
                TDynamicCounterPtr Count;
                TDynamicCounterPtr Size;
            };

        private:
            TDynamicCountersPtr CounterGroup;

            NYT::TEnumIndexedVector<NYT::NYTAlloc::ETotalCounter, TDynamicCounterPtr> TotalAllocationCounters;
            NYT::TEnumIndexedVector<NYT::NYTAlloc::ESmallCounter, TDynamicCounterPtr> SmallAllocationCounters;
            NYT::TEnumIndexedVector<NYT::NYTAlloc::ELargeCounter, TDynamicCounterPtr> LargeAllocationCounters;
            std::array<NYT::TEnumIndexedVector<NYT::NYTAlloc::ESmallArenaCounter, TDynamicCounterPtr>, NYT::NYTAlloc::SmallRankCount> SmallArenaAllocationCounters;
            std::array<NYT::TEnumIndexedVector<NYT::NYTAlloc::ELargeArenaCounter, TDynamicCounterPtr>, NYT::NYTAlloc::LargeRankCount> LargeArenaAllocationCounters;
            NYT::TEnumIndexedVector<NYT::NYTAlloc::EHugeCounter, TDynamicCounterPtr> HugeAllocationCounters;
            NYT::TEnumIndexedVector<NYT::NYTAlloc::ESystemCounter, TDynamicCounterPtr> SystemAllocationCounters;
            NYT::TEnumIndexedVector<NYT::NYTAlloc::EUndumpableCounter, TDynamicCounterPtr> UndumpableAllocationCounters;
            NYT::TEnumIndexedVector<NYT::NYTAlloc::ETimingEventType, TTimingEventCounters> TimingEventCounters;

        public:
            TYtAllocStats(TDynamicCountersPtr group) {
                CounterGroup = group->GetSubgroup("component", "ytalloc");

                InitCounters(TotalAllocationCounters, CounterGroup->GetSubgroup("category", "total"));
                InitCounters(SmallAllocationCounters, CounterGroup->GetSubgroup("category", "small"));
                InitCounters(LargeAllocationCounters, CounterGroup->GetSubgroup("category", "large"));
                InitCounters(SmallArenaAllocationCounters, CounterGroup->GetSubgroup("category", "small_arena"));
                InitCounters(LargeArenaAllocationCounters, CounterGroup->GetSubgroup("category", "large_arena"));
                InitCounters(HugeAllocationCounters, CounterGroup->GetSubgroup("category", "huge"));
                InitCounters(SystemAllocationCounters, CounterGroup->GetSubgroup("category", "system"));
                InitCounters(UndumpableAllocationCounters, CounterGroup->GetSubgroup("category", "undumpable"));
                InitCounters(TimingEventCounters, CounterGroup->GetSubgroup("category", "timing_event"));
            }

            void Update() override {
                UpdateCounters(TotalAllocationCounters, NYT::NYTAlloc::GetTotalAllocationCounters());
                UpdateCounters(SmallAllocationCounters, NYT::NYTAlloc::GetSmallAllocationCounters());
                UpdateCounters(LargeAllocationCounters, NYT::NYTAlloc::GetLargeAllocationCounters());
                UpdateCounters(SmallArenaAllocationCounters, NYT::NYTAlloc::GetSmallArenaAllocationCounters());
                UpdateCounters(LargeArenaAllocationCounters, NYT::NYTAlloc::GetLargeArenaAllocationCounters());
                UpdateCounters(HugeAllocationCounters, NYT::NYTAlloc::GetHugeAllocationCounters());
                UpdateCounters(SystemAllocationCounters, NYT::NYTAlloc::GetSystemAllocationCounters());
                UpdateCounters(UndumpableAllocationCounters, NYT::NYTAlloc::GetUndumpableAllocationCounters());
                UpdateCounters(TimingEventCounters, NYT::NYTAlloc::GetTimingEventCounters());
            }

        private:
            template <typename E>
            static void InitCounters(
                NYT::TEnumIndexedVector<E, TDynamicCounterPtr>& counters,
                TDynamicCountersPtr group) {
                for (auto c : NYT::TEnumTraits<E>::GetDomainValues()) {
                    counters[c] = group->GetCounter(NYT::TEnumTraits<E>::ToString(c));
                }
            }

            template <typename E, size_t N>
            static void InitCounters(
                std::array<NYT::TEnumIndexedVector<E, TDynamicCounterPtr>, N>& counters,
                TDynamicCountersPtr group) {
                for (size_t i = 0; i < N; ++i) {
                    InitCounters(counters[i], group->GetSubgroup("rank", ToString(i)));
                }
            }

            template <typename E>
            static void InitCounters(
                NYT::TEnumIndexedVector<E, TTimingEventCounters>& counters,
                TDynamicCountersPtr group) {
                for (auto c : NYT::TEnumTraits<E>::GetDomainValues()) {
                    const auto& name = NYT::TEnumTraits<E>::ToString(c);
                    counters[c].Count = group->GetCounter(name + "_Count");
                    counters[c].Size = group->GetCounter(name + "_Size");
                }
            }

            template <typename E>
            static void UpdateCounters(
                NYT::TEnumIndexedVector<E, TDynamicCounterPtr>& counters,
                const NYT::TEnumIndexedVector<E, ssize_t>& source) {
                for (auto c : NYT::TEnumTraits<E>::GetDomainValues()) {
                    *counters[c] = source[c];
                }
            }

            template <typename E, size_t N>
            static void UpdateCounters(
                std::array<NYT::TEnumIndexedVector<E, TDynamicCounterPtr>, N>& counters,
                const std::array<NYT::TEnumIndexedVector<E, ssize_t>, N>& source) {
                for (size_t i = 0; i < N; ++i) {
                    UpdateCounters(counters[i], source[i]);
                }
            }

            template <typename E>
            static void UpdateCounters(
                NYT::TEnumIndexedVector<E, TTimingEventCounters>& counters,
                const NYT::TEnumIndexedVector<E, NYT::NYTAlloc::TTimingEventCounters>& source) {
                for (auto c : NYT::TEnumTraits<E>::GetDomainValues()) {
                    *counters[c].Count = source[c].Count;
                    *counters[c].Size = source[c].Size;
                }
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
            } else if (name.StartsWith("yt")) {
                stats = std::make_unique<TYtAllocStats>(std::move(group));
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

    TMemoryUsage TAllocState::GetMemoryUsage() {
        NActors::TProcStat procStat;
        procStat.Fill(getpid());
        return TMemoryUsage {
            .AnonRss = procStat.AnonRss,
            .CGroupLimit = procStat.CGroupMemLim
        };
    }
}
