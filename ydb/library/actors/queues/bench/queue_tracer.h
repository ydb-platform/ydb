#pragma once

#include "defs.h"
#include "probes.h"

#include <ydb/library/actors/queues/observer/observer.h>


namespace NActors::NQueueBench::NTracing {

    LWTRACE_USING(BENCH_TRACING_PROVIDER);

    struct TMPMCRingQueueBadPathTracer {

        using TStats = TStatsObserver::TStats;

#define LWPROBE_PROXY_STATIC_METHOD(STAT_NAME, PROBE, ...)    \
    static void Observe ## STAT_NAME () {                     \
        LWPROBE(PROBE, __VA_ARGS__);                          \
        TStatsObserver::Observe ## STAT_NAME (); \
    }                                                         \
// LWPROBE_PROXY_STATIC_METHOD
        LWPROBE_PROXY_STATIC_METHOD(FoundOldSlotInFastPush, FoundOldSlot, "FastPush");
        LWPROBE_PROXY_STATIC_METHOD(FoundOldSlotInSlowPush, FoundOldSlot, "SlowPush");
        LWPROBE_PROXY_STATIC_METHOD(FoundOldSlotInSlowPop, FoundOldSlot, "SlowPop");
        LWPROBE_PROXY_STATIC_METHOD(FoundOldSlotInFastPop, FoundOldSlot, "FastPop");
        LWPROBE_PROXY_STATIC_METHOD(FoundOldSlotInReallyFastPop, FoundOldSlot, "ReallyFastPop");

        LWPROBE_PROXY_STATIC_METHOD(FailedPush, FailedOperation, "SlowPush");

        LWPROBE_PROXY_STATIC_METHOD(LongPush10It, LongOperation, "Push10It");
        LWPROBE_PROXY_STATIC_METHOD(LongSlowPop10It, LongOperation, "SlowPop10It");
        LWPROBE_PROXY_STATIC_METHOD(LongFastPop10It, LongOperation, "FastPop10It");
        LWPROBE_PROXY_STATIC_METHOD(LongReallyFastPop10It, LongOperation, "ReallyFastPop10It");

        LWPROBE_PROXY_STATIC_METHOD(LongPush100It, LongOperation, "LongPush100It");
        LWPROBE_PROXY_STATIC_METHOD(LongSlowPop100It, LongOperation, "LongSlowPop100It");
        LWPROBE_PROXY_STATIC_METHOD(LongFastPop100It, LongOperation, "LongFastPop100It");
        LWPROBE_PROXY_STATIC_METHOD(LongReallyFastPop100It, LongOperation, "LongReallyFastPop100It");

        LWPROBE_PROXY_STATIC_METHOD(LongPush1000It, LongOperation, "LongPush1000It");
        LWPROBE_PROXY_STATIC_METHOD(LongSlowPop1000It, LongOperation, "LongSlowPop1000It");
        LWPROBE_PROXY_STATIC_METHOD(LongFastPop1000It, LongOperation, "LongFastPop1000It");
        LWPROBE_PROXY_STATIC_METHOD(LongReallyFastPop1000It, LongOperation, "LongReallyFastPop1000It");
#undef LWPROBE_PROXY_STATIC_METHOD

#define DEFAULT_OBSERVER_PROXY_STATIC_METHOD(STAT_NAME)       \
    static void Observe ## STAT_NAME () {                     \
        TStatsObserver::Observe ## STAT_NAME (); \
    }                                                         \
// DEFAULT_OBSERVER_PROXY_STATIC_METHOD
        DEFAULT_OBSERVER_PROXY_STATIC_METHOD(SuccessSlowPush);
        DEFAULT_OBSERVER_PROXY_STATIC_METHOD(SuccessFastPush);

        DEFAULT_OBSERVER_PROXY_STATIC_METHOD(SuccessOvertakenPop);
        DEFAULT_OBSERVER_PROXY_STATIC_METHOD(SuccessSlowPop);
        DEFAULT_OBSERVER_PROXY_STATIC_METHOD(SuccessFastPop);
        DEFAULT_OBSERVER_PROXY_STATIC_METHOD(SuccessReallyFastPop);
#undef DEFAULT_OBSERVER_PROXY_STATIC_METHOD

        static TStats GetLocalStats() {
            return TStatsObserver::GetLocalStats();
        }

    };

    template <ui64 StepsBeforeAskingToSleep, ui64 MaxInFlight, ui64 SleepUs>
    struct TMPMCRingQueueDegradator {
        using TThis = TMPMCRingQueueDegradator<StepsBeforeAskingToSleep, MaxInFlight, SleepUs>;

#define OBSERVER_SLEEPING_STATIC_METHOD(STAT_NAME)                                                                \
    static void Observe ## STAT_NAME () {                                                                         \
        if (TThis::InFlight++ == StepsBeforeAskingToSleep) {                                                      \
            TThis::InFlight = 0;                                                                                  \
            ui64 inFlight = TThis::InFlight.load(std::memory_order_acquire);                                      \
            while (inFlight < MaxInFlight) {                                                                      \
                if (TThis::InFlight.compare_exchange_strong(inFlight, inFlight + 1, std::memory_order_acq_rel)) { \
                    NanoSleep(SleepUs * 1000);                                                                    \
                    TThis::InFlight.fetch_sub(1, std::memory_order_relaxed);                                      \
                    break;                                                                                        \
                }                                                                                                 \
            }                                                                                                     \
        }                                                                                                         \
    }                                                                                                             \
// OBSERVER_SLEEPING_STATIC_METHOD
        OBSERVER_SLEEPING_STATIC_METHOD(AfterReserveSlotInFastPush)
        OBSERVER_SLEEPING_STATIC_METHOD(AfterReserveSlotInFastPop)
        OBSERVER_SLEEPING_STATIC_METHOD(AfterReserveSlotInReallyFastPop)
        OBSERVER_SLEEPING_STATIC_METHOD(AfterIncorectlyChangeSlotGenerationInReallyFastPop)
#undef OBSERVER_SLEEPING_STATIC_METHOD

        static thread_local ui64 SkipSteps;
        alignas(64) static std::atomic_uint64_t InFlight;
    };

    template <ui64 StepsBeforeAskingToSleep, ui64 MaxInFlight, ui64 SleepUs>
    struct TMPMCRingQueueDegradatorAndTracer
        : TMPMCRingQueueBadPathTracer
        , TMPMCRingQueueDegradator<StepsBeforeAskingToSleep, MaxInFlight, SleepUs>
    {
        using TDegradator = TMPMCRingQueueDegradator<StepsBeforeAskingToSleep, MaxInFlight, SleepUs>;
    };


    struct TStatsCollector {
        using TStatsSource = TMPMCRingQueueBadPathTracer;

        TMutex Mutex;
        NActors::TStatsObserver::TStats Stats;

        void AddStats(const NActors::TStatsObserver::TStats &stats);
        static TString PrettyInt(ui64 value);
        static TString PrettyDivision(ui64 value, ui64 divider);
        static TString Pretty(ui64 stat, ui64 seconds, ui64  threads);
        void GeneralPrint(ui64 seconds, ui64 producerThreads, ui64 consumerThreads, bool shortOutput) const;
        void Print(ui64 seconds, ui64 producerThreads, ui64 consumerThreads, bool shortOutput) const;
        void Print(ui64 seconds, ui64 threads, bool shortOutput) const;
    };

}