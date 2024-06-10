#pragma once

#include "defs.h"
#include "probes.h"


namespace NActors::NQueueBench::NTracing {

    LWTRACE_USING(BENCH_TRACING_PROVIDER);

    struct TMPMCRingQueueBadPathTracer {

#define LWPROBE_PROXY_STATIC_METHOD(STAT_NAME, PROBE, ...) \
    static void Mark ## STAT_NAME () {                     \
        LWPROBE(PROBE, __VA_ARGS__);                       \
    }                                                      \
// LWPROBE_PROXY_STATIC_METHOD

        LWPROBE_PROXY_STATIC_METHOD(FoundOldSlotInFastPush, FoundOldSlot, "FastPush");
        LWPROBE_PROXY_STATIC_METHOD(FoundOldSlotInSlowPush, FoundOldSlot, "SlowPush");
        LWPROBE_PROXY_STATIC_METHOD(FoundOldSlotInSlowPop, FoundOldSlot, "SlowPop");
        LWPROBE_PROXY_STATIC_METHOD(FoundOldSlotInFastPop, FoundOldSlot, "FastPop");
        LWPROBE_PROXY_STATIC_METHOD(FoundOldSlotInReallyFastPop, FoundOldSlot, "ReallyFastPop");

        LWPROBE_PROXY_STATIC_METHOD(FaildedPush, FailedOperation, "SlowPush");

        LWPROBE_PROXY_STATIC_METHOD(LongPush, LongOperation, "SlowPush");
        LWPROBE_PROXY_STATIC_METHOD(LongSlowPop, LongOperation, "SlowPop");
        LWPROBE_PROXY_STATIC_METHOD(LongFastPop, LongOperation, "FastPop");
        LWPROBE_PROXY_STATIC_METHOD(LongReallyFastPop, LongOperation, "ReallyFastPop");

#undef LWPROBE_PROXY_STATIC_METHOD

    };

}