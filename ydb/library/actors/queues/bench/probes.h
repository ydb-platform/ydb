#pragma once

#include <library/cpp/lwtrace/all.h>
#include <util/generic/vector.h>


#define BENCH_TRACING_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES) \
    PROBE(FoundOldSlot, GROUPS("BenchTracing"),                    \
          TYPES(TString),                                          \
          NAMES("method"))                                         \
    PROBE(FailedOperation, GROUPS("BenchTracing"),                 \
          TYPES(TString),                                          \
          NAMES("method"))                                         \
    PROBE(LongOperation, GROUPS("BenchTracing"),                   \
          TYPES(TString),                                          \
          NAMES("method"))                                         \
// BENCH_TRACING_PROVIDER

LWTRACE_DECLARE_PROVIDER(BENCH_TRACING_PROVIDER)
