#pragma once

#include "all.h"

#define LWTRACE_INTERNAL_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES)    \
    PROBE(PerfReport, GROUPS(),                                          \
          TYPES(double, double, double, double),                         \
          NAMES("probeShare", "probeMinMs", "probeMaxMs", "probeAvgMs")) \
    PROBE(DeserializationError, GROUPS("LWTraceError"),                  \
          TYPES(TString, TString),                                       \
          NAMES("probeName", "providerName"))                            \
    PROBE(Fork, GROUPS(),                                                \
          TYPES(ui64),                                                   \
          NAMES("spanId"))                                               \
    PROBE(Join, GROUPS(),                                                \
          TYPES(ui64, ui64),                                             \
          NAMES("spanId", "trackLength"))                                \
    PROBE(OrbitIsUsedConcurrentlyError, GROUPS("LWTraceError"),          \
          TYPES(TString),                                                \
          NAMES("backtrace"))                                            \
    /**/

LWTRACE_DECLARE_PROVIDER(LWTRACE_INTERNAL_PROVIDER)
