#pragma once
#include <library/cpp/lwtrace/all.h>

#define MONITORING_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES) \
    PROBE(MonitoringCounterLookup, GROUPS("Monitoring"), \
            TYPES(TString, TString, TString), \
            NAMES("methodName", "nameArg", "valueArg")) \
/**/
LWTRACE_DECLARE_PROVIDER(MONITORING_PROVIDER)


