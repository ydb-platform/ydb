#pragma once

#include <library/cpp/lwtrace/all.h>

#define YQ_TEST_CONNECTION_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES) \
    PROBE(TestDataStreamsConnectionRequest, \
        GROUPS(), \
        TYPES(TString, TString, TDuration, bool), \
        NAMES("scope", "user", "latencyMs", "success")) \
    PROBE(TestMonitoringConnectionRequest, \
        GROUPS(), \
        TYPES(TString, TString, TDuration, bool), \
        NAMES("scope", "user", "latencyMs", "success")) \
    PROBE(TestObjectStorageConnectionRequest, \
        GROUPS(), \
        TYPES(TString, TString, TDuration, bool), \
        NAMES("scope", "user", "latencyMs", "success")) \
    PROBE(TestUnsupportedConnectionRequest, \
        GROUPS(), \
        TYPES(TString, TString), \
        NAMES("scope", "user")) \

// YQ_TEST_CONNECTION_PROVIDER

LWTRACE_DECLARE_PROVIDER(YQ_TEST_CONNECTION_PROVIDER)
