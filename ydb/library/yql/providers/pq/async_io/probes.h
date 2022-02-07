#pragma once

#include <library/cpp/lwtrace/all.h>

#define DQ_PQ_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES) \
    PROBE(PqReadDataReceived, \
        GROUPS(), \
        TYPES(TString, TString, TString), \
        NAMES("queryId", "topic", "data")) \
    PROBE(PqWriteDataToSend, \
        GROUPS(), \
        TYPES(TString, TString, TString), \
        NAMES("queryId", "topic", "data")) \

// DQ_PQ_PROVIDER

LWTRACE_DECLARE_PROVIDER(DQ_PQ_PROVIDER)
