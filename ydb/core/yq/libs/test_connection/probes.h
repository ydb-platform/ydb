#pragma once 
 
#include <library/cpp/lwtrace/all.h> 
 
#define YQ_TEST_CONNECTION_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES) \ 
    PROBE(TestConnectionRequest, \ 
        GROUPS(), \ 
        TYPES(TString, TString, TDuration, i64, bool, bool), \ 
        NAMES("scope", "user", "latencyMs", "size", "success", "timeout")) \ 
 
// YQ_TEST_CONNECTION_PROVIDER 
 
LWTRACE_DECLARE_PROVIDER(YQ_TEST_CONNECTION_PROVIDER) 
