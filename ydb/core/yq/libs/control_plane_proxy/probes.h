#pragma once

#include <library/cpp/lwtrace/all.h>

#define YQ_CONTROL_PLANE_PROXY_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES) \
    PROBE(CreateQueryRequest, \
        GROUPS(), \
        TYPES(TString, TString, TDuration, i64, bool, bool), \
        NAMES("scope", "user", "latencyMs", "size", "success", "timeout")) \
    PROBE(ListQueriesRequest, \
        GROUPS(), \
        TYPES(TString, TString, TDuration, i64, bool, bool), \
        NAMES("scope", "user", "latencyMs", "size", "success", "timeout")) \
    PROBE(DescribeQueryRequest, \
        GROUPS(), \
        TYPES(TString, TString, TString, TDuration, i64, bool, bool), \
        NAMES("scope", "user", "queryId", "latencyMs", "size", "success", "timeout")) \
    PROBE(GetQueryStatusRequest, \
        GROUPS(), \
        TYPES(TString, TString, TString, TDuration, i64, bool, bool), \
        NAMES("scope", "user", "queryId", "latencyMs", "size", "success", "timeout")) \
    PROBE(ModifyQueryRequest, \
        GROUPS(), \
        TYPES(TString, TString, TString, TDuration, i64, bool, bool), \
        NAMES("scope", "user", "queryId", "latencyMs", "size", "success", "timeout")) \
    PROBE(DeleteQueryRequest, \
        GROUPS(), \
        TYPES(TString, TString, TString, TDuration, i64, bool, bool), \
        NAMES("scope", "user", "queryId", "latencyMs", "size", "success", "timeout")) \
    PROBE(ControlQueryRequest, \
        GROUPS(), \
        TYPES(TString, TString, TString, TDuration, i64, bool, bool), \
        NAMES("scope", "user", "queryId", "latencyMs", "size", "success", "timeout")) \
    PROBE(GetResultDataRequest, \
        GROUPS(), \
        TYPES(TString, TString, TString, i32, i64, i64, TDuration, i64, bool, bool), \
        NAMES("scope", "user", "queryId", "resultSetIndex", "offset", "limit", "latencyMs", "size", "success", "timeout")) \
    PROBE(ListJobsRequest, \
        GROUPS(), \
        TYPES(TString, TString, TString, TDuration, i64, bool, bool), \
        NAMES("scope", "user", "queryId", "latencyMs", "size", "success", "timeout")) \
    PROBE(DescribeJobRequest, \
        GROUPS(), \
        TYPES(TString, TString, TString, TDuration, i64, bool, bool), \
        NAMES("scope", "user", "jobId", "latencyMs", "size", "success", "timeout")) \
    PROBE(CreateConnectionRequest, \
        GROUPS(), \
        TYPES(TString, TString, TDuration, i64, bool, bool), \
        NAMES("scope", "user", "latencyMs", "size", "success", "timeout")) \
    PROBE(ListConnectionsRequest, \
        GROUPS(), \
        TYPES(TString, TString, TDuration, i64, bool, bool), \
        NAMES("scope", "user", "latencyMs", "size", "success", "timeout")) \
    PROBE(DescribeConnectionRequest, \
        GROUPS(), \
        TYPES(TString, TString, TString, TDuration, i64, bool, bool), \
        NAMES("scope", "user", "connectionId", "latencyMs", "size", "success", "timeout")) \
    PROBE(ModifyConnectionRequest, \
        GROUPS(), \
        TYPES(TString, TString, TString, TDuration, i64, bool, bool), \
        NAMES("scope", "user", "connectionId", "latencyMs", "size", "success", "timeout")) \
    PROBE(DeleteConnectionRequest, \
        GROUPS(), \
        TYPES(TString, TString, TString, TDuration, i64, bool, bool), \
        NAMES("scope", "user", "connectionId", "latencyMs", "size", "success", "timeout")) \
    PROBE(TestConnectionRequest, \
        GROUPS(), \
        TYPES(TString, TString, TDuration, i64, bool, bool), \
        NAMES("scope", "user", "latencyMs", "size", "success", "timeout")) \
    PROBE(CreateBindingRequest, \
        GROUPS(), \
        TYPES(TString, TString, TDuration, i64, bool, bool), \
        NAMES("scope", "user", "latencyMs", "size", "success", "timeout")) \
    PROBE(ListBindingsRequest, \
        GROUPS(), \
        TYPES(TString, TString, TDuration, i64, bool, bool), \
        NAMES("scope", "user", "latencyMs", "size", "success", "timeout")) \
    PROBE(DescribeBindingRequest, \
        GROUPS(), \
        TYPES(TString, TString, TString, TDuration, i64, bool, bool), \
        NAMES("scope", "user", "bindingId", "latencyMs", "size", "success", "timeout")) \
    PROBE(ModifyBindingRequest, \
        GROUPS(), \
        TYPES(TString, TString, TString, TDuration, i64, bool, bool), \
        NAMES("scope", "user", "bindingId", "latencyMs", "size", "success", "timeout")) \
    PROBE(DeleteBindingRequest, \
        GROUPS(), \
        TYPES(TString, TString, TString, TDuration, i64, bool, bool), \
        NAMES("scope", "user", "bindingId", "latencyMs", "size", "success", "timeout")) \

// YQ_CONTROL_PLANE_PROXY_PROVIDER

LWTRACE_DECLARE_PROVIDER(YQ_CONTROL_PLANE_PROXY_PROVIDER)
