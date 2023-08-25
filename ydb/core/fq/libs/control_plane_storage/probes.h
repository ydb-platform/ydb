#pragma once

#include <library/cpp/lwtrace/all.h>

#define YQ_CONTROL_PLANE_STORAGE_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES) \
    PROBE(CreateQueryRequest, \
        GROUPS(), \
        TYPES(TString, TString, TDuration, i64, bool), \
        NAMES("scope", "user", "latencyMs", "size", "success")) \
    PROBE(ListQueriesRequest, \
        GROUPS(), \
        TYPES(TString, TString, TDuration, i64, bool), \
        NAMES("scope", "user", "latencyMs", "size", "success")) \
    PROBE(DescribeQueryRequest, \
        GROUPS(), \
        TYPES(TString, TString, TString, TDuration, i64, bool), \
        NAMES("scope", "user", "queryId", "latencyMs", "size", "success")) \
    PROBE(GetQueryStatusRequest, \
        GROUPS(), \
        TYPES(TString, TString, TString, TDuration, i64, bool), \
        NAMES("scope", "user", "queryId", "latencyMs", "size", "success")) \
    PROBE(ModifyQueryRequest, \
        GROUPS(), \
        TYPES(TString, TString, TString, TDuration, i64, bool), \
        NAMES("scope", "user", "queryId", "latencyMs", "size", "success")) \
    PROBE(DeleteQueryRequest, \
        GROUPS(), \
        TYPES(TString, TString, TString, TDuration, i64, bool), \
        NAMES("scope", "user", "queryId", "latencyMs", "size", "success")) \
    PROBE(ControlQueryRequest, \
        GROUPS(), \
        TYPES(TString, TString, TString, TDuration, i64, bool), \
        NAMES("scope", "user", "queryId", "latencyMs", "size", "success")) \
    PROBE(GetResultDataRequest, \
        GROUPS(), \
        TYPES(TString, TString, TString, i32, i64, i64, TDuration, i64, bool), \
        NAMES("scope", "user", "queryId", "resultSetIndex", "offset", "limit", "latencyMs", "size", "success")) \
    PROBE(ListJobsRequest, \
        GROUPS(), \
        TYPES(TString, TString, TString, TDuration, i64, bool), \
        NAMES("scope", "user", "queryId", "latencyMs", "size", "success")) \
    PROBE(DescribeJobRequest, \
        GROUPS(), \
        TYPES(TString, TString, TString, TDuration, i64, bool), \
        NAMES("scope", "user", "jobId", "latencyMs", "size", "success")) \
    PROBE(CreateConnectionRequest, \
        GROUPS(), \
        TYPES(TString, TString, TDuration, i64, bool), \
        NAMES("scope", "user", "latencyMs", "size", "success")) \
    PROBE(ListConnectionsRequest, \
        GROUPS(), \
        TYPES(TString, TString, TDuration, i64, bool), \
        NAMES("scope", "user", "latencyMs", "size", "success")) \
    PROBE(DescribeConnectionRequest, \
        GROUPS(), \
        TYPES(TString, TString, TString, TDuration, i64, bool), \
        NAMES("scope", "user", "connectionId", "latencyMs", "size", "success")) \
    PROBE(ModifyConnectionRequest, \
        GROUPS(), \
        TYPES(TString, TString, TString, TDuration, i64, bool), \
        NAMES("scope", "user", "connectionId", "latencyMs", "size", "success")) \
    PROBE(DeleteConnectionRequest, \
        GROUPS(), \
        TYPES(TString, TString, TString, TDuration, i64, bool), \
        NAMES("scope", "user", "connectionId", "latencyMs", "size", "success")) \
    PROBE(CreateBindingRequest, \
        GROUPS(), \
        TYPES(TString, TString, TDuration, i64, bool), \
        NAMES("scope", "user", "latencyMs", "size", "success")) \
    PROBE(ListBindingsRequest, \
        GROUPS(), \
        TYPES(TString, TString, TDuration, i64, bool), \
        NAMES("scope", "user", "latencyMs", "size", "success")) \
    PROBE(DescribeBindingRequest, \
        GROUPS(), \
        TYPES(TString, TString, TString, TDuration, i64, bool), \
        NAMES("scope", "user", "bindingId", "latencyMs", "size", "success")) \
    PROBE(ModifyBindingRequest, \
        GROUPS(), \
        TYPES(TString, TString, TString, TDuration, i64, bool), \
        NAMES("scope", "user", "bindingId", "latencyMs", "size", "success")) \
    PROBE(DeleteBindingRequest, \
        GROUPS(), \
        TYPES(TString, TString, TString, TDuration, i64, bool), \
        NAMES("scope", "user", "bindingId", "latencyMs", "size", "success")) \
    PROBE(WriteResultDataRequest, \
        GROUPS(), \
        TYPES(TString, i32, i64, i64, TDuration, TInstant, i64, bool), \
        NAMES("resultId", "resulSetId", "startRowId", "countRows", "latencyMs", "deadlineSec", "size", "success")) \
    PROBE(GetTaskRequest, \
        GROUPS(), \
        TYPES(TString, TString, TDuration, bool), \
        NAMES("owner", "hostName", "latencyMs", "success")) \
    PROBE(PingTaskRequest, \
        GROUPS(), \
        TYPES(TString, TDuration, bool), \
        NAMES("queryId", "latencyMs", "success")) \
    PROBE(NodesHealthCheckRequest, \
        GROUPS(), \
        TYPES(TString, ui32, TString, TString, TDuration, bool), \
        NAMES("tenant", "nodeId", "instanceId", "hostName", "latencyMs", "success")) \
    PROBE(CreateRateLimiterResourceRequest, \
        GROUPS(), \
        TYPES(TString, TDuration, bool), \
        NAMES("queryId", "latencyMs", "success")) \
    PROBE(DeleteRateLimiterResourceRequest, \
        GROUPS(), \
        TYPES(TString, TDuration, bool), \
        NAMES("queryId", "latencyMs", "success")) \
    PROBE(CreateDatabaseRequest, \
        GROUPS(), \
        TYPES(TString, TString, TDuration, i64, bool), \
        NAMES("scope", "user", "latencyMs", "size", "success")) \
    PROBE(DescribeDatabaseRequest, \
        GROUPS(), \
        TYPES(TString, TString, TDuration, i64, bool), \
        NAMES("scope", "user", "latencyMs", "size", "success")) \
    PROBE(ModifyDatabaseRequest, \
        GROUPS(), \
        TYPES(TString, TString, TDuration, i64, bool), \
        NAMES("scope", "user", "latencyMs", "size", "success")) \

// YQ_CONTROL_PLANE_STORAGE_PROVIDER

LWTRACE_DECLARE_PROVIDER(YQ_CONTROL_PLANE_STORAGE_PROVIDER)
