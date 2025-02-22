#pragma once

#include <library/cpp/lwtrace/all.h>

#define FQ_ROW_DISPATCHER_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES) \
    PROBE(CoordinatorChanged, \
        GROUPS(), \
        TYPES(TString, ui64, TString, ui64, TString), \
        NAMES("sender", "newGeneration", "newCoordinator", "oldGeneration", "oldCoordinator")) \
    PROBE(NodeConnected, \
        GROUPS(), \
        TYPES(TString, ui32), \
        NAMES("sender", "nodeId")) \
    PROBE(NodeDisconnected, \
        GROUPS(), \
        TYPES(TString, ui32), \
        NAMES("sender", "nodeId")) \
    PROBE(UndeliveredStart, \
        GROUPS(), \
        TYPES(TString, ui64, ui64), \
        NAMES("sender", "reason", "generation")) \
    PROBE(UndeliveredSkipGeneration, \
        GROUPS(), \
        TYPES(TString, ui64, ui64, ui64), \
        NAMES("sender", "reason", "generation", "consumerGeneration")) \
    PROBE(UndeliveredDeleteConsumer, \
        GROUPS(), \
        TYPES(TString, ui64, ui64, TString), \
        NAMES("sender", "reason", "generation", "key")) \
    PROBE(CoordinatorPing, \
        GROUPS(), \
        TYPES(TString), \
        NAMES("coordinatorActor")) \
    PROBE(Pong, \
        GROUPS(), \
        TYPES(), \
        NAMES()) \
    PROBE(CoordinatorChangesSubscribe, \
        GROUPS(), \
        TYPES(TString, ui64, TString), \
        NAMES("sender", "coordinatorGeneration", "coordinatorActor")) \
    PROBE(StartSession, \
        GROUPS(), \
        TYPES(TString, TString, ui64), \
        NAMES("sender", "queryId", "size")) \
    PROBE(GetNextBatch, \
        GROUPS(), \
        TYPES(TString, ui32, TString, ui64), \
        NAMES("sender", "partitionId", "queryId", "size")) \
    PROBE(Heartbeat, \
        GROUPS(), \
        TYPES(TString, ui32, TString, ui64), \
        NAMES("sender", "partitionId", "queryId", "size")) \
    PROBE(StopSession, \
        GROUPS(), \
        TYPES(TString, TString, ui64), \
        NAMES("sender", "queryId", "size")) \
    PROBE(TryConnect, \
        GROUPS(), \
        TYPES(TString, ui32), \
        NAMES("sender", "nodeId")) \
    PROBE(PrivateHeartbeat, \
        GROUPS(), \
        TYPES(TString, TString, ui64), \
        NAMES("sender", "queryId", "generation")) \
    PROBE(NewDataArrived, \
        GROUPS(), \
        TYPES(TString, TString, TString, ui64, ui64), \
        NAMES("sender", "readActor", "queryId", "generation", "size")) \
    PROBE(MessageBatch, \
        GROUPS(), \
        TYPES(TString, TString, TString, ui64, ui64), \
        NAMES("sender", "readActor", "queryId", "generation", "size")) \
    PROBE(SessionError, \
        GROUPS(), \
        TYPES(TString, TString, TString, ui64, ui64), \
        NAMES("sender", "readActor", "queryId", "generation", "size")) \
    PROBE(Statistics, \
        GROUPS(), \
        TYPES(TString, TString, ui64, ui64), \
        NAMES("readActor", "queryId", "generation", "size")) \
    PROBE(UpdateMetrics, \
        GROUPS(), \
        TYPES(), \
        NAMES()) \
    PROBE(PrintStateToLog, \
        GROUPS(), \
        TYPES(ui64), \
        NAMES("printStateToLogPeriodSec")) \
    PROBE(SessionStatistic, \
        GROUPS(), \
        TYPES(TString, TString, TString, TString, TString, ui32, ui64, ui64, ui64, ui64, ui64), \
        NAMES("sender", "readGroup", "endpoint","database", "partitionId", "readBytes", "queuedBytes", "restartSessionByOffsets", "readEvents", "lastReadedOffset")) \
    PROBE(GetInternalState, \
        GROUPS(), \
        TYPES(TString, ui64), \
        NAMES("sender", "size")) \

// FQ_ROW_DISPATCHER_PROVIDER

LWTRACE_DECLARE_PROVIDER(FQ_ROW_DISPATCHER_PROVIDER)
