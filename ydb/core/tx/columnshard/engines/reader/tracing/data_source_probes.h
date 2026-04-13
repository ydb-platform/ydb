#pragma once

#include <library/cpp/lwtrace/all.h>

namespace NKikimr::NOlap::NReader {

#define YDB_CS_DATA_SOURCE(PROBE, EVENT, GROUPS, TYPES, NAMES) \
    PROBE(DataSourceStepStart, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "columnsCount", "recordsCount")) \
    PROBE(DataSourceStepFinish, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, TDuration, ui32, ui32, ui64, ui64), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "executionDurationMs", "columnsCount", "recordsCount", "bytesRead", "indexUsage")) \
    PROBE(ProgramChainStart, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, ui32, TDuration, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "programNodeId", "durationMs", "recordsCount")) \
    PROBE(ProgramChainFinish, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, ui32, TDuration, TDuration, ui32, TString), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "programNodeId", "durationMs", "executionDurationMs", "recordsCount", "executionResult")) \
    PROBE(AssemblerStepStart, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "columnsCount", "recordsCount")) \
    PROBE(AssemblerStepFinish, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, TDuration, ui32, ui64, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "executionDurationMs", "columnsCount", "bytesAssembled", "recordsCount")) \
    PROBE(ColumnBlobsFetchingStart, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "columnsCount", "recordsCount")) \
    PROBE(ColumnBlobsFetchingFinish, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, TDuration, ui32, ui64, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "executionDurationMs", "columnsCount", "bytesRead", "recordsCount")) \
    PROBE(MemoryAllocationStart, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui64), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "requestedBytes")) \
    PROBE(MemoryAllocationFinish, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, TDuration, ui64, bool), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "executionDurationMs", "allocatedBytes", "success")) \
    PROBE(SourceFinished, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, TDuration, ui64, TDuration), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "totalDurationMs", "totalBytesRead", "totalExecutionTimeMs")) \

LWTRACE_DECLARE_PROVIDER(YDB_CS_DATA_SOURCE)

}
