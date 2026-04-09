#pragma once

#include <library/cpp/lwtrace/all.h>

namespace NKikimr::NOlap::NReader {

#define YDB_CS_DATA_SOURCE(PROBE, EVENT, GROUPS, TYPES, NAMES) \
    PROBE(ProgramConst, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, ui32, TDuration, TDuration, ui32, TString, TString), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "programNodeId", "durationMs", "executionDurationMs", "recordsCount", "executionResult", "details")) \
    PROBE(ProgramCalculation, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, ui32, TDuration, TDuration, ui32, TString, TString), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "programNodeId", "durationMs", "executionDurationMs", "recordsCount", "executionResult", "details")) \
    PROBE(ProgramProjection, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, ui32, TDuration, TDuration, ui32, TString, TString), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "programNodeId", "durationMs", "executionDurationMs", "recordsCount", "executionResult", "details")) \
    PROBE(ProgramFilter, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, ui32, TDuration, TDuration, ui32, TString, TString), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "programNodeId", "durationMs", "executionDurationMs", "recordsCount", "executionResult", "details")) \
    PROBE(ProgramAggregation, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, ui32, TDuration, TDuration, ui32, TString, TString), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "programNodeId", "durationMs", "executionDurationMs", "recordsCount", "executionResult", "details")) \
    PROBE(ProgramFetchOriginalData, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, ui32, TDuration, TDuration, ui32, TString, TString), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "programNodeId", "durationMs", "executionDurationMs", "recordsCount", "executionResult", "details")) \
    PROBE(ProgramAssembleOriginalData, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, ui32, TDuration, TDuration, ui32, TString, TString), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "programNodeId", "durationMs", "executionDurationMs", "recordsCount", "executionResult", "details")) \
    PROBE(ProgramCheckIndexData, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, ui32, TDuration, TDuration, ui32, TString, TString), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "programNodeId", "durationMs", "executionDurationMs", "recordsCount", "executionResult", "details")) \
    PROBE(ProgramCheckHeaderData, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, ui32, TDuration, TDuration, ui32, TString, TString), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "programNodeId", "durationMs", "executionDurationMs", "recordsCount", "executionResult", "details")) \
    PROBE(ProgramStreamLogic, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, ui32, TDuration, TDuration, ui32, TString, TString), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "programNodeId", "durationMs", "executionDurationMs", "recordsCount", "executionResult", "details")) \
    PROBE(ProgramReserveMemory, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, ui32, TDuration, TDuration, ui32, TString, TString), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "programNodeId", "durationMs", "executionDurationMs", "recordsCount", "executionResult", "details")) \
    PROBE(AssemblerStep, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, TDuration, ui32, ui64, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "executionDurationMs", "columnsCount", "bytesAssembled", "recordsCount")) \
    PROBE(ColumnBlobsFetching, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, TDuration, ui32, ui64, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "executionDurationMs", "columnsCount", "bytesRead", "recordsCount")) \
    PROBE(MemoryAllocation, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, TDuration, ui64, bool), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "executionDurationMs", "allocatedBytes", "success")) \
    PROBE(StartSourceProcessing, \
        GROUPS("Orbit", "DataSource"), \
        TYPES(ui64, ui64, ui64, ui64), \
        NAMES("pathId", "tabletId", "txId", "sourceId")) \
    PROBE(Deduplication, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "recordsCount")) \
    PROBE(DetectInMemFlag, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui64, bool, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "columnRawBytes", "sourceInMemory", "recordsCount")) \
    PROBE(InitializeSource, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "recordsCount")) \
    PROBE(PredicateFilter, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "recordsCount")) \
    PROBE(SnapshotFilter, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "recordsCount")) \
    PROBE(DeletionFilter, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "recordsCount")) \
    PROBE(ShardingFilter, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "recordsCount")) \
    PROBE(FilterCutLimit, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "recordsCount")) \
    PROBE(ConflictDetector, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "recordsCount")) \
    PROBE(BuildResult, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "recordsCount")) \
    PROBE(PrepareResult, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "recordsCount")) \
    PROBE(StartPortionAccessorFetching, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "recordsCount")) \
    PROBE(PortionAccessorFetched, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "recordsCount")) \
    PROBE(UpdateAggregatedMemory, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "recordsCount")) \
    PROBE(AggregationSources, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "recordsCount")) \
    PROBE(CleanAggregationSources, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "recordsCount")) \
    PROBE(DetectInMem, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "recordsCount")) \
    PROBE(SourceFinished, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, TDuration, ui64, TDuration), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "totalDurationMs", "totalBytesRead", "totalExecutionTimeMs")) \

LWTRACE_DECLARE_PROVIDER(YDB_CS_DATA_SOURCE)

}
