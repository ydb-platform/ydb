#pragma once

#include <library/cpp/lwtrace/all.h>

namespace NKikimr::NOlap::NReader {

#define YDB_CS_DATA_SOURCE(PROBE, EVENT, GROUPS, TYPES, NAMES) \
    PROBE(ProgramConst, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, ui32, TDuration, TDuration, ui32, TString, TString), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "programNodeId", "durationMs", "executionDurationMs", "rowsCount", "executionResult", "details")) \
    PROBE(ProgramCalculation, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, ui32, TDuration, TDuration, ui32, TString, TString), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "programNodeId", "durationMs", "executionDurationMs", "rowsCount", "executionResult", "details")) \
    PROBE(ProgramProjection, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, ui32, TDuration, TDuration, ui32, TString, TString), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "programNodeId", "durationMs", "executionDurationMs", "rowsCount", "executionResult", "details")) \
    PROBE(ProgramFilter, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, ui32, TDuration, TDuration, ui32, ui32, TString, TString), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "programNodeId", "durationMs", "executionDurationMs", "rowsCount", "filteredRows", "executionResult", "details")) \
    PROBE(ProgramAggregation, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, ui32, TDuration, TDuration, ui32, TString, TString), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "programNodeId", "durationMs", "executionDurationMs", "rowsCount", "executionResult", "details")) \
    PROBE(ProgramFetchOriginalData, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, ui32, TDuration, TDuration, ui32, ui64, ui64, TString, TString), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "programNodeId", "durationMs", "executionDurationMs", "rowsCount", "blobBytes", "rawBytes", "executionResult", "details")) \
    PROBE(ProgramAssembleOriginalData, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, ui32, TDuration, TDuration, ui32, TString, TString), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "programNodeId", "durationMs", "executionDurationMs", "rowsCount", "executionResult", "details")) \
    PROBE(ProgramCheckIndexData, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, ui32, TDuration, TDuration, ui32, ui32, TString, TString, TString), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "programNodeId", "durationMs", "executionDurationMs", "rowsCount", "filteredRows", "indexStatus", "executionResult", "details")) \
    PROBE(ProgramCheckHeaderData, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, ui32, TDuration, TDuration, ui32, TString, TString), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "programNodeId", "durationMs", "executionDurationMs", "rowsCount", "executionResult", "details")) \
    PROBE(ProgramStreamLogic, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, ui32, TDuration, TDuration, ui32, TString, TString), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "programNodeId", "durationMs", "executionDurationMs", "rowsCount", "executionResult", "details")) \
    PROBE(ProgramReserveMemory, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, ui32, TDuration, TDuration, ui32, ui64, TString, TString), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "programNodeId", "durationMs", "executionDurationMs", "rowsCount", "reservedBytes", "executionResult", "details")) \
    PROBE(AssemblerStep, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, TDuration, ui32, ui64, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "executionDurationMs", "columnsCount", "bytesAssembled", "rowsCount")) \
    PROBE(ColumnBlobsFetching, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, TDuration, ui32, ui64, ui64, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "executionDurationMs", "columnsCount", "blobBytes", "rawBytes", "rowsCount")) \
    PROBE(MemoryAllocation, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, TDuration, ui64, bool, ui64), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "executionDurationMs", "allocatedBytes", "success", "reservedBytes")) \
    PROBE(StartSourceProcessing, \
        GROUPS("Orbit", "DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui64, ui64), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "blobBytes", "rawBytes")) \
    PROBE(Deduplication, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "rowsCount")) \
    PROBE(DetectInMemFlag, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui64, ui64, bool, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "totalColumnBlobBytes", "totalColumnRawBytes", "sourceInMemory", "rowsCount")) \
    PROBE(InitializeSource, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "rowsCount")) \
    PROBE(PredicateFilter, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "rowsCount", "filteredRows")) \
    PROBE(SnapshotFilter, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "rowsCount")) \
    PROBE(DeletionFilter, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "rowsCount")) \
    PROBE(ShardingFilter, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "rowsCount")) \
    PROBE(FilterCutLimit, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "rowsCount")) \
    PROBE(ConflictDetector, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "rowsCount")) \
    PROBE(BuildResult, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "rowsCount")) \
    PROBE(PrepareResult, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "rowsCount")) \
    PROBE(StartPortionAccessorFetching, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "rowsCount")) \
    PROBE(PortionAccessorFetched, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "rowsCount")) \
    PROBE(UpdateAggregatedMemory, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "rowsCount")) \
    PROBE(AggregationSources, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "rowsCount")) \
    PROBE(CleanAggregationSources, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "rowsCount")) \
    PROBE(DetectInMem, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "rowsCount")) \
    PROBE(SourceFinished, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, TDuration, ui64, TDuration), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "totalDurationMs", "totalBytesRead", "totalExecutionTimeMs")) \

LWTRACE_DECLARE_PROVIDER(YDB_CS_DATA_SOURCE)

}
