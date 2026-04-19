#pragma once

#include <library/cpp/lwtrace/all.h>

namespace NKikimr::NOlap::NReader {

#define YDB_CS_DATA_SOURCE(PROBE, EVENT, GROUPS, TYPES, NAMES) \
    PROBE(ProgramConst, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, ui32, TDuration, TDuration, ui32, ui64, TString, TString), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "programNodeId", "durationMs", "executionDurationMs", "rowsCount", "totalReservedBytes", "executionResult", "details")) \
    PROBE(ProgramCalculation, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, ui32, TDuration, TDuration, ui32, ui64, TString, TString), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "programNodeId", "durationMs", "executionDurationMs", "rowsCount", "totalReservedBytes", "executionResult", "details")) \
    PROBE(ProgramProjection, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, ui32, TDuration, TDuration, ui32, ui64, TString, TString), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "programNodeId", "durationMs", "executionDurationMs", "rowsCount", "totalReservedBytes", "executionResult", "details")) \
    PROBE(ProgramFilter, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, ui32, TDuration, TDuration, ui32, ui64, TString, TString), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "programNodeId", "durationMs", "executionDurationMs", "rowsCount", "totalReservedBytes", "executionResult", "details")) \
    PROBE(ProgramAggregation, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, ui32, TDuration, TDuration, ui32, ui64, TString, TString), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "programNodeId", "durationMs", "executionDurationMs", "rowsCount", "totalReservedBytes", "executionResult", "details")) \
    PROBE(ProgramFetchOriginalData, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, ui32, TDuration, TDuration, ui32, ui64, ui64, ui64, TString, TString), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "programNodeId", "durationMs", "executionDurationMs", "rowsCount", "blobBytes", "rawBytes", "totalReservedBytes", "executionResult", "details")) \
    PROBE(SubColumnsHeaderRead, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32, ui64, ui64), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "columnId", "columnName", "durationMs", "chunkIndex", "blobBytes", "rawBytes")) \
    PROBE(SubColumnsDataRead, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, TString, ui32, ui64, ui64), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "columnId", "columnName", "durationMs", "subColumnName", "chunkIndex", "blobBytes", "rawBytes")) \
    PROBE(ProgramAssembleOriginalData, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, ui32, TDuration, TDuration, ui32, ui64, TString, TString), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "programNodeId", "durationMs", "executionDurationMs", "rowsCount", "totalReservedBytes", "executionResult", "details")) \
    PROBE(ProgramCheckIndexData, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, ui32, TDuration, TDuration, ui32, ui32, TString, ui64, TString, TString), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "programNodeId", "durationMs", "executionDurationMs", "rowsCount", "filteredRows", "indexStatus", "totalReservedBytes", "executionResult", "details")) \
    PROBE(ProgramCheckHeaderData, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, ui32, TDuration, TDuration, ui32, ui64, TString, TString), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "programNodeId", "durationMs", "executionDurationMs", "rowsCount", "totalReservedBytes", "executionResult", "details")) \
    PROBE(ProgramStreamLogic, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, ui32, TDuration, TDuration, ui32, ui64, TString, TString), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "programNodeId", "durationMs", "executionDurationMs", "rowsCount", "totalReservedBytes", "executionResult", "details")) \
    PROBE(ProgramReserveMemory, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, ui32, TDuration, TDuration, ui32, ui64, ui64, TString, TString), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "programNodeId", "durationMs", "executionDurationMs", "rowsCount", "reservedBytes", "totalReservedBytes", "executionResult", "details")) \
    PROBE(AssemblerStep, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, TDuration, ui32, ui64, ui32, ui64), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "executionDurationMs", "columnsCount", "bytesAssembled", "rowsCount", "totalReservedBytes")) \
    PROBE(ColumnBlobsFetching, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, TDuration, ui32, ui64, ui64, ui32, ui64), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "executionDurationMs", "columnsCount", "blobBytes", "rawBytes", "rowsCount", "totalReservedBytes")) \
    PROBE(MemoryAllocation, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, TDuration, ui64, bool, ui64), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "executionDurationMs", "reservedBytes", "success", "totalReservedBytes")) \
    PROBE(StartSourceProcessing, \
        GROUPS("Orbit", "DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui64, ui64, ui64, TString, TString, TString, TString), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "blobBytes", "rawBytes", "totalReservedBytes", "minPk", "maxPk", "minSnapshot", "maxSnapshot")) \
    PROBE(Deduplication, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32, ui64), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "rowsCount", "totalReservedBytes")) \
    PROBE(DetectInMemFlag, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui64, ui64, bool, ui32, ui64), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "totalColumnBlobBytes", "totalColumnRawBytes", "sourceInMemory", "rowsCount", "totalReservedBytes")) \
    PROBE(InitializeSource, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32, ui64), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "rowsCount", "totalReservedBytes")) \
    PROBE(PredicateFilter, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32, ui32, ui64), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "rowsCount", "filteredRows", "totalReservedBytes")) \
    PROBE(SnapshotFilter, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32, ui64), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "rowsCount", "totalReservedBytes")) \
    PROBE(DeletionFilter, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32, ui64), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "rowsCount", "totalReservedBytes")) \
    PROBE(ShardingFilter, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32, ui64), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "rowsCount", "totalReservedBytes")) \
    PROBE(FilterCutLimit, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32, ui64), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "rowsCount", "totalReservedBytes")) \
    PROBE(ConflictDetector, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32, ui64), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "rowsCount", "totalReservedBytes")) \
    PROBE(BuildResult, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, TDuration, ui32, ui32, ui64, TDuration, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "executionDurationMs", "rowsCount", "pageRecordsCount", "totalReservedBytes", "sourcesAheadQueueWaitMs", "sourcesAhead")) \
    PROBE(PrepareResult, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, TDuration, ui32, ui64, TDuration, ui32), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "executionDurationMs", "rowsCount", "totalReservedBytes", "sourcesAheadQueueWaitMs", "sourcesAhead")) \
    PROBE(StartPortionAccessorFetching, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32, ui64), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "rowsCount", "totalReservedBytes")) \
    PROBE(PortionAccessorFetched, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32, ui64), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "rowsCount", "totalReservedBytes")) \
    PROBE(UpdateAggregatedMemory, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32, ui64), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "rowsCount", "totalReservedBytes")) \
    PROBE(AggregationSources, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32, ui64), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "rowsCount", "totalReservedBytes")) \
    PROBE(CleanAggregationSources, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32, ui64), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "rowsCount", "totalReservedBytes")) \
    PROBE(DetectInMem, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, ui32, ui64), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "rowsCount", "totalReservedBytes")) \
    PROBE(SourceFinished, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, TString, TDuration, TDuration, ui64, TDuration, ui64), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "stepIndex", "name", "durationMs", "totalDurationMs", "totalBytesRead", "totalExecutionTimeMs", "totalReservedBytes")) \
    PROBE(ResultSyncPoint, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, TString, ui32, ui32, ui64, TDuration, ui32, TString), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "name", "rowsCount", "resultChunkRowsCount", "totalReservedBytes", "sourcesAheadQueueWaitMs", "sourcesAhead", "details")) \
    PROBE(LimitSyncPoint, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, TString, ui32, ui64, TDuration, ui32, TString), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "name", "rowsCount", "totalReservedBytes", "sourcesAheadQueueWaitMs", "sourcesAhead", "details")) \
    PROBE(SyncAggrSyncPoint, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, TString, ui32, ui64, TDuration, ui32, TString), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "name", "rowsCount", "totalReservedBytes", "sourcesAheadQueueWaitMs", "sourcesAhead", "details")) \
    PROBE(MemoryFree, \
        GROUPS("DataSource"), \
        TYPES(ui64, ui64, ui64, ui64, ui64), \
        NAMES("pathId", "tabletId", "txId", "sourceId", "freedBytes")) \

LWTRACE_DECLARE_PROVIDER(YDB_CS_DATA_SOURCE)

}
