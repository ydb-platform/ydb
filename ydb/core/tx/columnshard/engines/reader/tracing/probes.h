#pragma once

#include <library/cpp/lwtrace/all.h>

namespace NKikimr::NOlap::NReader {

#define YDB_CS_SCAN(PROBE, EVENT, GROUPS, TYPES, NAMES) \
    PROBE(StartScan, \
        GROUPS("Orbit", "Scan"), \
        TYPES(ui64, ui64, ui64, ui64), \
        NAMES("pathId", "tabletId", "txId", "scanId")) \
    PROBE(ScanFinished, \
        GROUPS("Scan"), \
        TYPES(ui64, ui64, ui64, ui64, TDuration), \
        NAMES("pathId", "tabletId", "txId", "scanId", "durationMs")) \
    PROBE(SendResult, \
        GROUPS("Scan"), \
        TYPES(ui64, ui64, ui64, ui64, ui64, ui64, ui64, TDuration, TDuration, TDuration, bool), \
        NAMES("pathId", "tabletId", "txId", "scanId", "sourceId", "rows", "bytes", "cpuTimeMs", "waitTimeMs", "elapsedMs", "finished")) \
    PROBE(AckReceived, \
        GROUPS("Scan"), \
        TYPES(ui64, ui64, ui64, ui64, TDuration), \
        NAMES("pathId", "tabletId", "txId", "scanId", "elapsedMs")) \
    PROBE(ScanStartSource, \
        GROUPS("Scan"), \
        TYPES(ui64, ui64, ui64, ui64, ui64), \
        NAMES("pathId", "tabletId", "txId", "scanId", "sourceId")) \
    PROBE(ScanFinishSource, \
        GROUPS("Scan"), \
        TYPES(ui64, ui64, ui64, ui64, ui64, TString), \
        NAMES("pathId", "tabletId", "txId", "scanId", "sourceId", "name")) \
    PROBE(ColumnEngineForLogsSelect, \
        GROUPS("Scan"), \
        TYPES(ui64, ui64, ui64, ui64, ui64, ui64, ui64, ui64, ui64), \
        NAMES("pathId", "tabletId", "txId", "scanId", "timeOfInsertedSelectMs", "timeOfCommittedSelectMs", "totalPortionsCount", "totalFilteredPortionsCount", "totalResultSize")) \

LWTRACE_DECLARE_PROVIDER(YDB_CS_SCAN)

}