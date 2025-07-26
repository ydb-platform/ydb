#pragma once

#include <library/cpp/lwtrace/all.h>

namespace NKikimr::NOlap::NReader {

#define YDB_CS_READER(PROBE, EVENT, GROUPS, TYPES, NAMES) \
    PROBE(SendResult, \
        GROUPS("Reader"), \
        TYPES(ui64, ui64, ui64, ui64, ui64, TDuration, TDuration, TDuration, bool), \
        NAMES("tabletId", "txId", "scanId", "rows", "bytes", "cpuTimeMs", "waitTimeMs", "elapsedMs", "finished")) \
    PROBE(AckReceived, \
        GROUPS("Reader"), \
        TYPES(ui64, ui64, ui64, TDuration), \
        NAMES("tabletId", "txId", "scanId", "elapsedMs")) \
    PROBE(TaskProcessed, \
        GROUPS("Reader"), \
        TYPES(ui64, ui64, ui64, TDuration), \
        NAMES("tabletId", "txId", "scanId", "elapsedMs")) \

LWTRACE_DECLARE_PROVIDER(YDB_CS_READER)

}