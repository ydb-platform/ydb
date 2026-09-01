#pragma once

#include <library/cpp/lwtrace/all.h>

namespace NKikimr::NColumnShard {

// LWPROBE(TxAskPortionChunks, self->TabletID(), transactionTime, totalTime, PortionsByPath.size());

#define YDB_CS(PROBE, EVENT, GROUPS, TYPES, NAMES)                                                                                             \
    PROBE(TxAskPortionChunks, GROUPS("Write"), TYPES(ui64, TDuration, TDuration, ui64),                                                        \
        NAMES("tabletId", "transactionTimeMs", "totalTimeMs", "portionsSize"))                                                                 \
    PROBE(StartCleanup, GROUPS("Cleanup"), TYPES(ui64, ui64, ui64, ui64, ui64, ui64, ui64, ui64, bool, ui64, ui64),                            \
        NAMES("tabletId", "totalPortionsCount", "totalPortions", "portionsPrepared", "drop", "skip", "portionsBatchCount", "chunksBatchCount", \
            "limitExceeded", "maxPortionsBatchLimit", "maxChunksBatchLimit"))                                                                 \
    PROBE(StartWrite, GROUPS("Orbit", "Write"), TYPES(ui64, ui64, ui64, ui64, TString, TDuration, ui64, TString, bool),                        \
        NAMES("pathId", "tabletId", "txId", "cookie", "sender", "writeTimeout", "size", "modificationType", "isBulk"))                         \
    PROBE(WriteEnqueued, GROUPS("Write"), TYPES(ui64, ui64, ui64, ui64, TDuration),                                                            \
        NAMES("pathId", "tabletId", "txId", "cookie", "preprocessDurationMs"))                                                                 \
    PROBE(WriteDequeued, GROUPS("Write"), TYPES(ui64, ui64, ui64, ui64, TDuration),                                                            \
        NAMES("pathId", "tabletId", "txId", "cookie", "queueWaitMs"))                                                                          \
    PROBE(WritePrepareDataBlobs, GROUPS("Write"), TYPES(ui64, ui64, ui64, ui64, TDuration, ui64),                                              \
        NAMES("pathId", "tabletId", "txId", "cookie", "durationMs", "blobBytes"))                                                              \
    PROBE(WritePrepareIndexBlobs, GROUPS("Write"), TYPES(ui64, ui64, ui64, ui64, TDuration, ui64),                                             \
        NAMES("pathId", "tabletId", "txId", "cookie", "durationMs", "blobBytes"))                                                             \
    PROBE(WriteToBlobStorageStart, GROUPS("Write"), TYPES(ui64, ui64, ui64, ui64, ui64),                                                       \
        NAMES("pathId", "tabletId", "txId", "cookie", "blobBytes"))                                                                            \
    PROBE(WriteToBlobStorage, GROUPS("Write"), TYPES(ui64, ui64, ui64, ui64, TDuration, ui64, bool),                                           \
        NAMES("pathId", "tabletId", "txId", "cookie", "durationMs", "blobBytes", "success"))                                                   \
    PROBE(WriteFinished, GROUPS("Write"),                                                                                                     \
        TYPES(ui64, ui64, ui64, ui64, TString, TString, TDuration, TDuration, TDuration, TDuration, TDuration),                               \
        NAMES("pathId", "tabletId", "txId", "cookie", "sender", "type", "totalDurationMs", "transactionTimeMs", "completeTimeMs",              \
            "txTotalTimeMs", "writeMs"))                                                                                                      \
    PROBE(WriteFailed, GROUPS("Write"), TYPES(ui64, ui64, ui64, ui64, TString, TString, TString, TString),                                     \
        NAMES("pathId", "tabletId", "txId", "cookie", "sender", "type", "status", "reason"))

LWTRACE_DECLARE_PROVIDER(YDB_CS)

}   // namespace NKikimr::NColumnShard
