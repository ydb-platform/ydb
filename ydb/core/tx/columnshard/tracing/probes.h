#pragma once

#include <library/cpp/lwtrace/all.h>

namespace NKikimr::NColumnShard  {

// LWPROBE(TxAskPortionChunks, self->TabletID(), transactionTime, totalTime, PortionsByPath.size());

#define YDB_CS(PROBE, EVENT, GROUPS, TYPES, NAMES) \
    PROBE(EvWrite, \
        GROUPS("Write"), \
        TYPES(ui64, TString, ui64, ui64, TDuration, ui64, TString, bool, bool, TString, TString), \
        NAMES("tabletId", "sender", "cookie", "txId", "writeTimeout", "size", "modificationType", "isBulk", "success", "status", "reason")) \
    PROBE(EvWriteResult, \
        GROUPS("Write"), \
        TYPES(ui64, TString, ui64, ui64, TString, bool, TString), \
        NAMES("tabletId", "sender", "txId", "cookie", "type", "success", "reason")) \
    PROBE(TxAskPortionChunks, \
        GROUPS("Write"), \
        TYPES(ui64, TDuration, TDuration, ui64), \
        NAMES("tabletId", "transactionTimeMs", "totalTimeMs", "portionsSize")) \
    PROBE(TxBlobsWritingFinished, \
        GROUPS("Write"), \
        TYPES(ui64, TDuration, TDuration, TDuration), \
        NAMES("tabletId", "transactionTimeMs", "completeTimeMs", "totalTimeMs")) \

LWTRACE_DECLARE_PROVIDER(YDB_CS)

}