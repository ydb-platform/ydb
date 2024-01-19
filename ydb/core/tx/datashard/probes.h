#pragma once

#include <library/cpp/lwtrace/all.h>

#define DATASHARD_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES)                 \
    PROBE(ProposeTransactionRequest,                                           \
        GROUPS("DataShard", "LWTrackStart"),                                   \
        TYPES(),                                                               \
        NAMES())                                                               \
    PROBE(ProposeTransactionWaitMediatorState,                                 \
        GROUPS("DataShard"),                                                   \
        TYPES(),                                                               \
        NAMES())                                                               \
    PROBE(ProposeTransactionWaitDelayers,                                      \
        GROUPS("DataShard"),                                                   \
        TYPES(),                                                               \
        NAMES())                                                               \
    PROBE(ProposeTransactionWaitSnapshot,                                      \
        GROUPS("DataShard"),                                                   \
        TYPES(ui64, ui64),                                                     \
        NAMES("snapshotStep", "snapshotTxId"))                                 \
    PROBE(ProposeTransactionReject,                                            \
        GROUPS("DataShard"),                                                   \
        TYPES(),                                                               \
        NAMES())                                                               \
    PROBE(ProposeTransactionEnqueue,                                           \
        GROUPS("DataShard"),                                                   \
        TYPES(),                                                               \
        NAMES())                                                               \
    PROBE(ProposeTransactionParsed,                                            \
        GROUPS("DataShard"),                                                   \
        TYPES(bool),                                                           \
        NAMES("success"))                                                      \
    PROBE(ProposeTransactionSendPrepared,                                      \
        GROUPS("DataShard"),                                                   \
        TYPES(),                                                               \
        NAMES())                                                               \
    PROBE(ProposeTransactionKqpDataExecute,                                    \
        GROUPS("DataShard"),                                                   \
        TYPES(),                                                               \
        NAMES())                                                               \
    PROBE(ProposeTransactionSendResult,                                        \
        GROUPS("DataShard"),                                                   \
        TYPES(),                                                               \
        NAMES())                                                               \
    PROBE(ReadRequest,                                                         \
        GROUPS("DataShard", "LWTrackStart"),                                   \
        TYPES(ui64),                                                           \
        NAMES("readId"))                                                       \
    PROBE(ReadWaitMediatorState,                                               \
        GROUPS("DataShard"),                                                   \
        TYPES(),                                                               \
        NAMES())                                                               \
    PROBE(ReadWaitProposeDelayers,                                             \
        GROUPS("DataShard"),                                                   \
        TYPES(),                                                               \
        NAMES())                                                               \
    PROBE(ReadWaitSnapshot,                                                    \
        GROUPS("DataShard"),                                                   \
        TYPES(ui64, ui64),                                                     \
        NAMES("snapshotStep", "snapshotTxId"))                                 \
    PROBE(ReadExecute,                                                         \
        GROUPS("DataShard"),                                                   \
        TYPES(),                                                               \
        NAMES())                                                               \
    PROBE(ReadSendResult,                                                      \
        GROUPS("DataShard"),                                                   \
        TYPES(),                                                               \
        NAMES())                                                               \
    PROBE(ReadAck,                                                             \
        GROUPS("DataShard"),                                                   \
        TYPES(),                                                               \
        NAMES())                                                               \
    PROBE(ReadCancel,                                                          \
        GROUPS("DataShard"),                                                   \
        TYPES(),                                                               \
        NAMES())                                                               \
    PROBE(WriteRequest,                                                        \
        GROUPS("DataShard", "LWTrackStart"),                                   \
        TYPES(),                                                               \
        NAMES())                                                               \
    PROBE(WriteExecute,                                                        \
        GROUPS("DataShard"),                                                   \
        TYPES(),                                                               \
        NAMES())                                                               \
    PROBE(WriteResult,                                                         \
        GROUPS("DataShard"),                                                   \
        TYPES(),                                                               \
        NAMES())                                                               \

// DATASHARD_PROVIDER

LWTRACE_DECLARE_PROVIDER(DATASHARD_PROVIDER)

namespace NKikimr::NDataShard {

    void RegisterDataShardProbes();

}
