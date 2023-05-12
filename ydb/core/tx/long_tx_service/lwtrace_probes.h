#pragma once

#include <library/cpp/lwtrace/all.h>

#define LONG_TX_SERVICE_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES)           \
    PROBE(AcquireReadSnapshotRequest,                                          \
        GROUPS("LongTxSnapshots"),                                             \
        TYPES(TString),                                                        \
        NAMES("database"))                                                     \
    PROBE(AcquireReadSnapshotSuccess,                                          \
        GROUPS("LongTxSnapshots"),                                             \
        TYPES(ui64, ui64),                                                     \
        NAMES("step", "txId"))                                                 \
    PROBE(AcquireReadSnapshotFailure,                                          \
        GROUPS("LongTxSnapshots"),                                             \
        TYPES(int),                                                            \
        NAMES("status"))                                                       \
// LONG_TX_SERVICE_PROVIDER

LWTRACE_DECLARE_PROVIDER(LONG_TX_SERVICE_PROVIDER)

namespace NKikimr {
namespace NLongTxService {

    void RegisterLongTxServiceProbes();

}
}
