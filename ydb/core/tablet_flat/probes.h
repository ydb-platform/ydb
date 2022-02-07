#pragma once

#include <library/cpp/lwtrace/all.h>

#define TABLET_FLAT_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES)               \
    PROBE(TransactionBegin,                                                    \
        GROUPS("TabletTransactions", "LWTrackStart"),                          \
        TYPES(ui64, ui64, TString),                                            \
        NAMES("id", "tablet", "txclass"))                                      \
    PROBE(TransactionPending,                                                  \
        GROUPS("TabletTransactions"),                                          \
        TYPES(ui64, TString),                                                  \
        NAMES("id", "reason"))                                                 \
    PROBE(TransactionEnqueued,                                                 \
        GROUPS("TabletTransactions"),                                          \
        TYPES(ui64),                                                           \
        NAMES("id"))                                                           \
    PROBE(TransactionNeedMemory,                                               \
        GROUPS("TabletTransactions"),                                          \
        TYPES(ui64),                                                           \
        NAMES("id"))                                                           \
    PROBE(TransactionPageFault,                                                \
        GROUPS("TabletTransactions"),                                          \
        TYPES(ui64),                                                           \
        NAMES("id"))                                                           \
    PROBE(TransactionExecuteBegin,                                             \
        GROUPS("TabletTransactions"),                                          \
        TYPES(ui64),                                                           \
        NAMES("id"))                                                           \
    PROBE(TransactionExecuteEnd,                                               \
        GROUPS("TabletTransactions"),                                          \
        TYPES(ui64, bool),                                                     \
        NAMES("id", "success"))                                                \
    PROBE(TransactionReadWriteCommit,                                          \
        GROUPS("TabletTransactions"),                                          \
        TYPES(ui64, ui32),                                                     \
        NAMES("id", "step"))                                                   \
    PROBE(TransactionReadOnlyWait,                                             \
        GROUPS("TabletTransactions"),                                          \
        TYPES(ui64, ui32),                                                     \
        NAMES("id", "step"))                                                   \
    PROBE(TransactionCompleteBegin,                                            \
        GROUPS("TabletTransactions"),                                          \
        TYPES(ui64),                                                           \
        NAMES("id"))                                                           \
    PROBE(TransactionCompleteEnd,                                              \
        GROUPS("TabletTransactions"),                                          \
        TYPES(ui64),                                                           \
        NAMES("id"))                                                           \
// TABLET_FLAT_PROVIDER

LWTRACE_DECLARE_PROVIDER(TABLET_FLAT_PROVIDER)

namespace NKikimr {
namespace NTabletFlatExecutor {

    void RegisterTabletFlatProbes();

}
}
