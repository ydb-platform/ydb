#pragma once

#include <library/cpp/lwtrace/all.h>

struct TQueryType {
    using TStoreType = ui32;
    using TFuncParam = ui32;

    static void ToString(TStoreType value, TString* out);
    static TStoreType ToStoreType(TFuncParam value) {
        return value;
    }
};

struct TQueryAction {
    using TStoreType = ui32;
    using TFuncParam = ui32;

    static void ToString(TStoreType value, TString* out);
    static TStoreType ToStoreType(TFuncParam value) {
        return value;
    }
};

#define KQP_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES) \
    PROBE(KqpQueryRequest, GROUPS("KQP"), \
        TYPES(TString, TQueryType, TQueryAction, TString), \
        NAMES("database", "type", "action", "query")) \
    PROBE(KqpQueryCompiled, GROUPS("KQP"), \
        TYPES(TString), \
        NAMES("compileResultStatus")) \
    PROBE(KqpPhyQueryDefer, GROUPS("KQP"), \
        TYPES(ui64), \
        NAMES("currentTx")) \
    PROBE(KqpPhyQueryProposeTx, GROUPS("KQP"), \
        TYPES(ui64, ui32, ui32, bool), \
        NAMES("currentTx", "transactionsSize", "locksSize", "shouldAcquireLocks")) \
    PROBE(KqpPhyQueryTxResponse, GROUPS("KQP"), \
        TYPES(ui64, ui32), \
        NAMES("currentTx", "resultsSize")) \
    PROBE(KqpQueryReplySuccess, GROUPS("KQP"), \
        TYPES(ui64), \
        NAMES("responseArenaSpaceUsed")) \
    PROBE(KqpQueryReplyError, GROUPS("KQP"), \
        TYPES(TString), \
        NAMES("errMsg")) \
/**/
LWTRACE_DECLARE_PROVIDER(KQP_PROVIDER)
