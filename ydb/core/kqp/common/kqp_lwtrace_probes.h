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
    PROBE(KqpSessionQueryRequest, GROUPS("KQP"), \
        TYPES(TString, TQueryType, TQueryAction, TString), \
        NAMES("database", "type", "action", "query")) \
    PROBE(KqpSessionQueryCompiled, GROUPS("KQP"), \
        TYPES(TString), \
        NAMES("compileResultStatus")) \
    PROBE(KqpSessionPhyQueryDefer, GROUPS("KQP"), \
        TYPES(ui64), \
        NAMES("currentTx")) \
    PROBE(KqpSessionPhyQueryProposeTx, GROUPS("KQP"), \
        TYPES(ui64, ui32, ui32, bool), \
        NAMES("currentTx", "transactionsSize", "locksSize", "shouldAcquireLocks")) \
    PROBE(KqpSessionPhyQueryTxResponse, GROUPS("KQP"), \
        TYPES(ui64, ui32), \
        NAMES("currentTx", "resultsSize")) \
    PROBE(KqpSessionReplySuccess, GROUPS("KQP"), \
        TYPES(ui64), \
        NAMES("responseArenaSpaceUsed")) \
    PROBE(KqpSessionReplyError, GROUPS("KQP"), \
        TYPES(TString), \
        NAMES("errMsg")) \
    PROBE(KqpCompileRequestBootstrap, GROUPS("KQP"), \
        TYPES(TString, ui64), \
        NAMES("userSid", "queryHash")) \
    PROBE(KqpCompileRequestHandleServiceReply, GROUPS("KQP"), \
        TYPES(TString, ui64), \
        NAMES("userSid", "queryHash")) \
    PROBE(KqpCompileServiceHandleRequest, GROUPS("KQP"), \
        TYPES(TString, ui64), \
        NAMES("userSid", "queryHash")) \
    PROBE(KqpCompileServiceEnqueued, GROUPS("KQP"), \
        TYPES(TString, ui64), \
        NAMES("userSid", "queryHash")) \
    PROBE(KqpCompileServiceGetCompilation, GROUPS("KQP"), \
        TYPES(TString, ui64, TString), \
        NAMES("userSid", "queryHash", "compileActor")) \
    PROBE(KqpCompileServiceReply, GROUPS("KQP"), \
        TYPES(TString, ui64, TString), \
        NAMES("userSid", "queryHash", "compileResult")) \
    PROBE(KqpBaseExecuterHandleReady, GROUPS("KQP"), \
        TYPES(ui64), \
        NAMES("TxId")) \
    PROBE(KqpBaseExecuterReplyErrorAndDie, GROUPS("KQP"), \
        TYPES(ui64), \
        NAMES("TxId")) \
    PROBE(KqpDataExecuterStartExecute, GROUPS("KQP"), \
        TYPES(ui64), \
        NAMES("TxId")) \
    PROBE(KqpDataExecuterStartTasksAndTxs, GROUPS("KQP"), \
        TYPES(ui64, ui64, ui64), \
        NAMES("TxId", "tasksSize", "TxsSize")) \
    PROBE(KqpDataExecuterFinalize, GROUPS("KQP"), \
        TYPES(ui64, ui64, ui64, ui64), \
        NAMES("TxId", "lastCompletedShard", "ResultRows", "resultSize")) \
    PROBE(KqpDataExecuterReplyErrorAndDie, GROUPS("KQP"), \
        TYPES(ui64), \
        NAMES("TxId")) \
    PROBE(KqpScanExecutorReplyErrorAndDie, GROUPS("KQP"), \
        TYPES(ui64), \
        NAMES("TxId")) \
    PROBE(KqpScanExecutorFinalize, GROUPS("KQP"), \
        TYPES(ui64, ui64, TString, ui64), \
        NAMES("TxId", "lastCompletedTask", "lastCompletedComputeActor", "ResultsSize")) \
    PROBE(KqpLiteralExecuterReplyErrorAndDie, GROUPS("KQP"), \
        TYPES(ui64), \
        NAMES("TxId")) \
    PROBE(KqpLiteralExecuterFinalize, GROUPS("KQP"), \
        TYPES(ui64), \
        NAMES("TxId")) \
/**/
LWTRACE_DECLARE_PROVIDER(KQP_PROVIDER)
