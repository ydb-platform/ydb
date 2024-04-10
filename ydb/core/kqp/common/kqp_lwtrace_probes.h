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
    PROBE(KqpSessionSendRollback, GROUPS("KQP"), \
        TYPES(ui64), \
        NAMES("currentTx")) \
    PROBE(KqpSessionReplySuccess, GROUPS("KQP"), \
        TYPES(ui64), \
        NAMES("responseArenaSpaceUsed")) \
    PROBE(KqpSessionReplyError, GROUPS("KQP"), \
        TYPES(TString), \
        NAMES("errMsg")) \
    PROBE(KqpCompileRequestBootstrap, GROUPS("KQP"), \
        TYPES(TString), \
        NAMES("userSid")) \
    PROBE(KqpCompileRequestHandleServiceReply, GROUPS("KQP"), \
        TYPES(TString), \
        NAMES("userSid")) \
    PROBE(KqpCompileServiceHandleRequest, GROUPS("KQP"), \
        TYPES(TString), \
        NAMES("userSid")) \
    PROBE(KqpCompileServiceEnqueued, GROUPS("KQP"), \
        TYPES(TString), \
        NAMES("userSid")) \
    PROBE(KqpCompileServiceGetCompilation, GROUPS("KQP"), \
        TYPES(TString, TString), \
        NAMES("userSid", "compileActor")) \
    PROBE(KqpCompileServiceReply, GROUPS("KQP"), \
        TYPES(TString, TString), \
        NAMES("userSid", "compileResult")) \
    PROBE(KqpCompileServiceReplyFromCache, GROUPS("KQP"), \
        TYPES(), \
        NAMES()) \
    PROBE(KqpCompileServiceReplyStatements, GROUPS("KQP"), \
        TYPES(), \
        NAMES()) \
    PROBE(KqpCompileServiceReplyError, GROUPS("KQP"), \
        TYPES(), \
        NAMES()) \
    PROBE(KqpCompileServiceReplyInternalError, GROUPS("KQP"), \
        TYPES(), \
        NAMES()) \
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
    PROBE(KqpScanExecuterStartExecute, GROUPS("KQP"), \
        TYPES(ui64), \
        NAMES("TxId")) \
    PROBE(KqpScanExecuterStartTasksAndTxs, GROUPS("KQP"), \
        TYPES(ui64, ui64, ui64), \
        NAMES("TxId", "tasksSize", "scansSize")) \
    PROBE(KqpScanExecuterFinalize, GROUPS("KQP"), \
        TYPES(ui64, ui64, TString, ui64), \
        NAMES("TxId", "lastCompletedTask", "lastCompletedComputeActor", "ResultsSize")) \
    PROBE(KqpLiteralExecuterCreateErrorResponse, GROUPS("KQP"), \
        TYPES(ui64), \
        NAMES("TxId")) \
    PROBE(KqpLiteralExecuterFinalize, GROUPS("KQP"), \
        TYPES(ui64), \
        NAMES("TxId")) \
/**/
LWTRACE_DECLARE_PROVIDER(KQP_PROVIDER)
