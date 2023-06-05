#pragma once

#include <library/cpp/deprecated/enum_codegen/enum_codegen.h>

namespace NKikimr {
namespace NKqp {

#define DB_KQP_SIMPLE_COUNTERS_MAP(XX) \
    XX(DB_KQP_WORKERS_ACTIVE, ActiveWorkers) \
    XX(DB_KQP_YDB_WORKERS_ACTIVE, YdbSessionsActiveCount) \
    XX(DB_KQP_COMPILE_ACTIVE, CompileActive)

#define DB_KQP_CUMULATIVE_COUNTERS_MAP(XX) \
    XX(DB_KQP_REQ_QUERY_EXECUTE, QueryActionRequests[NKikimrKqp::QUERY_ACTION_EXECUTE]) \
    XX(DB_KQP_REQ_QUERY_EXPLAIN, QueryActionRequests[NKikimrKqp::QUERY_ACTION_EXPLAIN]) \
    XX(DB_KQP_REQ_QUERY_VALIDATE, QueryActionRequests[NKikimrKqp::QUERY_ACTION_VALIDATE]) \
    XX(DB_KQP_REQ_QUERY_PREPARE, QueryActionRequests[NKikimrKqp::QUERY_ACTION_PREPARE]) \
    XX(DB_KQP_REQ_QUERY_EXECUTE_PREPARED, QueryActionRequests[NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED]) \
    XX(DB_KQP_REQ_QUERY_BEGIN_TX, QueryActionRequests[NKikimrKqp::QUERY_ACTION_BEGIN_TX]) \
    XX(DB_KQP_REQ_QUERY_COMMIT_TX, QueryActionRequests[NKikimrKqp::QUERY_ACTION_COMMIT_TX]) \
    XX(DB_KQP_REQ_QUERY_ROLLBACK_TX, QueryActionRequests[NKikimrKqp::QUERY_ACTION_ROLLBACK_TX]) \
    XX(DB_KQP_REQ_QUERY_PARSE, QueryActionRequests[NKikimrKqp::QUERY_ACTION_PARSE]) \
    XX(DB_KQP_REQ_QUERY_OTHER, OtherQueryRequests) \
    XX(DB_KQP_CLOSE_SESSION_REQ, CloseSessionRequests) \
    XX(DB_KQP_PING_SESSION_REQ, PingSessionRequests) \
    XX(DB_KQP_CANCEL_QUERY_REQ, CancelQueryRequests) \
    XX(DB_KQP_REQUEST_BYTES, RequestBytes) \
    XX(DB_KQP_QUERY_BYTES, QueryBytes) \
    XX(DB_KQP_PARAMS_BYTES, ParametersBytes) \
    XX(DB_KQP_SQL_V0_TRANSLATIONS, SqlV0Translations) \
    XX(DB_KQP_SQL_V1_TRANSLATIONS, SqlV1Translations) \
    XX(DB_KQP_SQL_UNKNOWN_TRANSLATIONS, SqlUnknownTranslations) \
    XX(DB_KQP_QUERY_TYPE_UNDEFINED, QueryTypes[NKikimrKqp::EQueryType::QUERY_TYPE_UNDEFINED]) \
    XX(DB_KQP_QUERY_TYPE_SQL_DML, QueryTypes[NKikimrKqp::EQueryType::QUERY_TYPE_SQL_DML]) \
    XX(DB_KQP_QUERY_TYPE_SQL_DDL, QueryTypes[NKikimrKqp::EQueryType::QUERY_TYPE_SQL_DDL]) \
    XX(DB_KQP_QUERY_TYPE_PREPARED_DML, QueryTypes[NKikimrKqp::EQueryType::QUERY_TYPE_PREPARED_DML]) \
    XX(DB_KQP_QUERY_TYPE_AST_DML, QueryTypes[NKikimrKqp::EQueryType::QUERY_TYPE_AST_DML]) \
    XX(DB_KQP_QUERY_TYPE_SQL_SCRIPT, QueryTypes[NKikimrKqp::EQueryType::QUERY_TYPE_SQL_SCRIPT]) \
    XX(DB_KQP_QUERY_TYPE_SQL_SCRIPT_STREAMING, QueryTypes[NKikimrKqp::EQueryType::QUERY_TYPE_SQL_SCRIPT_STREAMING]) \
    XX(DB_KQP_QUERY_TYPE_SQL_SCAN, QueryTypes[NKikimrKqp::EQueryType::QUERY_TYPE_SQL_SCAN]) \
    XX(DB_KQP_QUERY_TYPE_AST_SCAN, QueryTypes[NKikimrKqp::EQueryType::QUERY_TYPE_AST_SCAN]) \
    XX(DB_KQP_RSP_STATUS_CODE_UNSPECIFIED, YdbResponses[Ydb::StatusIds::STATUS_CODE_UNSPECIFIED]) \
    XX(DB_KQP_RSP_SUCCESS, YdbResponses[Ydb::StatusIds::SUCCESS]) \
    XX(DB_KQP_RSP_BAD_REQUEST, YdbResponses[Ydb::StatusIds::BAD_REQUEST]) \
    XX(DB_KQP_RSP_UNAUTHORIZED, YdbResponses[Ydb::StatusIds::UNAUTHORIZED]) \
    XX(DB_KQP_RSP_INTERNAL_ERROR, YdbResponses[Ydb::StatusIds::INTERNAL_ERROR]) \
    XX(DB_KQP_RSP_ABORTED, YdbResponses[Ydb::StatusIds::ABORTED]) \
    XX(DB_KQP_RSP_UNAVAILABLE, YdbResponses[Ydb::StatusIds::UNAVAILABLE]) \
    XX(DB_KQP_RSP_OVERLOADED, YdbResponses[Ydb::StatusIds::OVERLOADED]) \
    XX(DB_KQP_RSP_SCHEME_ERROR, YdbResponses[Ydb::StatusIds::SCHEME_ERROR]) \
    XX(DB_KQP_RSP_GENERIC_ERROR, YdbResponses[Ydb::StatusIds::GENERIC_ERROR]) \
    XX(DB_KQP_RSP_TIMEOUT, YdbResponses[Ydb::StatusIds::TIMEOUT]) \
    XX(DB_KQP_RSP_BAD_SESSION, YdbResponses[Ydb::StatusIds::BAD_SESSION]) \
    XX(DB_KQP_RSP_PRECONDITION_FAILED, YdbResponses[Ydb::StatusIds::PRECONDITION_FAILED]) \
    XX(DB_KQP_RSP_ALREADY_EXISTS, YdbResponses[Ydb::StatusIds::ALREADY_EXISTS]) \
    XX(DB_KQP_RSP_NOT_FOUND, YdbResponses[Ydb::StatusIds::NOT_FOUND]) \
    XX(DB_KQP_RSP_SESSION_EXPIRED, YdbResponses[Ydb::StatusIds::SESSION_EXPIRED]) \
    XX(DB_KQP_RSP_CANCELLED, YdbResponses[Ydb::StatusIds::CANCELLED]) \
    XX(DB_KQP_RSP_UNDETERMINED, YdbResponses[Ydb::StatusIds::UNDETERMINED]) \
    XX(DB_KQP_RSP_UNSUPPORTED, YdbResponses[Ydb::StatusIds::UNSUPPORTED]) \
    XX(DB_KQP_RSP_SESSION_BUSY, YdbResponses[Ydb::StatusIds::SESSION_BUSY]) \
    XX(DB_KQP_RSP_OTHER, OtherYdbResponses) \
    XX(DB_KQP_RESPONSE_BYTES, ResponseBytes) \
    XX(DB_KQP_QUERY_RESULTS_BYTES, QueryResultsBytes) \
    XX(DB_KQP_WORKERS_CREATED, WorkersCreated) \
    XX(DB_KQP_WORKERS_CLOSED_IDLE, WorkersClosedIdle) \
    XX(DB_KQP_WORKERS_CLOSED_ERROR, WorkersClosedError) \
    XX(DB_KQP_WORKERS_CLOSED_REQUEST, WorkersClosedRequest) \
    XX(DB_KQP_TX_CREATED, TxCreated) \
    XX(DB_KQP_TX_ABORTED, TxAborted) \
    XX(DB_KQP_TX_COMMITED, TxCommited) \
    XX(DB_KQP_TX_EVICTED, TxEvicted) \
    XX(DB_KQP_COMPILE_QUERY_CACHE_HITS, CompileQueryCacheHits) \
    XX(DB_KQP_COMPILE_QUERY_CACHE_MISSES, CompileQueryCacheMisses) \
    XX(DB_KQP_COMPILE_REQ_COMPILE, CompileRequestsCompile) \
    XX(DB_KQP_COMPILE_REQ_GET, CompileRequestsGet) \
    XX(DB_KQP_COMPILE_REQ_INVALIDATE, CompileRequestsInvalidate) \
    XX(DB_KQP_COMPILE_REQ_REJECTED, CompileRequestsRejected) \
    XX(DB_KQP_COMPILE_REQ_TIMEOUT, CompileRequestsTimeout) \
    XX(DB_KQP_COMPILE_TOTAL, CompileTotal) \
    XX(DB_KQP_COMPILE_ERRORS, CompileErrors) \
    XX(DB_KQP_DEPRECATED1, &DeprecatedCounter) \
    XX(DB_KQP_YDB_WORKERS_CLOSED_IDLE, YdbSessionsClosedIdle) \
    XX(DB_KQP_YDB_REQUEST_BYTES, YdbRequestBytes) \
    XX(DB_KQP_YDB_PARAMS_BYTES, YdbParametersBytes) \
    XX(DB_KQP_YDB_RSP_LOCKS_INVALIDATED, YdbResponsesLocksInvalidated) \
    XX(DB_KQP_QUERIES_WITH_RANGE_SCAN, QueriesWithRangeScan) \
    XX(DB_KQP_QUERIES_WITH_FULL_SCAN, QueriesWithFullScan) \
    XX(DB_KQP_YDB_RESPONSE_BYTES, YdbResponseBytes) \
    XX(DB_KQP_CREATE_SESSION_REQ, CreateSessionRequests)

#define DB_KQP_HISTOGRAM_COUNTERS_MAP(XX) \
    XX(DB_KQP_QUERY_LATENCY_EXECUTE, QueryLatencies[NKikimrKqp::QUERY_ACTION_EXECUTE]) \
    XX(DB_KQP_QUERY_LATENCY_EXPLAIN, QueryLatencies[NKikimrKqp::QUERY_ACTION_EXPLAIN]) \
    XX(DB_KQP_QUERY_LATENCY_VALIDATE, QueryLatencies[NKikimrKqp::QUERY_ACTION_VALIDATE]) \
    XX(DB_KQP_QUERY_LATENCY_PREPARE, QueryLatencies[NKikimrKqp::QUERY_ACTION_PREPARE]) \
    XX(DB_KQP_QUERY_LATENCY_EXECUTE_PREPARED, QueryLatencies[NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED]) \
    XX(DB_KQP_QUERY_LATENCY_BEGIN_TX, QueryLatencies[NKikimrKqp::QUERY_ACTION_BEGIN_TX]) \
    XX(DB_KQP_QUERY_LATENCY_COMMIT_TX, QueryLatencies[NKikimrKqp::QUERY_ACTION_COMMIT_TX]) \
    XX(DB_KQP_QUERY_LATENCY_ROLLBACK_TX, QueryLatencies[NKikimrKqp::QUERY_ACTION_ROLLBACK_TX]) \
    XX(DB_KQP_WORKER_LIFE_SPAN, WorkerLifeSpan) \
    XX(DB_KQP_QUERIES_PER_WORKER, QueriesPerWorker) \
    XX(DB_KQP_WORKER_CLEANUP_LATENCY, WorkerCleanupLatency) \
    XX(DB_KQP_TX_ACTIVE_PER_SESSION, TxActivePerSession) \
    XX(DB_KQP_TX_ABORTED_PER_SESSION, TxAbortedPerSession) \
    XX(DB_KQP_PURE_TOTAL_DURATION, YdbTxByKind[TKqpTransactionInfo::EKind::Pure].TotalDuration) \
    XX(DB_KQP_PURE_SERVER_DURATION, YdbTxByKind[TKqpTransactionInfo::EKind::Pure].ServerDuration) \
    XX(DB_KQP_PURE_CLIENT_DURATION, YdbTxByKind[TKqpTransactionInfo::EKind::Pure].ClientDuration) \
    XX(DB_KQP_RO_TOTAL_DURATION, YdbTxByKind[TKqpTransactionInfo::EKind::ReadOnly].TotalDuration) \
    XX(DB_KQP_RO_SERVER_DURATION, YdbTxByKind[TKqpTransactionInfo::EKind::ReadOnly].ServerDuration) \
    XX(DB_KQP_RO_CLIENT_DURATION, YdbTxByKind[TKqpTransactionInfo::EKind::ReadOnly].ClientDuration) \
    XX(DB_KQP_WO_TOTAL_DURATION, YdbTxByKind[TKqpTransactionInfo::EKind::WriteOnly].TotalDuration) \
    XX(DB_KQP_WO_SERVER_DURATION, YdbTxByKind[TKqpTransactionInfo::EKind::WriteOnly].ServerDuration) \
    XX(DB_KQP_WO_CLIENT_DURATION, YdbTxByKind[TKqpTransactionInfo::EKind::WriteOnly].ClientDuration) \
    XX(DB_KQP_RW_TOTAL_DURATION, YdbTxByKind[TKqpTransactionInfo::EKind::ReadWrite].TotalDuration) \
    XX(DB_KQP_RW_SERVER_DURATION, YdbTxByKind[TKqpTransactionInfo::EKind::ReadWrite].ServerDuration) \
    XX(DB_KQP_RW_CLIENT_DURATION, YdbTxByKind[TKqpTransactionInfo::EKind::ReadWrite].ClientDuration) \
    XX(DB_KQP_YDB_COMPILE_DURATION, YdbCompileDuration) \
    XX(DB_KQP_COMPILE_CPU_TIME, CompileCpuTime) \
    XX(DB_KQP_YDB_QUERY_LATENCY_EXECUTE, YdbQueryExecuteLatency) \
    XX(DB_KQP_QUERY_AFFECTED_SHARDS, QueryAffectedShardsCount) \
    XX(DB_KQP_QUERY_READ_SETS, QueryReadSetsCount) \
    XX(DB_KQP_QUERY_READ_BYTES, QueryReadBytes) \
    XX(DB_KQP_QUERY_READ_ROWS, QueryReadRows) \
    XX(DB_KQP_QUERY_MAX_SHARD_REPLY, QueryMaxShardReplySize) \
    XX(DB_KQP_QUERY_MAX_SHARD_PROGRAM, QueryMaxShardProgramSize)
}
}
