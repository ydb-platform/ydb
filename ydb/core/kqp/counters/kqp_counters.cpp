#include "kqp_counters.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/base/counters.h>
#include <ydb/library/ydb_issue/proto/issue_id.pb.h>
#include <ydb/core/sys_view/service/db_counters.h>
#include <ydb/core/sys_view/service/sysview_service.h>

#include <ydb/library/actors/core/log.h>
#include <util/generic/size_literals.h>

#include <ydb/library/yql/core/issue/protos/issue_id.pb.h>
#include <ydb/public/api/protos/ydb_issue_message.pb.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;


::NMonitoring::TDynamicCounterPtr TKqpCountersBase::GetQueryReplayCounters() const {
    return QueryReplayGroup;
}

void TKqpCountersBase::CreateYdbTxKindCounters(TKqpTransactionInfo::EKind kind, const TString& name) {
    auto txKindGroup = YdbGroup->GetSubgroup("tx_kind", name);

    auto& ydbTxCounters = YdbTxByKind[kind];
    ydbTxCounters.TotalDuration = txKindGroup->GetNamedHistogram("name",
        "table.transaction.total_duration_milliseconds", NMonitoring::ExponentialHistogram(20, 2, 1));
    ydbTxCounters.ServerDuration = txKindGroup->GetNamedHistogram("name",
        "table.transaction.server_duration_milliseconds", NMonitoring::ExponentialHistogram(20, 2, 1));
    ydbTxCounters.ClientDuration = txKindGroup->GetNamedHistogram("name",
        "table.transaction.client_duration_milliseconds", NMonitoring::ExponentialHistogram(20, 2, 1));
}

void TKqpCountersBase::UpdateYdbTxCounters(const TKqpTransactionInfo& txInfo,
    THashMap<TKqpTransactionInfo::EKind, TYdbTxByKindCounters>& txCounters)
{
    auto byKind = txCounters.FindPtr(txInfo.Kind);
    if (!byKind) {
        return;
    }

    byKind->TotalDuration->Collect(txInfo.TotalDuration.MilliSeconds());
    byKind->ServerDuration->Collect(txInfo.ServerDuration.MilliSeconds());
    auto clientDuration = txInfo.TotalDuration - txInfo.ServerDuration;
    byKind->ClientDuration->Collect(clientDuration.MilliSeconds());
}

void TKqpCountersBase::Init() {
    /* Requests */
    QueryActionRequests[NKikimrKqp::QUERY_ACTION_EXECUTE] =
        KqpGroup->GetCounter("Requests/QueryExecute", true);
    QueryActionRequests[NKikimrKqp::QUERY_ACTION_EXPLAIN] =
        KqpGroup->GetCounter("Requests/QueryExplain", true);
    QueryActionRequests[NKikimrKqp::QUERY_ACTION_VALIDATE] =
        KqpGroup->GetCounter("Requests/QueryValidate", true);
    QueryActionRequests[NKikimrKqp::QUERY_ACTION_PREPARE] =
        KqpGroup->GetCounter("Requests/QueryPrepare", true);
    QueryActionRequests[NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED] =
        KqpGroup->GetCounter("Requests/QueryExecPrepared", true);
    QueryActionRequests[NKikimrKqp::QUERY_ACTION_BEGIN_TX] =
        KqpGroup->GetCounter("Requests/QueryBeginTx", true);
    QueryActionRequests[NKikimrKqp::QUERY_ACTION_COMMIT_TX] =
        KqpGroup->GetCounter("Requests/QueryCommitTx", true);
    QueryActionRequests[NKikimrKqp::QUERY_ACTION_ROLLBACK_TX] =
        KqpGroup->GetCounter("Requests/QueryRollbackTx", true);
    QueryActionRequests[NKikimrKqp::QUERY_ACTION_PARSE] =
        KqpGroup->GetCounter("Requests/QueryParse", true);
    OtherQueryRequests = KqpGroup->GetCounter("Requests/QueryOther", true);

    CloseSessionRequests = KqpGroup->GetCounter("Requests/CloseSession", true);
    CreateSessionRequests = KqpGroup->GetCounter("Requests/CreateSession", true);
    PingSessionRequests = KqpGroup->GetCounter("Requests/PingSession", true);
    CancelQueryRequests = KqpGroup->GetCounter("Requests/CancelQuery", true);

    RequestBytes = KqpGroup->GetCounter("Requests/Bytes", true);
    YdbRequestBytes = YdbGroup->GetNamedCounter("name", "table.query.request.bytes", true);
    QueryBytes = KqpGroup->GetCounter("Requests/QueryBytes", true);
    ParametersBytes = KqpGroup->GetCounter("Requests/ParametersBytes", true);
    YdbParametersBytes = YdbGroup->GetNamedCounter("name", "table.query.request.parameters_bytes", true);

    SqlV0Translations = KqpGroup->GetCounter("Requests/Sql/V0", true);
    SqlV1Translations = KqpGroup->GetCounter("Requests/Sql/V1", true);
    SqlUnknownTranslations = KqpGroup->GetCounter("Requests/Sql/Unknown", true);

    QueryTypes[NKikimrKqp::EQueryType::QUERY_TYPE_UNDEFINED] =
        KqpGroup->GetCounter("Request/QueryTypeUndefined", true);
    QueryTypes[NKikimrKqp::EQueryType::QUERY_TYPE_SQL_DML] =
        KqpGroup->GetCounter("Request/QueryTypeDml", true);
    QueryTypes[NKikimrKqp::EQueryType::QUERY_TYPE_SQL_DDL] =
        KqpGroup->GetCounter("Request/QueryTypeDdl", true);
    QueryTypes[NKikimrKqp::EQueryType::QUERY_TYPE_PREPARED_DML] =
        KqpGroup->GetCounter("Request/QueryTypePreparedDml", true);
    QueryTypes[NKikimrKqp::EQueryType::QUERY_TYPE_AST_DML] =
        KqpGroup->GetCounter("Request/QueryTypeAstDml", true);
    QueryTypes[NKikimrKqp::EQueryType::QUERY_TYPE_SQL_SCRIPT] =
        KqpGroup->GetCounter("Request/QueryTypeSqlScript", true);
    QueryTypes[NKikimrKqp::EQueryType::QUERY_TYPE_SQL_SCRIPT_STREAMING] =
        KqpGroup->GetCounter("Request/QueryTypeSqlScriptStreaming", true);
    QueryTypes[NKikimrKqp::EQueryType::QUERY_TYPE_SQL_SCAN] =
        KqpGroup->GetCounter("Request/QueryTypeSqlScan", true);
    QueryTypes[NKikimrKqp::EQueryType::QUERY_TYPE_AST_SCAN] =
        KqpGroup->GetCounter("Request/QueryTypeAstScan", true);
    QueryTypes[NKikimrKqp::EQueryType::QUERY_TYPE_SQL_GENERIC_QUERY] =
        KqpGroup->GetCounter("Request/QueryTypeGenericQuery", true);
    QueryTypes[NKikimrKqp::EQueryType::QUERY_TYPE_SQL_GENERIC_CONCURRENT_QUERY] =
        KqpGroup->GetCounter("Request/QueryTypeGenericConcurrentQuery", true);
    QueryTypes[NKikimrKqp::EQueryType::QUERY_TYPE_SQL_GENERIC_SCRIPT] =
        KqpGroup->GetCounter("Request/QueryTypeGenericScript", true);
    OtherQueryTypes = KqpGroup->GetCounter("Requests/QueryTypeOther", true);

    QueriesWithRangeScan = KqpGroup->GetCounter("Query/WithRangeScan", true);
    QueriesWithFullScan = KqpGroup->GetCounter("Query/WithFullScan", true);

    QueryAffectedShardsCount = KqpGroup->GetHistogram("Query/AffectedShards",
        NMonitoring::ExplicitHistogram({1, 9, 49, 99, 499, 999}));
    QueryReadSetsCount = KqpGroup->GetHistogram("Query/ReadSets",
        NMonitoring::ExplicitHistogram({99, 999, 9999, 99999}));
    QueryReadBytes = KqpGroup->GetHistogram("Query/ReadBytes",
        NMonitoring::ExplicitHistogram({1_MB, 9_MB, 99_MB, 999_MB}));
    QueryReadRows = KqpGroup->GetHistogram("Query/ReadRows",
        NMonitoring::ExplicitHistogram({1, 99, 999, 9999, 99999, 999999}));
    QueryMaxShardReplySize = KqpGroup->GetHistogram("Query/MaxShardReplySize",
        NMonitoring::ExplicitHistogram({1_MB, 9_MB, 29_MB}));
    QueryMaxShardProgramSize = KqpGroup->GetHistogram("Query/MaxShardProgramSize",
        NMonitoring::ExplicitHistogram({1_MB, 9_MB, 29_MB}));

    /* Request latency */
    QueryLatencies[NKikimrKqp::QUERY_ACTION_EXECUTE] = KqpGroup->GetHistogram(
        "Query/ExecuteLatencyMs", NMonitoring::ExponentialHistogram(20, 2, 1));
    QueryLatencies[NKikimrKqp::QUERY_ACTION_EXPLAIN] = KqpGroup->GetHistogram(
        "Query/ExplainLatencyMs", NMonitoring::ExponentialHistogram(10, 2, 1));
    QueryLatencies[NKikimrKqp::QUERY_ACTION_VALIDATE] = KqpGroup->GetHistogram(
        "Query/ValidateLatencyMs", NMonitoring::ExponentialHistogram(10, 2, 1));
    QueryLatencies[NKikimrKqp::QUERY_ACTION_PREPARE] = KqpGroup->GetHistogram(
        "Query/PrepareLatencyMs", NMonitoring::ExponentialHistogram(16, 2, 1));
    QueryLatencies[NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED] = KqpGroup->GetHistogram(
        "Query/ExecPreparedLatencyMs", NMonitoring::ExponentialHistogram(20, 2, 1));
    QueryLatencies[NKikimrKqp::QUERY_ACTION_BEGIN_TX] = KqpGroup->GetHistogram(
        "Query/BeginTxLatencyMs", NMonitoring::ExponentialHistogram(10, 2, 1));
    QueryLatencies[NKikimrKqp::QUERY_ACTION_COMMIT_TX] = KqpGroup->GetHistogram(
        "Query/CommitTxLatencyMs", NMonitoring::ExponentialHistogram(20, 2, 1));
    QueryLatencies[NKikimrKqp::QUERY_ACTION_ROLLBACK_TX] = KqpGroup->GetHistogram(
        "Query/RollbackTxLatencyMs", NMonitoring::ExponentialHistogram(10, 2, 1));

    YdbQueryExecuteLatency = YdbGroup->GetNamedHistogram("name",
        "table.query.execution.latency_milliseconds", NMonitoring::ExponentialHistogram(20, 2, 1));

    // TODO move to grpc
    YdbResponsesLocksInvalidated = YdbGroup->GetSubgroup("issue_type", "optimistic_locks_invalidation")
        ->GetNamedCounter("name", "api.grpc.response.issues", true);

    /* Response statuses */
    YdbResponses[Ydb::StatusIds::STATUS_CODE_UNSPECIFIED] = KqpGroup->GetCounter("YdbResponses/Unspecified", true);
    YdbResponses[Ydb::StatusIds::SUCCESS] = KqpGroup->GetCounter("YdbResponses/Success", true);
    YdbResponses[Ydb::StatusIds::BAD_REQUEST] = KqpGroup->GetCounter("YdbResponses/BadRequest", true);
    YdbResponses[Ydb::StatusIds::UNAUTHORIZED] = KqpGroup->GetCounter("YdbResponses/Unauthorized", true);
    YdbResponses[Ydb::StatusIds::INTERNAL_ERROR] = KqpGroup->GetCounter("YdbResponses/InternalError", true);
    YdbResponses[Ydb::StatusIds::ABORTED] = KqpGroup->GetCounter("YdbResponses/Aborted", true);
    YdbResponses[Ydb::StatusIds::UNAVAILABLE] = KqpGroup->GetCounter("YdbResponses/Unavailable", true);
    YdbResponses[Ydb::StatusIds::OVERLOADED] = KqpGroup->GetCounter("YdbResponses/Overloaded", true);
    YdbResponses[Ydb::StatusIds::SCHEME_ERROR] = KqpGroup->GetCounter("YdbResponses/SchemeError", true);
    YdbResponses[Ydb::StatusIds::GENERIC_ERROR] = KqpGroup->GetCounter("YdbResponses/GenericError", true);
    YdbResponses[Ydb::StatusIds::TIMEOUT] = KqpGroup->GetCounter("YdbResponses/Timeout", true);
    YdbResponses[Ydb::StatusIds::BAD_SESSION] = KqpGroup->GetCounter("YdbResponses/BadSession", true);
    YdbResponses[Ydb::StatusIds::PRECONDITION_FAILED] = KqpGroup->GetCounter("YdbResponses/PreconditionFailed", true);
    YdbResponses[Ydb::StatusIds::ALREADY_EXISTS] = KqpGroup->GetCounter("YdbResponses/AlreadyExists", true);
    YdbResponses[Ydb::StatusIds::NOT_FOUND] = KqpGroup->GetCounter("YdbResponses/NotFound", true);
    YdbResponses[Ydb::StatusIds::SESSION_EXPIRED] = KqpGroup->GetCounter("YdbResponses/SessionExpired", true);
    YdbResponses[Ydb::StatusIds::CANCELLED] = KqpGroup->GetCounter("YdbResponses/Cancelled", true);
    YdbResponses[Ydb::StatusIds::UNDETERMINED] = KqpGroup->GetCounter("YdbResponses/Undetermined", true);
    YdbResponses[Ydb::StatusIds::UNSUPPORTED] = KqpGroup->GetCounter("YdbResponses/Unsupported", true);
    YdbResponses[Ydb::StatusIds::SESSION_BUSY] = KqpGroup->GetCounter("YdbResponses/SessionBusy", true);
    OtherYdbResponses = KqpGroup->GetCounter("YdbResponses/Other", true);

    ResponseBytes = KqpGroup->GetCounter("Responses/Bytes", true);
    YdbResponseBytes = YdbGroup->GetNamedCounter("name", "table.query.response.bytes", true);
    QueryResultsBytes = KqpGroup->GetCounter("Responses/QueryResultBytes", true);

    /* Workers */
    WorkerLifeSpan = KqpGroup->GetHistogram(
        "Workers/LifeSpanMs", NMonitoring::ExponentialHistogram(20, 2, 1));
    QueriesPerWorker = KqpGroup->GetHistogram(
        "Workers/QueriesPerWorkerQ", NMonitoring::ExponentialHistogram(16, 2, 1));

    WorkersCreated = KqpGroup->GetCounter("Workers/Created", true);
    WorkersClosedIdle = KqpGroup->GetCounter("Workers/ClosedIdle", true);
    WorkersClosedError = KqpGroup->GetCounter("Workers/ClosedError", true);
    WorkersClosedRequest = KqpGroup->GetCounter("Workers/ClosedRequest", true);
    ActiveWorkers = KqpGroup->GetCounter("Workers/Active", false);
    ProxyForwardedRequests = KqpGroup->GetCounter("Proxy/Forwarded", true);

    WorkerCleanupLatency = KqpGroup->GetHistogram(
        "Workers/CleanupLatencyMs", NMonitoring::ExponentialHistogram(10, 2, 1));

    /* common for worker and session actors */
    YdbSessionsActiveCount = YdbGroup->GetNamedCounter("name", "table.session.active_count", false);
    YdbSessionsClosedIdle = YdbGroup->GetNamedCounter("name", "table.session.closed_by_idle_count", true);

    /* SessionActors */
    SessionActorLifeSpan = KqpGroup->GetHistogram(
        "SessionActors/LifeSpanMs", NMonitoring::ExponentialHistogram(20, 2, 1));
    QueriesPerSessionActor = KqpGroup->GetHistogram(
        "SessionActors/QueriesPerSessionActorQ", NMonitoring::ExponentialHistogram(16, 2, 1));

    SessionActorsCreated = KqpGroup->GetCounter("SessionActors/Created", true);
    SessionActorsClosedIdle = KqpGroup->GetCounter("SessionActors/ClosedIdle", true);
    SessionActorsClosedError = KqpGroup->GetCounter("SessionActors/ClosedError", true);
    SessionActorsClosedRequest = KqpGroup->GetCounter("SessionActors/ClosedRequest", true);
    ActiveSessionActors = KqpGroup->GetCounter("SessionActors/Active", false);
    SessionActorCleanupLatency = KqpGroup->GetHistogram(
        "SessionActors/CleanupLatencyMs", NMonitoring::ExponentialHistogram(10, 2, 1));

    SessionBalancerShutdowns = KqpGroup->GetCounter("SessionBalancer/Shutdown", true);
    SessionGracefulShutdownHit = KqpGroup->GetCounter("SessionBalancer/GracefulHit", true);

    /* Transactions */
    TxCreated = KqpGroup->GetCounter("Transactions/Created", true);
    TxAborted = KqpGroup->GetCounter("Transactions/Aborted", true);
    TxCommited = KqpGroup->GetCounter("Transactions/Commited", true);
    TxEvicted = KqpGroup->GetCounter("Transactions/Evicted", true);

    TxActivePerSession = KqpGroup->GetHistogram(
        "Transactions/TxActivePerSession", NMonitoring::ExponentialHistogram(16, 2, 1));
    TxAbortedPerSession = KqpGroup->GetHistogram(
        "Transactions/TxAbortedPerSession", NMonitoring::ExponentialHistogram(16, 2, 1));

    CompileRequestsCompile = KqpGroup->GetCounter("Compilation/Requests/Compile", true);
    CompileRequestsGet = KqpGroup->GetCounter("Compilation/Requests/Get", true);
    CompileRequestsInvalidate = KqpGroup->GetCounter("Compilation/Requests/Invalidate", true);
    CompileRequestsRejected = KqpGroup->GetCounter("Compilation/Requests/Rejected", true);
    CompileRequestsTimeout = KqpGroup->GetCounter("Compilation/Requests/Timeout", true);
    CompileRequestsRecompile = KqpGroup->GetCounter("Compilation/Requests/Recompile", true);

    CompileCpuTime = KqpGroup->GetHistogram(
        "Compilation/CPUTimeMs", NMonitoring::ExponentialHistogram(20, 2, 1));

    CreateYdbTxKindCounters(TKqpTransactionInfo::EKind::Pure, "pure");
    CreateYdbTxKindCounters(TKqpTransactionInfo::EKind::ReadOnly, "read_only");
    CreateYdbTxKindCounters(TKqpTransactionInfo::EKind::WriteOnly, "write_only");
    CreateYdbTxKindCounters(TKqpTransactionInfo::EKind::ReadWrite, "read_write");

    CompileQueryCacheHits = YdbGroup->GetNamedCounter("name", "table.query.compilation.cache_hits", true);
    CompileQueryCacheMisses = YdbGroup->GetNamedCounter("name", "table.query.compilation.cache_misses", true);

    CompileTotal = YdbGroup->GetNamedCounter("name", "table.query.compilation.count", true);
    CompileErrors = YdbGroup->GetNamedCounter("name", "table.query.compilation.error_count", true);
    CompileActive = YdbGroup->GetNamedCounter("name", "table.query.compilation.active_count", false);

    YdbCompileDuration = YdbGroup->GetNamedHistogram("name",
        "table.query.compilation.latency_milliseconds", NMonitoring::ExponentialHistogram(20, 2, 1));
}

void TKqpCountersBase::ReportQueryAction(NKikimrKqp::EQueryAction action) {
    auto counter = QueryActionRequests.FindPtr(action);
    if (counter) {
        (*counter)->Inc();
        return;
    }

    OtherQueryRequests->Inc();
}

void TKqpCountersBase::ReportQueryType(NKikimrKqp::EQueryType type) {
    auto counter = QueryTypes.FindPtr(type);
    if (counter) {
        (*counter)->Inc();
        return;
    }

    OtherQueryTypes->Inc();
}

void TKqpCountersBase::ReportSessionShutdownRequest() {
    SessionBalancerShutdowns->Inc();
}

void TKqpCountersBase::ReportSessionGracefulShutdownHit() {
    SessionGracefulShutdownHit->Inc();
}

void TKqpCountersBase::ReportCreateSession(ui64 requestSize) {
    CreateSessionRequests->Inc();
    *RequestBytes += requestSize;
    *YdbRequestBytes += requestSize;
}

void TKqpCountersBase::ReportPingSession(ui64 requestSize) {
    PingSessionRequests->Inc();
    *RequestBytes += requestSize;
    *YdbRequestBytes += requestSize;
}

void TKqpCountersBase::ReportCloseSession(ui64 requestSize) {
    CloseSessionRequests->Inc();
    *RequestBytes += requestSize;
    *YdbRequestBytes += requestSize;
}

void TKqpCountersBase::ReportQueryRequest(ui64 requestBytes, ui64 parametersBytes, ui64 queryBytes) {
    *RequestBytes += requestBytes;
    *YdbRequestBytes += requestBytes;

    *QueryBytes += queryBytes;

    *ParametersBytes += parametersBytes;
    *YdbParametersBytes += parametersBytes;
}

void TKqpCountersBase::ReportCancelQuery(ui64 requestSize) {
    CancelQueryRequests->Inc();
    *RequestBytes += requestSize;
    *YdbRequestBytes += requestSize;
}

void TKqpCountersBase::ReportQueryWithRangeScan() {
    QueriesWithRangeScan->Inc();
}

void TKqpCountersBase::ReportQueryWithFullScan() {
    QueriesWithFullScan->Inc();
}

void TKqpCountersBase::ReportQueryAffectedShards(ui64 shardsCount) {
    QueryAffectedShardsCount->Collect(shardsCount > Max<i64>() ? Max<i64>() : static_cast<i64>(shardsCount));
}

void TKqpCountersBase::ReportQueryReadSets(ui64 readSetsCount) {
    QueryReadSetsCount->Collect(readSetsCount > Max<i64>() ? Max<i64>() : static_cast<i64>(readSetsCount));
}

void TKqpCountersBase::ReportQueryReadBytes(ui64 bytesCount) {
    QueryReadBytes->Collect(bytesCount > Max<i64>() ? Max<i64>() : static_cast<i64>(bytesCount));
}

void TKqpCountersBase::ReportQueryReadRows(ui64 rowsCount) {
    QueryReadRows->Collect(rowsCount > Max<i64>() ? Max<i64>() : static_cast<i64>(rowsCount));
}

void TKqpCountersBase::ReportQueryMaxShardReplySize(ui64 replySize) {
    QueryMaxShardReplySize->Collect(replySize > Max<i64>() ? Max<i64>() : static_cast<i64>(replySize));
}

void TKqpCountersBase::ReportQueryMaxShardProgramSize(ui64 programSize) {
    QueryMaxShardProgramSize->Collect(programSize > Max<i64>() ? Max<i64>() : static_cast<i64>(programSize));
}

void TKqpCountersBase::ReportResponseStatus(ui64 responseSize, Ydb::StatusIds::StatusCode ydbStatus) {
    *ResponseBytes += responseSize;
    *YdbResponseBytes += responseSize;

    auto ydbCounter = YdbResponses.FindPtr(ydbStatus);
    if (ydbCounter) {
        (*ydbCounter)->Inc();
    } else {
        OtherYdbResponses->Inc();
    }
}

void TKqpCountersBase::ReportResultsBytes(ui64 resultsBytes) {
    *QueryResultsBytes += resultsBytes;
}

TString TKqpCountersBase::GetIssueName(ui32 issueCode) {
    auto kikimrIssueDescriptor = NKikimrIssues::TIssuesIds::EIssueCode_descriptor();
    if (kikimrIssueDescriptor) {
        auto valueDescriptor = kikimrIssueDescriptor->FindValueByNumber(issueCode);
        if (valueDescriptor) {
            return TStringBuilder() << "KIKIMR:" << valueDescriptor->name();
        }
    }

    auto yqlIssueDescriptor = NYql::TIssuesIds::EIssueCode_descriptor();
    if (yqlIssueDescriptor) {
        auto valueDescriptor = yqlIssueDescriptor->FindValueByNumber(issueCode);
        if (valueDescriptor) {
            return TStringBuilder() << "YQL:" << valueDescriptor->name();
        }
    }

    return TStringBuilder() << "CODE:" << ToString(issueCode);
}

void TKqpCountersBase::ReportIssues(
    THashMap<ui32, ::NMonitoring::TDynamicCounters::TCounterPtr>& issueCounters,
    const Ydb::Issue::IssueMessage& issue)
{
    auto issueCounter = issueCounters.FindPtr(issue.issue_code());
    if (!issueCounter) {
        auto counterName = TStringBuilder() << "Issues/" << GetIssueName(issue.issue_code());
        auto counter = KqpGroup->GetCounter(counterName , true);

        auto result = issueCounters.emplace(issue.issue_code(), counter);
        issueCounter = &result.first->second;
    }

    (*issueCounter)->Inc();

    if (issue.issue_code() == TIssuesIds::KIKIMR_LOCKS_INVALIDATED) {
        YdbResponsesLocksInvalidated->Inc();
    }

    for (auto& childIssue : issue.issues()) {
        ReportIssues(issueCounters, childIssue);
    }
}

void TKqpCountersBase::ReportQueryLatency(NKikimrKqp::EQueryAction action, const TDuration& duration) {
    if (action == NKikimrKqp::QUERY_ACTION_EXECUTE ||
        action == NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED ||
        action == NKikimrKqp::QUERY_ACTION_COMMIT_TX ||
        action == NKikimrKqp::QUERY_ACTION_ROLLBACK_TX)
    {
        YdbQueryExecuteLatency->Collect(duration.MilliSeconds());
    }

    auto counter = QueryLatencies.FindPtr(action);
    if (counter) {
        (*counter)->Collect(duration.MilliSeconds());
    }
}


void TKqpCountersBase::ReportTransaction(const TKqpTransactionInfo& txInfo) {
    switch (txInfo.Status) {
        case TKqpTransactionInfo::EStatus::Active:
            return;
        case TKqpTransactionInfo::EStatus::Aborted:
            TxAborted->Inc();
            return;
        case TKqpTransactionInfo::EStatus::Committed:
            TxCommited->Inc();
            break;
    }
    UpdateYdbTxCounters(txInfo, YdbTxByKind);
}

void TKqpCountersBase::ReportSqlVersion(ui16 sqlVersion) {
    switch (sqlVersion) {
        case 0:
            SqlV0Translations->Inc();
            break;
        case 1:
            SqlV1Translations->Inc();
            break;
        default:
            SqlUnknownTranslations->Inc();
            break;
    }
}

void TKqpCountersBase::ReportWorkerCreated() {
    WorkersCreated->Inc();
    ActiveWorkers->Inc();
    YdbSessionsActiveCount->Inc();
}


void TKqpCountersBase::ReportProxyForwardedRequest() {
    ProxyForwardedRequests->Inc();
}

void TKqpCountersBase::ReportWorkerFinished(TDuration lifeSpan) {
    WorkerLifeSpan->Collect(lifeSpan.MilliSeconds());
    ActiveWorkers->Dec();
    YdbSessionsActiveCount->Dec();
}

void TKqpCountersBase::ReportWorkerCleanupLatency(TDuration cleanupTime) {
    WorkerCleanupLatency->Collect(cleanupTime.MilliSeconds());
}

void TKqpCountersBase::ReportWorkerClosedIdle() {
    WorkersClosedIdle->Inc();
    YdbSessionsClosedIdle->Inc();
}

void TKqpCountersBase::ReportWorkerClosedError() {
    WorkersClosedError->Inc();
}

void TKqpCountersBase::ReportWorkerClosedRequest() {
    WorkersClosedRequest->Inc();
}

void TKqpCountersBase::ReportQueriesPerWorker(ui32 queryId) {
    QueriesPerWorker->Collect(queryId);
}

void TKqpCountersBase::ReportSessionActorCreated() {
    SessionActorsCreated->Inc();
    ActiveSessionActors->Inc();
    YdbSessionsActiveCount->Inc();
}

void TKqpCountersBase::ReportSessionActorClosedIdle() {
    SessionActorsClosedIdle->Inc();
    YdbSessionsClosedIdle->Inc();
}

void TKqpCountersBase::ReportSessionActorFinished(TDuration lifeSpan) {
    SessionActorLifeSpan->Collect(lifeSpan.MilliSeconds());
    ActiveSessionActors->Dec();
    YdbSessionsActiveCount->Dec();
}

void TKqpCountersBase::ReportSessionActorCleanupLatency(TDuration cleanupTime) {
    SessionActorCleanupLatency->Collect(cleanupTime.MilliSeconds());
}

void TKqpCountersBase::ReportSessionActorClosedError() {
    SessionActorsClosedError->Inc();
}

void TKqpCountersBase::ReportSessionActorClosedRequest() {
    SessionActorsClosedRequest->Inc();
}

void TKqpCountersBase::ReportQueriesPerSessionActor(ui32 queryId) {
    QueriesPerSessionActor->Collect(queryId);
}

void TKqpCountersBase::ReportBeginTransaction(ui32 evictedTx, ui32 currentActiveTx, ui32 currentAbortedTx) {
    TxEvicted->Add(evictedTx);
    TxAborted->Add(evictedTx);
    TxActivePerSession->Collect(currentActiveTx);
    TxAbortedPerSession->Collect(currentAbortedTx);
}

void TKqpCountersBase::ReportTxCreated() {
    TxCreated->Inc();
}

void TKqpCountersBase::ReportTxAborted(ui32 abortedCount) {
    TxAborted->Add(abortedCount);
}

void TKqpCountersBase::ReportQueryCacheHit(bool hit) {
    if (hit) {
        CompileQueryCacheHits->Inc();
    } else {
        CompileQueryCacheMisses->Inc();
    }
}

void TKqpCountersBase::ReportCompileStart() {
    CompileTotal->Inc();
    CompileActive->Inc();
}

void TKqpCountersBase::ReportCompileFinish() {
    CompileActive->Dec();
}

void TKqpCountersBase::ReportCompileError() {
    CompileErrors->Inc();
}

void TKqpCountersBase::ReportCompileRequestCompile() {
    CompileRequestsCompile->Inc();
}

void TKqpCountersBase::ReportCompileRequestGet() {
    CompileRequestsGet->Inc();
}

void TKqpCountersBase::ReportCompileRequestInvalidate() {
    CompileRequestsInvalidate->Inc();
}

void TKqpCountersBase::ReportCompileRequestRejected() {
    CompileRequestsRejected->Inc();
}

void TKqpCountersBase::ReportCompileRequestTimeout() {
    CompileRequestsTimeout->Inc();
}

void TKqpCountersBase::ReportCompileDurations(TDuration duration, TDuration cpuTime) {
    YdbCompileDuration->Collect(duration.MilliSeconds());
    CompileCpuTime->Collect(cpuTime.MilliSeconds());
}

void TKqpCountersBase::ReportRecompileRequestGet() {
    CompileRequestsRecompile->Inc();
}


TKqpDbCounters::TKqpDbCounters() {
    Counters = new ::NMonitoring::TDynamicCounters();
    KqpGroup = Counters->GetSubgroup("group", "kqp");
    YdbGroup = Counters->GetSubgroup("group", "ydb");

    Init();
}

TKqpDbCounters::TKqpDbCounters(const ::NMonitoring::TDynamicCounterPtr& externalGroup,
    const ::NMonitoring::TDynamicCounterPtr& internalGroup)
{
    Counters = internalGroup;
    KqpGroup = Counters;
    YdbGroup = externalGroup;
    QueryReplayGroup = KqpGroup->GetSubgroup("subsystem", "unified_agent_query_replay");

    Init();
}

template <typename T>
void SaveHistogram(T& histogram, int index, const NMonitoring::THistogramPtr& hgram) {
    auto* buckets = histogram[index].MutableBuckets();
    auto snapshot = hgram->Snapshot();
    auto count = snapshot->Count();
    buckets->Resize(count, 0);
    for (size_t i = 0; i < count; ++i) {
        (*buckets)[i] = snapshot->Value(i);
    }
}

void TKqpDbCounters::ToProto(NKikimr::NSysView::TDbServiceCounters& counters) {
    auto* main = counters.Proto().MutableMain();
    auto* simple = main->MutableSimple();
    auto* cumulative = main->MutableCumulative();
    auto* histogram = main->MutableHistogram();

    simple->Resize(DB_KQP_SIMPLE_COUNTER_SIZE, 0);
    cumulative->Resize(DB_KQP_CUMULATIVE_COUNTER_SIZE, 0);
    if (main->HistogramSize() < DB_KQP_HISTOGRAM_COUNTER_SIZE) {
        auto missing = DB_KQP_HISTOGRAM_COUNTER_SIZE - main->HistogramSize();
        for (; missing > 0; --missing) {
            main->AddHistogram();
        }
    }

#define SAVE_SIMPLE_COUNTER(INDEX, TARGET) { (*simple)[INDEX] = (TARGET)->Val(); }
#define SAVE_CUMULATIVE_COUNTER(INDEX, TARGET) { (*cumulative)[INDEX] = (TARGET)->Val(); }
#define SAVE_HISTOGRAM_COUNTER(INDEX, TARGET) { SaveHistogram(*histogram, INDEX, (TARGET)); }

    DB_KQP_SIMPLE_COUNTERS_MAP(SAVE_SIMPLE_COUNTER)
    DB_KQP_CUMULATIVE_COUNTERS_MAP(SAVE_CUMULATIVE_COUNTER)
    DB_KQP_HISTOGRAM_COUNTERS_MAP(SAVE_HISTOGRAM_COUNTER)
}

template <typename T>
void LoadHistogram(T& histogram, int index, NMonitoring::THistogramPtr& hgram) {
    auto* buckets = histogram[index].MutableBuckets();
    auto snapshot = hgram->Snapshot();
    auto count = snapshot->Count();
    buckets->Resize(count, 0);
    hgram->Reset();
    for (ui32 i = 0; i < count; ++i) {
        hgram->Collect(snapshot->UpperBound(i), (*buckets)[i]);
    }
}

void TKqpDbCounters::FromProto(NKikimr::NSysView::TDbServiceCounters& counters) {
    auto* main = counters.Proto().MutableMain();
    auto* simple = main->MutableSimple();
    auto* cumulative = main->MutableCumulative();
    auto* histogram = main->MutableHistogram();

    simple->Resize(DB_KQP_SIMPLE_COUNTER_SIZE, 0);
    cumulative->Resize(DB_KQP_CUMULATIVE_COUNTER_SIZE, 0);
    if (main->HistogramSize() < DB_KQP_HISTOGRAM_COUNTER_SIZE) {
        auto missing = DB_KQP_HISTOGRAM_COUNTER_SIZE - main->HistogramSize();
        for (; missing > 0; --missing) {
            main->AddHistogram();
        }
    }

#define LOAD_SIMPLE_COUNTER(INDEX, TARGET) { (TARGET)->Set((*simple)[INDEX]); }
#define LOAD_CUMULATIVE_COUNTER(INDEX, TARGET) { (TARGET)->Set((*cumulative)[INDEX]); }
#define LOAD_HISTOGRAM_COUNTER(INDEX, TARGET) { LoadHistogram(*histogram, INDEX, (TARGET)); }

    DB_KQP_SIMPLE_COUNTERS_MAP(LOAD_SIMPLE_COUNTER)
    DB_KQP_CUMULATIVE_COUNTERS_MAP(LOAD_CUMULATIVE_COUNTER)
    DB_KQP_HISTOGRAM_COUNTERS_MAP(LOAD_HISTOGRAM_COUNTER)
}


class TKqpDbWatcherCallback : public NKikimr::NSysView::TDbWatcherCallback {
    TIntrusivePtr<TKqpCounters> Counters;

public:
    explicit TKqpDbWatcherCallback(TIntrusivePtr<TKqpCounters> counters)
        : Counters(counters)
    {}

    void OnDatabaseRemoved(const TString& database, TPathId) override {
        Counters->RemoveDbCounters(database);
    }
};


void TKqpCounters::CreateTxKindCounters(TKqpTransactionInfo::EKind kind, const TString& name) {
    auto& txKindCounters = TxByKind[kind];

    txKindCounters.TotalDuration = KqpGroup->GetHistogram("Transactions/" + name + "/TotalDurationMs",
        NMonitoring::ExponentialHistogram(20, 2, 1));

    txKindCounters.ServerDuration = KqpGroup->GetHistogram("Transactions/" + name + "/ServerDurationMs",
        NMonitoring::ExponentialHistogram(20, 2, 1));

    txKindCounters.ClientDuration = KqpGroup->GetHistogram("Transactions/" + name + "/ClientDurationMs",
        NMonitoring::ExponentialHistogram(20, 2, 1));

    txKindCounters.Queries = KqpGroup->GetHistogram("Transactions/" + name + "/Queries",
        NMonitoring::ExponentialHistogram(16, 2, 1));
}

void TKqpCounters::UpdateTxCounters(const TKqpTransactionInfo& txInfo,
    THashMap<TKqpTransactionInfo::EKind, TTxByKindCounters>& txCounters)
{
    auto byKind = txCounters.FindPtr(txInfo.Kind);
    if (!byKind) {
        return;
    }

    byKind->TotalDuration->Collect(txInfo.TotalDuration.MilliSeconds());
    byKind->ServerDuration->Collect(txInfo.ServerDuration.MilliSeconds());
    byKind->ClientDuration->Collect((txInfo.TotalDuration - txInfo.ServerDuration).MilliSeconds());
    byKind->Queries->Collect(txInfo.QueriesCount);
}

TKqpCounters::TKqpCounters(const ::NMonitoring::TDynamicCounterPtr& counters, const TActorContext* ctx)
    : NYql::NDq::TSpillingCounters(GetServiceCounters(counters, "kqp"))
    , AllocCounters(counters, "kqp")
{
    Counters = counters;
    KqpGroup = GetServiceCounters(counters, "kqp");
    YdbGroup = GetServiceCounters(counters, "ydb");
    QueryReplayGroup = KqpGroup->GetSubgroup("subsystem", "unified_agent_query_replay");
    WorkloadManagerGroup = KqpGroup->GetSubgroup("subsystem", "workload_manager");

    Init();

    if (ctx) {
        ActorSystem = ctx->ActorSystem();
        if (AppData(ActorSystem)->FeatureFlags.GetEnableDbCounters()) {
            auto callback = MakeIntrusive<TKqpDbWatcherCallback>(this);
            DbWatcherActorId = ActorSystem->Register(NSysView::CreateDbWatcherActor(callback));
        }
    }

    /* Lease updates counters */
    LeaseUpdateLatency = KqpGroup->GetHistogram(
        "LeaseUpdatesLatencyMs", NMonitoring::ExponentialHistogram(20, 2, 1));
    RunActorLeaseUpdateBacklog = KqpGroup->GetHistogram(
        "LeaseUpdatesBacklogMs", NMonitoring::LinearHistogram(30, 0, 1000));

    /* Transactions */
    CreateTxKindCounters(TKqpTransactionInfo::EKind::Pure, "Pure");
    CreateTxKindCounters(TKqpTransactionInfo::EKind::ReadOnly, "ReadOnly");
    CreateTxKindCounters(TKqpTransactionInfo::EKind::WriteOnly, "WriteOnly");
    CreateTxKindCounters(TKqpTransactionInfo::EKind::ReadWrite, "ReadWrite");
    TxReplySizeExceededError = KqpGroup->GetCounter("Tx/TxReplySizeExceededErrorCount", true);
    DataShardTxReplySizeExceededError = KqpGroup->GetCounter("Tx/DataShardTxReplySizeExceededErrorCount", true);

    /* Compile service */
    CompileQueryCacheSize = YdbGroup->GetNamedCounter("name", "table.query.compilation.cached_query_count", false);
    CompileQueryCacheBytes = YdbGroup->GetNamedCounter("name", "table.query.compilation.cache_size_bytes", false);
    CompileQueryCacheEvicted = YdbGroup->GetNamedCounter("name", "table.query.compilation.cache_evictions", true);

    CompileQueueSize = KqpGroup->GetCounter("Compilation/QueueSize", false);

    /* Compile computation pattern service */
    CompiledComputationPatterns = KqpGroup->GetCounter("ComputationPatternCompilation/CompiledComputationPatterns");
    CompileComputationPatternsQueueSize = KqpGroup->GetCounter("ComputationPatternCompilation/CompileComputationPatternsQueueSize");

    /* Resource Manager */
    RmComputeActors = KqpGroup->GetCounter("RM/ComputeActors", false);
    RmMemory = KqpGroup->GetCounter("RM/Memory", false);
    RmExternalMemory = KqpGroup->GetCounter("RM/ExternalMemory", false);
    RmNotEnoughMemory = KqpGroup->GetCounter("RM/NotEnoughMemory", true);
    RmNotEnoughComputeActors = KqpGroup->GetCounter("RM/NotEnoughComputeActors", true);
    RmOnStartAllocs = KqpGroup->GetCounter("Rm/OnStartAllocs", true);
    RmExtraMemAllocs = KqpGroup->GetCounter("RM/ExtraMemAllocs", true);
    RmExtraMemFree = KqpGroup->GetCounter("RM/ExtraMemFree", true);
    RmOnCompleteFree = KqpGroup->GetCounter("RM/OnCompleteFree", true);
    RmInternalError = KqpGroup->GetCounter("RM/InternalError", true);
    RmSnapshotLatency = KqpGroup->GetHistogram(
        "RM/SnapshotLatency", NMonitoring::ExponentialHistogram(20, 2, 1));

    NodeServiceStartEventDelivery = KqpGroup->GetHistogram(
        "NodeService/StartEventDeliveryUs", NMonitoring::ExponentialHistogram(20, 2, 1));
    NodeServiceProcessTime = KqpGroup->GetHistogram(
        "NodeService/ProcessStartEventUs", NMonitoring::ExponentialHistogram(20, 2, 1));
    NodeServiceProcessCancelTime = KqpGroup->GetHistogram(
        "NodeService/ProcessCancelEventUs", NMonitoring::ExponentialHistogram(20, 2, 1));
    RmMaxSnapshotLatency = KqpGroup->GetCounter("RM/MaxSnapshotLatency", false);
    RmNodeNumberInSnapshot = KqpGroup->GetCounter("RM/NodeNumberInSnapshot", false);

    /* Scan queries */
    ScanQueryShardDisconnect = KqpGroup->GetCounter("ScanQuery/ShardDisconnect", true);
    ScanQueryShardResolve = KqpGroup->GetCounter("ScanQuery/ShardResolve", true);
    ScanQueryRateLimitLatency = KqpGroup->GetHistogram(
        "ScanQuery/RateLimitLatency", NMonitoring::ExponentialHistogram(20, 2, 1));

    /* iterator reads */
    IteratorsShardResolve = KqpGroup->GetCounter("IteratorReads/ShardResolves", true);
    IteratorsReadSplits = KqpGroup->GetCounter("IteratorReads/ReadSplits", true);
    SentIteratorAcks = KqpGroup->GetCounter("IteratorReads/SentAcks", true);
    SentIteratorCancels = KqpGroup->GetCounter("IteratorReads/SentCancels", true);
    CreatedIterators = KqpGroup->GetCounter("IteratorReads/Created", true);
    ReadActorsCount = KqpGroup->GetCounter("IteratorReads/ReadActorCount", false);
    ReadActorRemoteFetch = KqpGroup->GetCounter("IteratorReads/ReadActorRemoteFetch", true);
    ReadActorRemoteFirstFetch = KqpGroup->GetCounter("IteratorReads/ReadActorRemoteFirstFetch", true);
    ReadActorAbsentNodeId = KqpGroup->GetCounter("IteratorReads/AbsentNodeId", true);
    StreamLookupActorsCount = KqpGroup->GetCounter("IteratorReads/StreamLookupActorCount", false);
    ReadActorRetries = KqpGroup->GetCounter("IteratorReads/Retries", true);
    DataShardIteratorFails = KqpGroup->GetCounter("IteratorReads/DatashardFails", true);
    DataShardIteratorMessages = KqpGroup->GetCounter("IteratorReads/DatashardMessages", true);
    IteratorDeliveryProblems = KqpGroup->GetCounter("IteratorReads/DeliveryProblems", true);

    /* sequencers */

    SequencerActorsCount = KqpGroup->GetCounter("Sequencer/ActorCount", false);
    SequencerErrors = KqpGroup->GetCounter("Sequencer/Errors", true);
    SequencerOk = KqpGroup->GetCounter("Sequencer/Ok", true);

    LiteralTxTotalTimeHistogram = KqpGroup->GetHistogram(
        "PhyTx/LiteralTxTotalTimeMs", NMonitoring::ExponentialHistogram(10, 2, 1));
    DataTxTotalTimeHistogram = KqpGroup->GetHistogram(
        "PhyTx/DataTxTotalTimeMs", NMonitoring::ExponentialHistogram(20, 2, 1));
    ScanTxTotalTimeHistogram = KqpGroup->GetHistogram(
        "PhyTx/ScanTxTotalTimeMs", NMonitoring::ExponentialHistogram(20, 2, 1));

    FullScansExecuted = KqpGroup->GetCounter("FullScans", true);

    SchedulerThrottled = KqpGroup->GetCounter("NodeScheduler/ThrottledUs", true);
    SchedulerCapacity = KqpGroup->GetCounter("NodeScheduler/Capacity");
    ComputeActorExecutions = KqpGroup->GetHistogram("NodeScheduler/BatchUs", NMonitoring::ExponentialHistogram(20, 2, 1));
    ThrottledActorsSpuriousActivations = KqpGroup->GetCounter("NodeScheduler/SpuriousActivations", true);
    SchedulerDelays = KqpGroup->GetHistogram("NodeScheduler/Delay", NMonitoring::ExponentialHistogram(20, 2, 1));
}

::NMonitoring::TDynamicCounterPtr TKqpCounters::GetKqpCounters() const {
    return KqpGroup;
}

::NMonitoring::TDynamicCounterPtr TKqpCounters::GetQueryReplayCounters() const {
    return QueryReplayGroup;
}

::NMonitoring::TDynamicCounterPtr TKqpCounters::GetWorkloadManagerCounters() const {
    return WorkloadManagerGroup;
}


void TKqpCounters::ReportProxyForwardedRequest(TKqpDbCountersPtr dbCounters) {
    TKqpCountersBase::ReportProxyForwardedRequest();
    if (dbCounters) {
        dbCounters->ReportProxyForwardedRequest();
    }
}

void TKqpCounters::ReportSessionShutdownRequest(TKqpDbCountersPtr dbCounters) {
    TKqpCountersBase::ReportSessionShutdownRequest();
    if (dbCounters) {
        dbCounters->ReportSessionShutdownRequest();
    }
}

void TKqpCounters::ReportSessionGracefulShutdownHit(TKqpDbCountersPtr dbCounters) {
    TKqpCountersBase::ReportSessionGracefulShutdownHit();
    if (dbCounters) {
        dbCounters->ReportSessionGracefulShutdownHit();
    }

}

void TKqpCounters::ReportCreateSession(TKqpDbCountersPtr dbCounters, ui64 requestSize) {
    TKqpCountersBase::ReportCreateSession(requestSize);
    if (dbCounters) {
        dbCounters->ReportCreateSession(requestSize);
    }
}

void TKqpCounters::ReportPingSession(TKqpDbCountersPtr dbCounters, ui64 requestSize) {
    TKqpCountersBase::ReportPingSession(requestSize);
    if (dbCounters) {
        dbCounters->ReportPingSession(requestSize);
    }
}

void TKqpCounters::ReportCloseSession(TKqpDbCountersPtr dbCounters, ui64 requestSize) {
    TKqpCountersBase::ReportCloseSession(requestSize);
    if (dbCounters) {
        dbCounters->ReportCloseSession(requestSize);
    }
}

void TKqpCounters::ReportQueryAction(TKqpDbCountersPtr dbCounters, NKikimrKqp::EQueryAction action) {
    TKqpCountersBase::ReportQueryAction(action);
    if (dbCounters) {
        dbCounters->ReportQueryAction(action);
    }
}

void TKqpCounters::ReportQueryType(TKqpDbCountersPtr dbCounters, NKikimrKqp::EQueryType type) {
    TKqpCountersBase::ReportQueryType(type);
    if (dbCounters) {
        dbCounters->ReportQueryType(type);
    }
}

void TKqpCounters::ReportQueryRequest(TKqpDbCountersPtr dbCounters, ui64 requestBytes, ui64 parametersBytes, ui64 queryBytes) {
    TKqpCountersBase::ReportQueryRequest(requestBytes, parametersBytes, queryBytes);
    if (dbCounters) {
        dbCounters->ReportQueryRequest(requestBytes, parametersBytes, queryBytes);
    }
}

void TKqpCounters::ReportCancelQuery(TKqpDbCountersPtr dbCounters, ui64 requestSize) {
    TKqpCountersBase::ReportCancelQuery(requestSize);
    if (dbCounters) {
        dbCounters->ReportCancelQuery(requestSize);
    }
}

void TKqpCounters::ReportQueryWithRangeScan(TKqpDbCountersPtr dbCounters) {
    TKqpCountersBase::ReportQueryWithRangeScan();
    if (dbCounters) {
        dbCounters->ReportQueryWithRangeScan();
    }
}

void TKqpCounters::ReportQueryWithFullScan(TKqpDbCountersPtr dbCounters) {
    TKqpCountersBase::ReportQueryWithFullScan();
    if (dbCounters) {
        dbCounters->ReportQueryWithFullScan();
    }
}

void TKqpCounters::ReportQueryAffectedShards(TKqpDbCountersPtr dbCounters, ui64 shardsCount) {
    TKqpCountersBase::ReportQueryAffectedShards(shardsCount);
    if (dbCounters) {
        dbCounters->ReportQueryAffectedShards(shardsCount);
    }
}

void TKqpCounters::ReportQueryReadSets(TKqpDbCountersPtr dbCounters, ui64 readSetsCount) {
    TKqpCountersBase::ReportQueryReadSets(readSetsCount);
    if (dbCounters) {
        dbCounters->ReportQueryReadSets(readSetsCount);
    }
}

void TKqpCounters::ReportQueryReadBytes(TKqpDbCountersPtr dbCounters, ui64 bytesCount) {
    TKqpCountersBase::ReportQueryReadBytes(bytesCount);
    if (dbCounters) {
        dbCounters->ReportQueryReadBytes(bytesCount);
    }
}

void TKqpCounters::ReportQueryReadRows(TKqpDbCountersPtr dbCounters, ui64 rowsCount) {
    TKqpCountersBase::ReportQueryReadRows(rowsCount);
    if (dbCounters) {
        dbCounters->ReportQueryReadRows(rowsCount);
    }
}

void TKqpCounters::ReportQueryMaxShardReplySize(TKqpDbCountersPtr dbCounters, ui64 replySize) {
    TKqpCountersBase::ReportQueryMaxShardReplySize(replySize);
    if (dbCounters) {
        dbCounters->ReportQueryMaxShardReplySize(replySize);
    }
}

void TKqpCounters::ReportQueryMaxShardProgramSize(TKqpDbCountersPtr dbCounters, ui64 programSize) {
    TKqpCountersBase::ReportQueryMaxShardProgramSize(programSize);
    if (dbCounters) {
        dbCounters->ReportQueryMaxShardProgramSize(programSize);
    }
}

void TKqpCounters::ReportResponseStatus(TKqpDbCountersPtr dbCounters, ui64 responseSize,
    Ydb::StatusIds::StatusCode ydbStatus)
{
    TKqpCountersBase::ReportResponseStatus(responseSize, ydbStatus);
    if (dbCounters) {
        dbCounters->ReportResponseStatus(responseSize, ydbStatus);
    }
}

void TKqpCounters::ReportResultsBytes(TKqpDbCountersPtr dbCounters, ui64 resultsSize) {
    TKqpCountersBase::ReportResultsBytes(resultsSize);
    if (dbCounters) {
        dbCounters->ReportResultsBytes(resultsSize);
    }
}

void TKqpCounters::ReportIssues(TKqpDbCountersPtr dbCounters,
    THashMap<ui32, ::NMonitoring::TDynamicCounters::TCounterPtr>& issueCounters,
    const Ydb::Issue::IssueMessage& issue)
{
    TKqpCountersBase::ReportIssues(issueCounters, issue);
    if (dbCounters) {
        dbCounters->ReportIssues(issueCounters, issue);
    }
}

void TKqpCounters::ReportQueryLatency(TKqpDbCountersPtr dbCounters,
    NKikimrKqp::EQueryAction action, const TDuration& duration)
{
    TKqpCountersBase::ReportQueryLatency(action, duration);
    if (dbCounters) {
        dbCounters->ReportQueryLatency(action, duration);
    }
}

void TKqpCounters::ReportTransaction(TKqpDbCountersPtr dbCounters, const TKqpTransactionInfo& txInfo) {
    TKqpCountersBase::ReportTransaction(txInfo);
    if (dbCounters) {
        dbCounters->ReportTransaction(txInfo);
    }
    if (txInfo.Status == TKqpTransactionInfo::EStatus::Committed) {
        UpdateTxCounters(txInfo, TxByKind);
    }
}

void TKqpCounters::ReportLeaseUpdateLatency(const TDuration& duration) {
    LeaseUpdateLatency->Collect(duration.MilliSeconds());
}

void TKqpCounters::ReportRunActorLeaseUpdateBacklog(const TDuration& duration) {
    RunActorLeaseUpdateBacklog->Collect(duration.MilliSeconds());
}

void TKqpCounters::ReportSqlVersion(TKqpDbCountersPtr dbCounters, ui16 sqlVersion) {
    TKqpCountersBase::ReportSqlVersion(sqlVersion);
    if (dbCounters) {
        dbCounters->ReportSqlVersion(sqlVersion);
    }
}

void TKqpCounters::ReportWorkerCreated(TKqpDbCountersPtr dbCounters) {
    TKqpCountersBase::ReportWorkerCreated();
    if (dbCounters) {
        dbCounters->ReportWorkerCreated();
    }
}

void TKqpCounters::ReportWorkerFinished(TKqpDbCountersPtr dbCounters, TDuration lifeSpan) {
    TKqpCountersBase::ReportWorkerFinished(lifeSpan);
    if (dbCounters) {
        dbCounters->ReportWorkerFinished(lifeSpan);
    }
}

void TKqpCounters::ReportWorkerCleanupLatency(TKqpDbCountersPtr dbCounters, TDuration cleanupTime) {
    TKqpCountersBase::ReportWorkerCleanupLatency(cleanupTime);
    if (dbCounters) {
        dbCounters->ReportWorkerCleanupLatency(cleanupTime);
    }
}

void TKqpCounters::ReportWorkerClosedIdle(TKqpDbCountersPtr dbCounters) {
    TKqpCountersBase::ReportWorkerClosedIdle();
    if (dbCounters) {
        dbCounters->ReportWorkerClosedIdle();
    }
}

void TKqpCounters::ReportWorkerClosedError(TKqpDbCountersPtr dbCounters) {
    TKqpCountersBase::ReportWorkerClosedError();
    if (dbCounters) {
        dbCounters->ReportWorkerClosedError();
    }
}

void TKqpCounters::ReportWorkerClosedRequest(TKqpDbCountersPtr dbCounters) {
    TKqpCountersBase::ReportWorkerClosedRequest();
    if (dbCounters) {
        dbCounters->ReportWorkerClosedRequest();
    }
}

void TKqpCounters::ReportQueriesPerWorker(TKqpDbCountersPtr dbCounters, ui32 queryId) {
    TKqpCountersBase::ReportQueriesPerWorker(queryId);
    if (dbCounters) {
        dbCounters->ReportQueriesPerWorker(queryId);
    }
}

void TKqpCounters::ReportSessionActorCreated(TKqpDbCountersPtr dbCounters) {
    TKqpCountersBase::ReportSessionActorCreated();
    if (dbCounters) {
        dbCounters->ReportSessionActorCreated();
    }
}

void TKqpCounters::ReportSessionActorFinished(TKqpDbCountersPtr dbCounters, TDuration lifeSpan) {
    TKqpCountersBase::ReportSessionActorFinished(lifeSpan);
    if (dbCounters) {
        dbCounters->ReportSessionActorFinished(lifeSpan);
    }
}

void TKqpCounters::ReportSessionActorCleanupLatency(TKqpDbCountersPtr dbCounters, TDuration cleanupTime) {
    TKqpCountersBase::ReportSessionActorCleanupLatency(cleanupTime);
    if (dbCounters) {
        dbCounters->ReportSessionActorCleanupLatency(cleanupTime);
    }
}

void TKqpCounters::ReportSessionActorClosedError(TKqpDbCountersPtr dbCounters) {
    TKqpCountersBase::ReportSessionActorClosedError();
    if (dbCounters) {
        dbCounters->ReportSessionActorClosedError();
    }
}

void TKqpCounters::ReportSessionActorClosedRequest(TKqpDbCountersPtr dbCounters) {
    TKqpCountersBase::ReportSessionActorClosedRequest();
    if (dbCounters) {
        dbCounters->ReportSessionActorClosedRequest();
    }
}

void TKqpCounters::ReportSessionActorClosedIdle(TKqpDbCountersPtr dbCounters) {
    TKqpCountersBase::ReportSessionActorClosedIdle();
    if (dbCounters) {
        dbCounters->ReportSessionActorClosedIdle();
    }
}

void TKqpCounters::ReportQueriesPerSessionActor(TKqpDbCountersPtr dbCounters, ui32 queryId) {
    TKqpCountersBase::ReportQueriesPerSessionActor(queryId);
    if (dbCounters) {
        dbCounters->ReportQueriesPerSessionActor(queryId);
    }
}

void TKqpCounters::ReportBeginTransaction(TKqpDbCountersPtr dbCounters,
    ui32 evictedTx, ui32 currentActiveTx, ui32 currentAbortedTx)
{
    TKqpCountersBase::ReportBeginTransaction(evictedTx, currentActiveTx, currentAbortedTx);
    if (dbCounters) {
        dbCounters->ReportBeginTransaction(evictedTx, currentActiveTx, currentAbortedTx);
    }
}

void TKqpCounters::ReportTxCreated(TKqpDbCountersPtr dbCounters) {
    TKqpCountersBase::ReportTxCreated();
    if (dbCounters) {
        dbCounters->ReportTxCreated();
    }
}

void TKqpCounters::ReportTxAborted(TKqpDbCountersPtr dbCounters, ui32 abortedCount) {
    TKqpCountersBase::ReportTxAborted(abortedCount);
    if (dbCounters) {
        dbCounters->ReportTxAborted(abortedCount);
    }
}

void TKqpCounters::ReportQueryCacheHit(TKqpDbCountersPtr dbCounters, bool hit) {
    TKqpCountersBase::ReportQueryCacheHit(hit);
    if (dbCounters) {
        dbCounters->ReportQueryCacheHit(hit);
    }
}

void TKqpCounters::ReportCompileStart(TKqpDbCountersPtr dbCounters) {
    TKqpCountersBase::ReportCompileStart();
    if (dbCounters) {
        dbCounters->ReportCompileStart();
    }
}

void TKqpCounters::ReportCompileFinish(TKqpDbCountersPtr dbCounters) {
    TKqpCountersBase::ReportCompileFinish();
    if (dbCounters) {
        dbCounters->ReportCompileFinish();
    }
}

void TKqpCounters::ReportCompileError(TKqpDbCountersPtr dbCounters) {
    TKqpCountersBase::ReportCompileError();
    if (dbCounters) {
        dbCounters->ReportCompileError();
    }
}

void TKqpCounters::ReportCompileRequestCompile(TKqpDbCountersPtr dbCounters) {
    TKqpCountersBase::ReportCompileRequestCompile();
    if (dbCounters) {
        dbCounters->ReportCompileRequestCompile();
    }
}

void TKqpCounters::ReportCompileRequestGet(TKqpDbCountersPtr dbCounters) {
    TKqpCountersBase::ReportCompileRequestGet();
    if (dbCounters) {
        dbCounters->ReportCompileRequestGet();
    }
}

void TKqpCounters::ReportCompileRequestInvalidate(TKqpDbCountersPtr dbCounters) {
    TKqpCountersBase::ReportCompileRequestInvalidate();
    if (dbCounters) {
        dbCounters->ReportCompileRequestInvalidate();
    }
}

void TKqpCounters::ReportCompileRequestRejected(TKqpDbCountersPtr dbCounters) {
    TKqpCountersBase::ReportCompileRequestRejected();
    if (dbCounters) {
        dbCounters->ReportCompileRequestRejected();
    }
}

void TKqpCounters::ReportCompileRequestTimeout(TKqpDbCountersPtr dbCounters) {
    TKqpCountersBase::ReportCompileRequestTimeout();
    if (dbCounters) {
        dbCounters->ReportCompileRequestTimeout();
    }
}

void TKqpCounters::ReportCompileDurations(TKqpDbCountersPtr dbCounters, TDuration duration, TDuration cpuTime) {
    TKqpCountersBase::ReportCompileDurations(duration, cpuTime);
    if (dbCounters) {
        dbCounters->ReportCompileDurations(duration, cpuTime);
    }
}

void TKqpCounters::ReportRecompileRequestGet(TKqpDbCountersPtr dbCounters) {
    TKqpCountersBase::ReportRecompileRequestGet();
    if (dbCounters) {
        dbCounters->ReportRecompileRequestGet();
    }
}

const ::NMonitoring::TDynamicCounters::TCounterPtr TKqpCounters::RecompileRequestGet() const {
    return TKqpCountersBase::CompileRequestsRecompile;
}

const ::NMonitoring::TDynamicCounters::TCounterPtr TKqpCounters::GetActiveSessionActors() const {
    return TKqpCountersBase::ActiveSessionActors;
}

const ::NMonitoring::TDynamicCounters::TCounterPtr TKqpCounters::GetTxReplySizeExceededError() const {
    return TxReplySizeExceededError;
}

const ::NMonitoring::TDynamicCounters::TCounterPtr TKqpCounters::GetDataShardTxReplySizeExceededError() const {
    return DataShardTxReplySizeExceededError;
}

::NMonitoring::TDynamicCounters::TCounterPtr TKqpCounters::GetQueryTypeCounter(
    NKikimrKqp::EQueryType queryType)
{
    return QueryTypes[queryType];
}

TKqpDbCountersPtr TKqpCounters::GetDbCounters(const TString& database) {
    if (!ActorSystem || !AppData(ActorSystem)->FeatureFlags.GetEnableDbCounters() || database.empty()) {
        return {};
    }

    TKqpDbCountersPtr dbCounters;
    if (DbCounters.Get(database, dbCounters)) {
        return dbCounters;
    }

    return DbCounters.InsertIfAbsentWithInit(database, [&database, this] {
        auto counters = MakeIntrusive<TKqpDbCounters>();

        auto evRegister = MakeHolder<NSysView::TEvSysView::TEvRegisterDbCounters>(
            NKikimrSysView::KQP, database, counters);
        ActorSystem->Send(NSysView::MakeSysViewServiceID(ActorSystem->NodeId), evRegister.Release());

        if (DbWatcherActorId) {
            auto evWatch = MakeHolder<NSysView::TEvSysView::TEvWatchDatabase>(database);
            ActorSystem->Send(DbWatcherActorId, evWatch.Release());
        }
        return counters;
    });
}

void TKqpCounters::RemoveDbCounters(const TString& database) {
    DbCounters.Erase(database);
}

} // namespace NKqp
} // namespace NKikimr
