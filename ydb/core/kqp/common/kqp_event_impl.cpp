#include "kqp.h"

#include <ydb/core/base/path.h>
#include <ydb/core/util/proto_duration.h>

namespace NKikimr::NKqp {

TEvKqp::TEvQueryRequest::TEvQueryRequest(
    const std::shared_ptr<NGRpcService::IRequestCtxMtSafe>& ctx,
    const TString& sessionId,
    TActorId actorId,
    TString&& yqlText,
    TString&& queryId,
    NKikimrKqp::EQueryAction queryAction,
    NKikimrKqp::EQueryType queryType,
    const ::Ydb::Table::TransactionControl* txControl,
    const ::google::protobuf::Map<TProtoStringType, ::Ydb::TypedValue>* ydbParameters,
    const ::Ydb::Table::QueryStatsCollection::Mode collectStats,
    const ::Ydb::Table::QueryCachePolicy* queryCachePolicy,
    const ::Ydb::Operations::OperationParams* operationParams,
    bool keepSession)
    : RequestCtx(ctx)
    , Database(CanonizePath(ctx->GetDatabaseName().GetOrElse("")))
    , SessionId(sessionId)
    , YqlText(std::move(yqlText))
    , QueryId(std::move(queryId))
    , QueryAction(queryAction)
    , QueryType(queryType)
    , TxControl(txControl)
    , YdbParameters(ydbParameters)
    , CollectStats(collectStats)
    , QueryCachePolicy(queryCachePolicy)
    , OperationParams(operationParams)
    , KeepSession(keepSession)
{
    if (OperationParams) {
        OperationTimeout = GetDuration(OperationParams->operation_timeout());
        CancelAfter = GetDuration(OperationParams->cancel_after());
    }
    ActorIdToProto(actorId, Record.MutableCancelationActor());
}

void TEvKqp::TEvQueryRequest::PrepareRemote() const {
    if (RequestCtx) {
        if (RequestCtx->GetSerializedToken()) {
            Record.SetUserToken(RequestCtx->GetSerializedToken());
        }

        Record.MutableRequest()->SetDatabase(Database);

        if (auto traceId = RequestCtx->GetTraceId()) {
            Record.SetTraceId(traceId.GetRef());
        }

        if (auto requestType = RequestCtx->GetRequestType()) {
            Record.SetRequestType(requestType.GetRef());
        }

        if (TxControl) {
            Record.MutableRequest()->MutableTxControl()->CopyFrom(*TxControl);
        }

        if (YdbParameters) {
            Record.MutableRequest()->MutableYdbParameters()->insert(YdbParameters->begin(), YdbParameters->end());
        }

        if (QueryCachePolicy) {
            Record.MutableRequest()->MutableQueryCachePolicy()->CopyFrom(*QueryCachePolicy);
        }

        if (CollectStats) {
            Record.MutableRequest()->SetCollectStats(CollectStats);
        }

        if (!YqlText.empty()) {
            Record.MutableRequest()->SetQuery(YqlText);
        }

        if (!QueryId.empty()) {
            Record.MutableRequest()->SetPreparedQuery(QueryId);
        }

        Record.MutableRequest()->SetSessionId(SessionId);
        Record.MutableRequest()->SetAction(QueryAction);
        Record.MutableRequest()->SetType(QueryType);
        if (OperationParams) {
            Record.MutableRequest()->SetCancelAfterMs(CancelAfter.MilliSeconds());
            Record.MutableRequest()->SetTimeoutMs(OperationTimeout.MilliSeconds());
        }

        RequestCtx.reset();
    }
}

}
