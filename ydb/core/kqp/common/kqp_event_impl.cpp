#include "kqp.h"

#include <ydb/core/base/path.h>
#include <ydb/core/util/proto_duration.h>

namespace NKikimr::NKqp {

TEvKqp::TEvQueryRequest::TEvQueryRequest(
    NKikimrKqp::EQueryAction queryAction,
    NKikimrKqp::EQueryType queryType,
    TActorId requestActorId,
    const std::shared_ptr<NGRpcService::IRequestCtxMtSafe>& ctx,
    const TString& sessionId,
    TString&& yqlText,
    TString&& queryId,
    const ::Ydb::Table::TransactionControl* txControl,
    const ::google::protobuf::Map<TProtoStringType, ::Ydb::TypedValue>* ydbParameters,
    const ::Ydb::Table::QueryStatsCollection::Mode collectStats,
    const ::Ydb::Table::QueryCachePolicy* queryCachePolicy,
    const ::Ydb::Operations::OperationParams* operationParams,
    const TQueryRequestSettings& querySettings,
    const TString& poolId,
    std::optional<NKqp::NFormats::TArrowFormatSettings> arrowFormatSettings)
    : RequestCtx(ctx)
    , RequestActorId(requestActorId)
    , Database(CanonizePath(ctx->GetDatabaseName().GetOrElse("")))
    , SessionId(sessionId)
    , YqlText(std::move(yqlText))
    , QueryId(std::move(queryId))
    , PoolId(poolId)
    , QueryAction(queryAction)
    , QueryType(queryType)
    , TxControl(txControl)
    , YdbParameters(ydbParameters)
    , CollectStats(collectStats)
    , QueryCachePolicy(queryCachePolicy)
    , HasOperationParams(operationParams)
    , QuerySettings(querySettings)
    , ArrowFormatSettings(std::move(arrowFormatSettings))
{

    if (HasOperationParams) {
        OperationTimeout = GetDuration(operationParams->operation_timeout());
        if (QuerySettings.UseCancelAfter) {
            CancelAfter = GetDuration(operationParams->cancel_after());
        }
    }
}

TEvKqp::TEvQueryRequest::TEvQueryRequest(const TString& userSID) : TEvQueryRequest()
{
    NACLib::TUserToken::TUserTokenInitFields fields {
        .UserSID = userSID
    };
    Token_ = new NACLib::TUserToken(fields);
}

void TEvKqp::TEvQueryRequest::PrepareRemote() const {
    if (RequestCtx) {
        if (RequestCtx->GetSerializedToken()) {
            Record.SetUserToken(RequestCtx->GetSerializedToken());
        }
        Record.MutableRequest()->SetClientAddress(RequestCtx->GetPeerName());

        Record.MutableRequest()->SetDatabase(Database);
        ActorIdToProto(RequestActorId, Record.MutableCancelationActor());
        ActorIdToProto(RequestActorId, Record.MutableRequestActorId());

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

        if (!PoolId.empty()) {
            Record.MutableRequest()->SetPoolId(PoolId);
        }

        if (!DatabaseId.empty()) {
            Record.MutableRequest()->SetDatabaseId(DatabaseId);
        }

        if (ArrowFormatSettings) {
            ArrowFormatSettings->ExportToProto(Record.MutableRequest()->MutableArrowFormatSettings());
        }

        Record.MutableRequest()->SetUsePublicResponseDataFormat(true);
        Record.MutableRequest()->SetSessionId(SessionId);
        Record.MutableRequest()->SetAction(QueryAction);
        Record.MutableRequest()->SetType(QueryType);
        Record.MutableRequest()->SetSyntax(QuerySettings.Syntax);
        if (HasOperationParams) {
            Record.MutableRequest()->SetCancelAfterMs(CancelAfter.MilliSeconds());
            Record.MutableRequest()->SetTimeoutMs(OperationTimeout.MilliSeconds());
        }
        Record.MutableRequest()->SetIsInternalCall(RequestCtx->IsInternalCall());
        Record.MutableRequest()->SetOutputChunkMaxSize(QuerySettings.OutputChunkMaxSize);
        Record.MutableRequest()->SetSchemaInclusionMode(QuerySettings.SchemaInclusionMode);
        Record.MutableRequest()->SetResultSetFormat(QuerySettings.ResultSetFormat);

        RequestCtx.reset();
    }
}

}
