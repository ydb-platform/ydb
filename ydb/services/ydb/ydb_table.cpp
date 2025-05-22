#include "ydb_table.h"

#include <ydb/core/grpc_services/service_table.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>

namespace NKikimr {
namespace NGRpcService {

TGRpcYdbTableService::TGRpcYdbTableService(NActors::TActorSystem *system,
                                           TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
                                            TIntrusivePtr<TInFlightLimiterRegistry> inFlightLimiterRegistry,
                                           const NActors::TActorId& proxyId,
                                           bool rlAllowed,
                                           size_t handlersPerCompletionQueue)
    : TGrpcServiceBase(system, counters, proxyId, rlAllowed)
    , HandlersPerCompletionQueue(Max(size_t{1}, handlersPerCompletionQueue))
    , LimiterRegistry_(inFlightLimiterRegistry)
{
}

TGRpcYdbTableService::TGRpcYdbTableService(NActors::TActorSystem *system,
                                           TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
                                            TIntrusivePtr<TInFlightLimiterRegistry> inFlightLimiterRegistry,
                                           const TVector<NActors::TActorId>& proxies,
                                           bool rlAllowed,
                                           size_t handlersPerCompletionQueue)
    : TGrpcServiceBase(system, counters, proxies, rlAllowed)
    , HandlersPerCompletionQueue(Max(size_t{1}, handlersPerCompletionQueue))
    , LimiterRegistry_(inFlightLimiterRegistry)
{
}

void TGRpcYdbTableService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);
    auto getLimiter = CreateLimiterCb(LimiterRegistry_);

    size_t proxyCounter = 0;

#ifdef ADD_REQUEST_LIMIT
#error ADD_REQUEST_LIMIT macro already defined
#endif

#ifdef ADD_STREAM_REQUEST_LIMIT
#error ADD_STREAM_REQUEST_LIMIT macro already defined
#endif

#define ADD_REQUEST_LIMIT(NAME, CB, LIMIT_TYPE, REQUEST_TYPE, ...)                                                    \
    for (size_t i = 0; i < HandlersPerCompletionQueue; ++i) {                                                         \
        for (auto* cq: CQS) {                                                                                         \
            MakeIntrusive<TGRpcRequest<Ydb::Table::NAME##Request, Ydb::Table::NAME##Response, TGRpcYdbTableService>>  \
                (this, &Service_, cq,                                                                                 \
                    [this, proxyCounter](NYdbGrpc::IRequestContextBase *ctx) {                                        \
                        NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer());                              \
                        ActorSystem_->Send(GRpcProxies_[proxyCounter % GRpcProxies_.size()],                          \
                            new TGrpcRequestOperationCall<Ydb::Table::NAME##Request, Ydb::Table::NAME##Response>      \
                                (ctx, &CB, TRequestAuxSettings {                                                      \
                                    .RlMode = RLSWITCH(TRateLimiterMode::LIMIT_TYPE),                                 \
                                    __VA_OPT__(.AuditMode = TAuditMode::__VA_ARGS__,)                                 \
                                    .RequestType = NJaegerTracing::ERequestType::TABLE_##REQUEST_TYPE,                \
                                }));                                                                                  \
                    }, &Ydb::Table::V1::TableService::AsyncService::Request ## NAME,                                  \
                    #NAME, logger, getCounterBlock("table", #NAME))->Run();                                           \
            ++proxyCounter;                                                                                           \
        }                                                                                                             \
    }

#define ADD_STREAM_REQUEST_LIMIT(NAME, IN, OUT, CB, LIMIT_TYPE, REQUEST_TYPE, USE_LIMITER) \
    for (size_t i = 0; i < HandlersPerCompletionQueue; ++i) {                                                                           \
        for (auto* cq: CQS) {                                                                                                           \
            MakeIntrusive<TGRpcRequest<Ydb::Table::IN, Ydb::Table::OUT, TGRpcYdbTableService>>                                          \
                (this, &Service_, cq,                                                                                                   \
                    [this, proxyCounter](NYdbGrpc::IRequestContextBase *ctx) {                                                          \
                        NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer());                                                \
                        ActorSystem_->Send(GRpcProxies_[proxyCounter % GRpcProxies_.size()],                                            \
                            new TGrpcRequestNoOperationCall<Ydb::Table::IN, Ydb::Table::OUT>                                            \
                                (ctx, &CB, TRequestAuxSettings {                                                                        \
                                    .RlMode = RLSWITCH(TRateLimiterMode::LIMIT_TYPE),                                                   \
                                    .RequestType = NJaegerTracing::ERequestType::TABLE_##REQUEST_TYPE,                                  \
                                }));                                                                                                    \
                    }, &Ydb::Table::V1::TableService::AsyncService::Request ## NAME,                                                    \
                    #NAME, logger, getCounterBlock("table", #NAME),                                                                     \
                    (USE_LIMITER ? getLimiter("TableService", #NAME, UNLIMITED_INFLIGHT) : nullptr))->Run();                         \
        ++proxyCounter;                                                                                                                 \
    }                                                                                                                                   \
    }

    ADD_REQUEST_LIMIT(CreateSession, DoCreateSessionRequest, Rps, CREATESESSION)
    ADD_REQUEST_LIMIT(KeepAlive, DoKeepAliveRequest, Rps, KEEPALIVE)
    ADD_REQUEST_LIMIT(AlterTable, DoAlterTableRequest, Rps, ALTERTABLE)
    ADD_REQUEST_LIMIT(CreateTable, DoCreateTableRequest, Rps, CREATETABLE)
    ADD_REQUEST_LIMIT(DropTable, DoDropTableRequest, Rps, DROPTABLE)
    ADD_REQUEST_LIMIT(DescribeTable, DoDescribeTableRequest, Rps, DESCRIBETABLE)
    ADD_REQUEST_LIMIT(DescribeExternalDataSource, DoDescribeExternalDataSourceRequest, Rps, DESCRIBEEXTERNALDATASOURCE)
    ADD_REQUEST_LIMIT(DescribeExternalTable, DoDescribeExternalTableRequest, Rps, DESCRIBEEXTERNALTABLE)
    ADD_REQUEST_LIMIT(CopyTable, DoCopyTableRequest, Rps, COPYTABLE)
    ADD_REQUEST_LIMIT(CopyTables, DoCopyTablesRequest, Rps, COPYTABLES)
    ADD_REQUEST_LIMIT(RenameTables, DoRenameTablesRequest, Rps, RENAMETABLES)
    ADD_REQUEST_LIMIT(ExplainDataQuery, DoExplainDataQueryRequest, Rps, EXPLAINDATAQUERY)
    ADD_REQUEST_LIMIT(ExecuteSchemeQuery, DoExecuteSchemeQueryRequest, Rps, EXECUTESCHEMEQUERY)
    ADD_REQUEST_LIMIT(BeginTransaction, DoBeginTransactionRequest, Rps, BEGINTRANSACTION, Auditable)
    ADD_REQUEST_LIMIT(DescribeTableOptions, DoDescribeTableOptionsRequest, Rps, DESCRIBETABLEOPTIONS)

    ADD_REQUEST_LIMIT(DeleteSession, DoDeleteSessionRequest, Off, DELETESESSION)
    ADD_REQUEST_LIMIT(CommitTransaction, DoCommitTransactionRequest, Off, COMMITTRANSACTION, Auditable)
    ADD_REQUEST_LIMIT(RollbackTransaction, DoRollbackTransactionRequest, Off, ROLLBACKTRANSACTION, Auditable)

    ADD_REQUEST_LIMIT(PrepareDataQuery, DoPrepareDataQueryRequest, Ru, PREPAREDATAQUERY, Auditable)
    ADD_REQUEST_LIMIT(ExecuteDataQuery, DoExecuteDataQueryRequest, Ru, EXECUTEDATAQUERY, Auditable)
    ADD_REQUEST_LIMIT(BulkUpsert, DoBulkUpsertRequest, Ru, BULKUPSERT, Auditable)

    ADD_STREAM_REQUEST_LIMIT(StreamExecuteScanQuery, ExecuteScanQueryRequest, ExecuteScanQueryPartialResponse, DoExecuteScanQueryRequest, RuOnProgress, STREAMEXECUTESCANQUERY, false)
    ADD_STREAM_REQUEST_LIMIT(StreamReadTable, ReadTableRequest, ReadTableResponse, DoReadTableRequest, RuOnProgress, STREAMREADTABLE, false)
    ADD_STREAM_REQUEST_LIMIT(ReadRows, ReadRowsRequest, ReadRowsResponse, DoReadRowsRequest, Ru, READROWS, true)

#undef ADD_REQUEST_LIMIT
#undef ADD_STREAM_REQUEST_LIMIT
}

} // namespace NGRpcService
} // namespace NKikimr
