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

#define ADD_REQUEST_LIMIT(NAME, CB, LIMIT_TYPE, REQUEST_TYPE, AUDIT_MODE)                                             \
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
                                    .AuditMode = AUDIT_MODE,                                                          \
                                    .RequestType = NJaegerTracing::ERequestType::TABLE_##REQUEST_TYPE,                \
                                }));                                                                                  \
                    }, &Ydb::Table::V1::TableService::AsyncService::Request ## NAME,                                  \
                    #NAME, logger, getCounterBlock("table", #NAME))->Run();                                           \
            ++proxyCounter;                                                                                           \
        }                                                                                                             \
    }

#define ADD_STREAM_REQUEST_LIMIT(NAME, IN, OUT, CB, LIMIT_TYPE, REQUEST_TYPE, USE_LIMITER, AUDIT_MODE)       \
    for (size_t i = 0; i < HandlersPerCompletionQueue; ++i) {                                                \
        for (auto* cq: CQS) {                                                                                \
            MakeIntrusive<TGRpcRequest<Ydb::Table::IN, Ydb::Table::OUT, TGRpcYdbTableService>>               \
                (this, &Service_, cq,                                                                        \
                    [this, proxyCounter](NYdbGrpc::IRequestContextBase *ctx) {                               \
                        NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer());                     \
                        ActorSystem_->Send(GRpcProxies_[proxyCounter % GRpcProxies_.size()],                 \
                            new TGrpcRequestNoOperationCall<Ydb::Table::IN, Ydb::Table::OUT>                 \
                                (ctx, &CB, TRequestAuxSettings {                                             \
                                    .RlMode = RLSWITCH(TRateLimiterMode::LIMIT_TYPE),                        \
                                    .AuditMode = AUDIT_MODE,                                                 \
                                    .RequestType = NJaegerTracing::ERequestType::TABLE_##REQUEST_TYPE,       \
                                }));                                                                         \
                    }, &Ydb::Table::V1::TableService::AsyncService::Request ## NAME,                         \
                    #NAME, logger, getCounterBlock("table", #NAME),                                          \
                    (USE_LIMITER ? getLimiter("TableService", #NAME, UNLIMITED_INFLIGHT) : nullptr))->Run(); \
        ++proxyCounter;                                                                                      \
    }                                                                                                        \
    }

    ADD_REQUEST_LIMIT(CreateSession, DoCreateSessionRequest, Rps, CREATESESSION, TAuditMode::NonModifying())
    ADD_REQUEST_LIMIT(KeepAlive, DoKeepAliveRequest, Rps, KEEPALIVE, TAuditMode::NonModifying())
    ADD_REQUEST_LIMIT(AlterTable, DoAlterTableRequest, Rps, ALTERTABLE, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl))
    ADD_REQUEST_LIMIT(CreateTable, DoCreateTableRequest, Rps, CREATETABLE, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl))
    ADD_REQUEST_LIMIT(DropTable, DoDropTableRequest, Rps, DROPTABLE, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl))
    ADD_REQUEST_LIMIT(DescribeTable, DoDescribeTableRequest, Rps, DESCRIBETABLE, TAuditMode::NonModifying())
    ADD_REQUEST_LIMIT(CopyTable, DoCopyTableRequest, Rps, COPYTABLE, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl))
    ADD_REQUEST_LIMIT(CopyTables, DoCopyTablesRequest, Rps, COPYTABLES, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl))
    ADD_REQUEST_LIMIT(RenameTables, DoRenameTablesRequest, Rps, RENAMETABLES, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl))
    ADD_REQUEST_LIMIT(ExplainDataQuery, DoExplainDataQueryRequest, Rps, EXPLAINDATAQUERY, TAuditMode::NonModifying())
    ADD_REQUEST_LIMIT(ExecuteSchemeQuery, DoExecuteSchemeQueryRequest, Rps, EXECUTESCHEMEQUERY, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl))
    ADD_REQUEST_LIMIT(BeginTransaction, DoBeginTransactionRequest, Rps, BEGINTRANSACTION, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml))
    ADD_REQUEST_LIMIT(DescribeTableOptions, DoDescribeTableOptionsRequest, Rps, DESCRIBETABLEOPTIONS, TAuditMode::NonModifying())

    ADD_REQUEST_LIMIT(DeleteSession, DoDeleteSessionRequest, Off, DELETESESSION, TAuditMode::NonModifying())
    ADD_REQUEST_LIMIT(CommitTransaction, DoCommitTransactionRequest, Off, COMMITTRANSACTION, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml))
    ADD_REQUEST_LIMIT(RollbackTransaction, DoRollbackTransactionRequest, Off, ROLLBACKTRANSACTION, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml))

    ADD_REQUEST_LIMIT(PrepareDataQuery, DoPrepareDataQueryRequest, Ru, PREPAREDATAQUERY, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml))
    ADD_REQUEST_LIMIT(ExecuteDataQuery, DoExecuteDataQueryRequest, Ru, EXECUTEDATAQUERY, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml))
    ADD_REQUEST_LIMIT(BulkUpsert, DoBulkUpsertRequest, Ru, BULKUPSERT, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml))

    ADD_STREAM_REQUEST_LIMIT(StreamExecuteScanQuery, ExecuteScanQueryRequest, ExecuteScanQueryPartialResponse, DoExecuteScanQueryRequest, RuOnProgress, STREAMEXECUTESCANQUERY, false, TAuditMode::NonModifying())
    ADD_STREAM_REQUEST_LIMIT(StreamReadTable, ReadTableRequest, ReadTableResponse, DoReadTableRequest, RuOnProgress, STREAMREADTABLE, false, TAuditMode::NonModifying())
    ADD_STREAM_REQUEST_LIMIT(ReadRows, ReadRowsRequest, ReadRowsResponse, DoReadRowsRequest, Ru, READROWS, true, TAuditMode::NonModifying())

#undef ADD_REQUEST_LIMIT
#undef ADD_STREAM_REQUEST_LIMIT
}

} // namespace NGRpcService
} // namespace NKikimr
