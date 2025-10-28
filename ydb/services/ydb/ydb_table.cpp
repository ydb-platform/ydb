#include "ydb_table.h"

#include <ydb/core/base/appdata.h>

#include <ydb/core/grpc_services/service_table.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>

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
    using namespace Ydb::Table;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);
    auto getLimiter = CreateLimiterCb(LimiterRegistry_);
    auto& icb = *ActorSystem_->AppData<TAppData>()->Icb;
    size_t proxyCounter = 0;

#ifdef SETUP_TABLE_METHOD
#error SETUP_TABLE_METHOD macro already defined
#endif

#ifdef SETUP_TABLE_STREAM_METHOD
#error SETUP_TABLE_STREAM_METHOD macro already defined
#endif

#ifdef GET_LIMITER_BY_PATH
#error GET_LIMITER_BY_PATH macro already defined
#endif

#define SETUP_TABLE_METHOD(methodName, methodCallback, rlMode, requestType, auditMode) \
    for (size_t i = 0; i < HandlersPerCompletionQueue; ++i) {                          \
        for (auto* cq: CQS) {                                                          \
            SETUP_RUNTIME_EVENT_METHOD(                                                \
                methodName,                                                            \
                YDB_API_DEFAULT_REQUEST_TYPE(methodName),                              \
                YDB_API_DEFAULT_RESPONSE_TYPE(methodName),                             \
                methodCallback,                                                        \
                rlMode,                                                                \
                requestType,                                                           \
                YDB_API_DEFAULT_COUNTER_BLOCK(table, methodName),                      \
                auditMode,                                                             \
                COMMON,                                                                \
                ::NKikimr::NGRpcService::TGrpcRequestOperationCall,                    \
                GRpcProxies_[proxyCounter % GRpcProxies_.size()],                      \
                cq,                                                                    \
                nullptr,                                                               \
                nullptr                                                                \
            );                                                                         \
            ++proxyCounter;                                                            \
        }                                                                              \
    }

#define GET_LIMITER_BY_PATH(ICB_PATH)\
    getLimiter(#ICB_PATH, icb.ICB_PATH, UNLIMITED_INFLIGHT)

#define SETUP_TABLE_STREAM_METHOD(methodName, inputType, outputType, methodCallback, rlMode, requestType, auditMode, limiter) \
    for (size_t i = 0; i < HandlersPerCompletionQueue; ++i) {                                                                 \
        for (auto* cq: CQS) {                                                                                                 \
            SETUP_RUNTIME_EVENT_METHOD(                                                                                       \
                methodName,                                                                                                   \
                inputType,                                                                                                    \
                outputType,                                                                                                   \
                methodCallback,                                                                                               \
                rlMode,                                                                                                       \
                requestType,                                                                                                  \
                YDB_API_DEFAULT_COUNTER_BLOCK(table, methodName),                                                             \
                auditMode,                                                                                                    \
                COMMON,                                                                                                       \
                ::NKikimr::NGRpcService::TGrpcRequestNoOperationCall,                                                         \
                GRpcProxies_[proxyCounter % GRpcProxies_.size()],                                                             \
                cq,                                                                                                           \
                limiter,                                                                                                      \
                nullptr                                                                                                       \
            );                                                                                                                \
            ++proxyCounter;                                                                                                   \
        }                                                                                                                     \
    }

    SETUP_TABLE_METHOD(CreateSession, DoCreateSessionRequest, RLSWITCH(Rps), TABLE_CREATESESSION, TAuditMode::NonModifying());
    SETUP_TABLE_METHOD(KeepAlive, DoKeepAliveRequest, RLSWITCH(Rps), TABLE_KEEPALIVE, TAuditMode::NonModifying());
    SETUP_TABLE_METHOD(AlterTable, DoAlterTableRequest, RLSWITCH(Rps), TABLE_ALTERTABLE, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    SETUP_TABLE_METHOD(CreateTable, DoCreateTableRequest, RLSWITCH(Rps), TABLE_CREATETABLE, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    SETUP_TABLE_METHOD(DropTable, DoDropTableRequest, RLSWITCH(Rps), TABLE_DROPTABLE, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    SETUP_TABLE_METHOD(DescribeTable, DoDescribeTableRequest, RLSWITCH(Rps), TABLE_DESCRIBETABLE, TAuditMode::NonModifying());
    SETUP_TABLE_METHOD(DescribeExternalDataSource, DoDescribeExternalDataSourceRequest, RLSWITCH(Rps), TABLE_DESCRIBEEXTERNALDATASOURCE, TAuditMode::NonModifying());
    SETUP_TABLE_METHOD(DescribeExternalTable, DoDescribeExternalTableRequest, RLSWITCH(Rps), TABLE_DESCRIBEEXTERNALTABLE, TAuditMode::NonModifying());
    SETUP_TABLE_METHOD(DescribeSystemView, DoDescribeSystemViewRequest, RLSWITCH(Rps), TABLE_DESCRIBESYSTEMVIEW, TAuditMode::NonModifying());
    SETUP_TABLE_METHOD(CopyTable, DoCopyTableRequest, RLSWITCH(Rps), TABLE_COPYTABLE, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    SETUP_TABLE_METHOD(CopyTables, DoCopyTablesRequest, RLSWITCH(Rps), TABLE_COPYTABLES, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    SETUP_TABLE_METHOD(RenameTables, DoRenameTablesRequest, RLSWITCH(Rps), TABLE_RENAMETABLES, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    SETUP_TABLE_METHOD(ExplainDataQuery, DoExplainDataQueryRequest, RLSWITCH(Rps), TABLE_EXPLAINDATAQUERY, TAuditMode::NonModifying());
    SETUP_TABLE_METHOD(ExecuteSchemeQuery, DoExecuteSchemeQueryRequest, RLSWITCH(Rps), TABLE_EXECUTESCHEMEQUERY, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    SETUP_TABLE_METHOD(BeginTransaction, DoBeginTransactionRequest, RLSWITCH(Rps), TABLE_BEGINTRANSACTION, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml));
    SETUP_TABLE_METHOD(DescribeTableOptions, DoDescribeTableOptionsRequest, RLSWITCH(Rps), TABLE_DESCRIBETABLEOPTIONS, TAuditMode::NonModifying());

    SETUP_TABLE_METHOD(DeleteSession, DoDeleteSessionRequest, RLMODE(Off), TABLE_DELETESESSION, TAuditMode::NonModifying());
    SETUP_TABLE_METHOD(CommitTransaction, DoCommitTransactionRequest, RLMODE(Off), TABLE_COMMITTRANSACTION, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml));
    SETUP_TABLE_METHOD(RollbackTransaction, DoRollbackTransactionRequest, RLMODE(Off), TABLE_ROLLBACKTRANSACTION, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml));

    SETUP_TABLE_METHOD(PrepareDataQuery, DoPrepareDataQueryRequest, RLSWITCH(Ru), TABLE_PREPAREDATAQUERY, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml));
    SETUP_TABLE_METHOD(ExecuteDataQuery, DoExecuteDataQueryRequest, RLSWITCH(Ru), TABLE_EXECUTEDATAQUERY, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml));
    SETUP_TABLE_METHOD(BulkUpsert, DoBulkUpsertRequest, RLSWITCH(Ru), TABLE_BULKUPSERT, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml));

    SETUP_TABLE_STREAM_METHOD(StreamExecuteScanQuery, ExecuteScanQueryRequest, ExecuteScanQueryPartialResponse, DoExecuteScanQueryRequest, RLSWITCH(RuOnProgress), TABLE_STREAMEXECUTESCANQUERY, TAuditMode::NonModifying(), nullptr);
    SETUP_TABLE_STREAM_METHOD(StreamReadTable, ReadTableRequest, ReadTableResponse, DoReadTableRequest, RLSWITCH(RuOnProgress), TABLE_STREAMREADTABLE, TAuditMode::NonModifying(), nullptr);
    SETUP_TABLE_STREAM_METHOD(ReadRows, ReadRowsRequest, ReadRowsResponse, DoReadRowsRequest, RLSWITCH(Ru), TABLE_READROWS, TAuditMode::NonModifying(), GET_LIMITER_BY_PATH(GRpcControls.RequestConfigs.TableService_ReadRows.MaxInFlight));

#undef GET_LIMITER_BY_PATH
#undef SETUP_TABLE_METHOD
#undef SETUP_TABLE_STREAM_METHOD
}

} // namespace NGRpcService
} // namespace NKikimr
