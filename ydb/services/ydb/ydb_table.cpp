#include "ydb_table.h"

#include <ydb/core/grpc_services/service_table.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>

namespace NKikimr {
namespace NGRpcService {

void TGRpcYdbTableService::SetupIncomingRequests(NGrpc::TLoggerPtr logger) {
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);
#ifdef ADD_REQUEST_LIMIT
#error ADD_REQUEST_LIMIT macro already defined
#endif

#ifdef ADD_STREAM_REQUEST_LIMIT
#error ADD_STREAM_REQUEST_LIMIT macro already defined
#endif

#define ADD_REQUEST_LIMIT(NAME, CB, LIMIT_TYPE) \
    MakeIntrusive<TGRpcRequest<Ydb::Table::NAME##Request, Ydb::Table::NAME##Response, TGRpcYdbTableService>>    \
        (this, &Service_, CQ_,                                                                                  \
            [this](NGrpc::IRequestContextBase *ctx) {                                                           \
                NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer());                                \
                ActorSystem_->Send(GRpcRequestProxyId_,                                                         \
                    new TGrpcRequestOperationCall<Ydb::Table::NAME##Request, Ydb::Table::NAME##Response>        \
                        (ctx, &CB, TRequestAuxSettings{RLSWITCH(TRateLimiterMode::LIMIT_TYPE), nullptr}));      \
            }, &Ydb::Table::V1::TableService::AsyncService::Request ## NAME,                                    \
            #NAME, logger, getCounterBlock("table", #NAME))->Run();

#define ADD_STREAM_REQUEST_LIMIT(NAME, IN, OUT, CB, LIMIT_TYPE) \
    MakeIntrusive<TGRpcRequest<Ydb::Table::IN, Ydb::Table::OUT, TGRpcYdbTableService>>                          \
        (this, &Service_, CQ_,                                                                                  \
            [this](NGrpc::IRequestContextBase *ctx) {                                                           \
                NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer());                                \
                ActorSystem_->Send(GRpcRequestProxyId_,                                                         \
                    new TGrpcRequestNoOperationCall<Ydb::Table::IN, Ydb::Table::OUT>                            \
                        (ctx, &CB, TRequestAuxSettings{RLSWITCH(TRateLimiterMode::LIMIT_TYPE), nullptr}));      \
            }, &Ydb::Table::V1::TableService::AsyncService::Request ## NAME,                                    \
            #NAME, logger, getCounterBlock("table", #NAME))->Run();

    ADD_REQUEST_LIMIT(CreateSession, DoCreateSessionRequest, Rps)
    ADD_REQUEST_LIMIT(KeepAlive, DoKeepAliveRequest, Rps)
    ADD_REQUEST_LIMIT(AlterTable, DoAlterTableRequest, Rps)
    ADD_REQUEST_LIMIT(CreateTable, DoCreateTableRequest, Rps)
    ADD_REQUEST_LIMIT(DropTable, DoDropTableRequest, Rps)
    ADD_REQUEST_LIMIT(DescribeTable, DoDescribeTableRequest, Rps)
    ADD_REQUEST_LIMIT(CopyTable, DoCopyTableRequest, Rps)
    ADD_REQUEST_LIMIT(CopyTables, DoCopyTablesRequest, Rps)
    ADD_REQUEST_LIMIT(RenameTables, DoRenameTablesRequest, Rps)
    ADD_REQUEST_LIMIT(ExplainDataQuery, DoExplainDataQueryRequest, Rps)
    ADD_REQUEST_LIMIT(ExecuteSchemeQuery, DoExecuteSchemeQueryRequest, Rps)
    ADD_REQUEST_LIMIT(BeginTransaction, DoBeginTransactionRequest, Rps)
    ADD_REQUEST_LIMIT(DescribeTableOptions, DoDescribeTableOptionsRequest, Rps)

    ADD_REQUEST_LIMIT(DeleteSession, DoDeleteSessionRequest, Off)
    ADD_REQUEST_LIMIT(CommitTransaction, DoCommitTransactionRequest, Off)
    ADD_REQUEST_LIMIT(RollbackTransaction, DoRollbackTransactionRequest, Off)


    ADD_REQUEST_LIMIT(PrepareDataQuery, DoPrepareDataQueryRequest, Ru)
    ADD_REQUEST_LIMIT(ExecuteDataQuery, DoExecuteDataQueryRequest, Ru)
    ADD_REQUEST_LIMIT(BulkUpsert, DoBulkUpsertRequest, Ru)

    ADD_STREAM_REQUEST_LIMIT(StreamExecuteScanQuery, ExecuteScanQueryRequest, ExecuteScanQueryPartialResponse, DoExecuteScanQueryRequest, RuOnProgress)
    ADD_STREAM_REQUEST_LIMIT(StreamReadTable, ReadTableRequest, ReadTableResponse, DoReadTableRequest, RuOnProgress)

#undef ADD_REQUEST_LIMIT
#undef ADD_STREAM_REQUEST_LIMIT
}

} // namespace NGRpcService
} // namespace NKikimr
