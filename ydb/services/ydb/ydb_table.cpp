#include "ydb_table.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/rpc_calls.h>

namespace NKikimr {
namespace NGRpcService {

TGRpcYdbTableService::TGRpcYdbTableService(NActors::TActorSystem *system, TIntrusivePtr<NMonitoring::TDynamicCounters> counters, NActors::TActorId id) 
    : ActorSystem_(system)
    , Counters_(counters)
    , GRpcRequestProxyId_(id)
{ }

void TGRpcYdbTableService::InitService(grpc::ServerCompletionQueue *cq, NGrpc::TLoggerPtr logger) { 
    CQ_ = cq;
    SetupIncomingRequests(std::move(logger)); 
}

void TGRpcYdbTableService::SetGlobalLimiterHandle(NGrpc::TGlobalLimiter* limiter) { 
    Limiter_ = limiter;
}

bool TGRpcYdbTableService::IncRequest() {
    return Limiter_->Inc();
}

void TGRpcYdbTableService::DecRequest() {
    Limiter_->Dec();
    Y_ASSERT(Limiter_->GetCurrentInFlight() >= 0);
}

void TGRpcYdbTableService::SetupIncomingRequests(NGrpc::TLoggerPtr logger) { 
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);
#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, IN, OUT, ACTION) \
    MakeIntrusive<TGRpcRequest<Ydb::Table::IN, Ydb::Table::OUT, TGRpcYdbTableService>>(this, &Service_, CQ_, \
        [this](NGrpc::IRequestContextBase *ctx) { \ 
            NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer()); \
            ACTION; \
        }, &Ydb::Table::V1::TableService::AsyncService::Request ## NAME, \
        #NAME, logger, getCounterBlock("table", #NAME))->Run(); 

#define ADD_BYTES_REQUEST(NAME, IN, OUT, ACTION) \
    MakeIntrusive<TGRpcRequest<Ydb::Table::IN, Ydb::Table::OUT, TGRpcYdbTableService>>(this, &Service_, CQ_, \
        [this](NGrpc::IRequestContextBase *ctx) { \ 
            NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer()); \
            ACTION; \
        }, &Ydb::Table::V1::TableService::AsyncService::Request ## NAME, \
        #NAME, logger, getCounterBlock("table", #NAME))->Run(); 

    ADD_REQUEST(CreateSession, CreateSessionRequest, CreateSessionResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvCreateSessionRequest(ctx));
    })
    ADD_REQUEST(DeleteSession, DeleteSessionRequest, DeleteSessionResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvDeleteSessionRequest(ctx));
    })
    ADD_REQUEST(KeepAlive, KeepAliveRequest, KeepAliveResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvKeepAliveRequest(ctx));
    })
    ADD_REQUEST(AlterTable, AlterTableRequest, AlterTableResponse, {
       ActorSystem_->Send(GRpcRequestProxyId_, new TEvAlterTableRequest(ctx));
    })
    ADD_REQUEST(CreateTable, CreateTableRequest, CreateTableResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvCreateTableRequest(ctx));
    })
    ADD_REQUEST(DropTable, DropTableRequest, DropTableResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvDropTableRequest(ctx));
    })
    ADD_BYTES_REQUEST(StreamReadTable, ReadTableRequest, ReadTableResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvReadTableRequest(ctx));
    })
    ADD_REQUEST(DescribeTable, DescribeTableRequest, DescribeTableResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvDescribeTableRequest(ctx));
    })
    ADD_REQUEST(CopyTable, CopyTableRequest, CopyTableResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvCopyTableRequest(ctx));
    })
    ADD_REQUEST(CopyTables, CopyTablesRequest, CopyTablesResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvCopyTablesRequest(ctx));
    })
    ADD_REQUEST(RenameTables, RenameTablesRequest, RenameTablesResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvRenameTablesRequest(ctx));
    })
    ADD_REQUEST(ExplainDataQuery, ExplainDataQueryRequest, ExplainDataQueryResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvExplainDataQueryRequest(ctx));
    })
    ADD_REQUEST(PrepareDataQuery, PrepareDataQueryRequest, PrepareDataQueryResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvPrepareDataQueryRequest(ctx));
    })
    ADD_REQUEST(ExecuteDataQuery, ExecuteDataQueryRequest, ExecuteDataQueryResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvExecuteDataQueryRequest(ctx));
    })
    ADD_REQUEST(ExecuteSchemeQuery, ExecuteSchemeQueryRequest, ExecuteSchemeQueryResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvExecuteSchemeQueryRequest(ctx));
    })
    ADD_REQUEST(BeginTransaction, BeginTransactionRequest, BeginTransactionResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvBeginTransactionRequest(ctx));
    })
    ADD_REQUEST(CommitTransaction, CommitTransactionRequest, CommitTransactionResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvCommitTransactionRequest(ctx));
    })
    ADD_REQUEST(RollbackTransaction, RollbackTransactionRequest, RollbackTransactionResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvRollbackTransactionRequest(ctx));
    })
    ADD_REQUEST(DescribeTableOptions, DescribeTableOptionsRequest, DescribeTableOptionsResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvDescribeTableOptionsRequest(ctx));
    })
    ADD_REQUEST(BulkUpsert, BulkUpsertRequest, BulkUpsertResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvBulkUpsertRequest(ctx));
    })
    ADD_REQUEST(StreamExecuteScanQuery, ExecuteScanQueryRequest, ExecuteScanQueryPartialResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvStreamExecuteScanQueryRequest(ctx));
    })

#undef ADD_REQUEST
}

} // namespace NGRpcService
} // namespace NKikimr
