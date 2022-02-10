#include "ydb_long_tx.h" 
 
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/rpc_calls.h>
 
namespace NKikimr { 
namespace NGRpcService { 
 
 
TGRpcYdbLongTxService::TGRpcYdbLongTxService(NActors::TActorSystem* system, 
                                             TIntrusivePtr<NMonitoring::TDynamicCounters> counters, 
                                             NActors::TActorId id) 
    : ActorSystem_(system) 
    , Counters_(counters) 
    , GRpcRequestProxyId_(id) 
{} 
 
void TGRpcYdbLongTxService::InitService(grpc::ServerCompletionQueue* cq, NGrpc::TLoggerPtr logger) {
    CQ_ = cq; 
    SetupIncomingRequests(std::move(logger));
} 
 
void TGRpcYdbLongTxService::SetGlobalLimiterHandle(NGrpc::TGlobalLimiter* limiter) {
    Limiter_ = limiter; 
} 
 
bool TGRpcYdbLongTxService::IncRequest() { 
    return Limiter_->Inc(); 
} 
 
void TGRpcYdbLongTxService::DecRequest() { 
    Limiter_->Dec(); 
    Y_ASSERT(Limiter_->GetCurrentInFlight() >= 0); 
} 
 
void TGRpcYdbLongTxService::SetupIncomingRequests(NGrpc::TLoggerPtr logger) {
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_); 
 
#ifdef ADD_REQUEST 
#error ADD_REQUEST macro already defined 
#endif 
#define ADD_REQUEST(NAME, IN, OUT, ACTION) \ 
    MakeIntrusive<TGRpcRequest<Ydb::LongTx::IN, Ydb::LongTx::OUT, TGRpcYdbLongTxService>>(this, &Service_, CQ_, \ 
        [this](NGrpc::IRequestContextBase *ctx) { \
            NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer()); \ 
            ACTION; \ 
        }, &Ydb::LongTx::V1::LongTxService::AsyncService::Request ## NAME, \ 
        #NAME, logger, getCounterBlock("long_tx", #NAME))->Run();
 
    ADD_REQUEST(BeginTx, BeginTransactionRequest, BeginTransactionResponse, { 
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvLongTxBeginRequest(ctx)); 
    }) 
    ADD_REQUEST(CommitTx, CommitTransactionRequest, CommitTransactionResponse, { 
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvLongTxCommitRequest(ctx)); 
    }) 
    ADD_REQUEST(RollbackTx, RollbackTransactionRequest, RollbackTransactionResponse, { 
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvLongTxRollbackRequest(ctx)); 
    }) 
    ADD_REQUEST(Write, WriteRequest, WriteResponse, { 
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvLongTxWriteRequest(ctx)); 
    }) 
    ADD_REQUEST(Read, ReadRequest, ReadResponse, { 
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvLongTxReadRequest(ctx)); 
    }) 
#undef ADD_REQUEST 
} 
 
} // namespace NGRpcService 
} // namespace NKikimr 
