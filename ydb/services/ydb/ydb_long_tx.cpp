#include "ydb_long_tx.h"

#include <ydb/core/grpc_services/service_longtx.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>

namespace NKikimr {
namespace NGRpcService {


TGRpcYdbLongTxService::TGRpcYdbLongTxService(NActors::TActorSystem* system,
                                             TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
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
#define ADD_REQUEST(NAME, REQ, CB) \
    MakeIntrusive<TGRpcRequest<Ydb::LongTx::REQ##Request, Ydb::LongTx::REQ##Response, TGRpcYdbLongTxService>>   \
        (this, &Service_, CQ_,                                                                                  \
            [this](NGrpc::IRequestContextBase *ctx) {                                                           \
                NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer());                                \
                ActorSystem_->Send(GRpcRequestProxyId_,                                                         \
                    new TGrpcRequestOperationCall<Ydb::LongTx::REQ##Request, Ydb::LongTx::REQ##Response>        \
                        (ctx, &CB, TRequestAuxSettings{TRateLimiterMode::Off, nullptr}));                       \
            }, &Ydb::LongTx::V1::LongTxService::AsyncService::Request ## NAME,                                  \
            #NAME, logger, getCounterBlock("long_tx", #NAME))->Run();

    ADD_REQUEST(BeginTx, BeginTransaction, DoLongTxBeginRPC)
    ADD_REQUEST(CommitTx, CommitTransaction, DoLongTxCommitRPC)
    ADD_REQUEST(RollbackTx, RollbackTransaction, DoLongTxRollbackRPC)
    ADD_REQUEST(Write, Write, DoLongTxWriteRPC)
    ADD_REQUEST(Read, Read, DoLongTxReadRPC)
#undef ADD_REQUEST
}

} // namespace NGRpcService
} // namespace NKikimr
