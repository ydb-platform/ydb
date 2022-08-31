#include "ydb_long_tx.h"

#include <ydb/core/grpc_services/service_longtx.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>

namespace NKikimr {
namespace NGRpcService {

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
