#include "grpc_service.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/grpc_services/service_replication.h>

namespace NKikimr::NGRpcService {

void TGRpcReplicationService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    Y_UNUSED(logger);

    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);
    using namespace Ydb;

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif

#define ADD_REQUEST(NAME, REQUEST, RESPONSE, CB) \
    MakeIntrusive<TGRpcRequest<Replication::REQUEST, Replication::RESPONSE, TGRpcReplicationService>> \
        (this, &Service_, CQ_,                                                                        \
            [this](NYdbGrpc::IRequestContextBase *ctx) {                                              \
                NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer());                      \
                ActorSystem_->Send(GRpcRequestProxyId_,                                               \
                    new TGrpcRequestOperationCall<Replication::REQUEST, Replication::RESPONSE>        \
                        (ctx, &CB, TRequestAuxSettings{RLSWITCH(TRateLimiterMode::Rps), nullptr}));   \
            }, &Replication::V1::ReplicationService::AsyncService::Request ## NAME,                   \
            #NAME, logger, getCounterBlock("replication", #NAME))->Run();

    ADD_REQUEST(DescribeReplication, DescribeReplicationRequest, DescribeReplicationResponse, DoDescribeReplication);

#undef ADD_REQUEST
}

}
