#include "ydb_operation.h"

#include <ydb/core/grpc_services/service_operation.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/public/api/protos/ydb_operation.pb.h>

namespace NKikimr {
namespace NGRpcService {

void TGRpcOperationService::SetupIncomingRequests(NGrpc::TLoggerPtr logger) {
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);
    using namespace Ydb;

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, CB, TCALL)                                                                              \
    MakeIntrusive<TGRpcRequest<Operations::NAME##Request, Operations::NAME##Response, TGRpcOperationService>>     \
        (this, &Service_, CQ_,                                                                                    \
            [this](NGrpc::IRequestContextBase *ctx) {                                                             \
                NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer());                                  \
                ActorSystem_->Send(GRpcRequestProxyId_,                                                           \
                    new TCALL<Operations::NAME##Request, Operations::NAME##Response>                              \
                        (ctx, &CB, TRequestAuxSettings{RLSWITCH(TRateLimiterMode::Rps), nullptr}));                         \
            }, &Operation::V1::OperationService::AsyncService::Request ## NAME,                                   \
            #NAME, logger, getCounterBlock("operation", #NAME))->Run();

    ADD_REQUEST(GetOperation, DoGetOperationRequest, TGrpcRequestOperationCall)
    ADD_REQUEST(CancelOperation, DoCancelOperationRequest, TGrpcRequestNoOperationCall)
    ADD_REQUEST(ForgetOperation, DoForgetOperationRequest, TGrpcRequestNoOperationCall)
    ADD_REQUEST(ListOperations, DoListOperationsRequest, TGrpcRequestNoOperationCall)

#undef ADD_REQUEST
}

} // namespace NGRpcService
} // namespace NKikimr
