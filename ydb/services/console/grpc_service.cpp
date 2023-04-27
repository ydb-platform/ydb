#include "grpc_service.h"

#include <ydb/core/grpc_services/service_console.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>

namespace NKikimr {
namespace NGRpcService {

void TGRpcConsoleService::SetupIncomingRequests(NGrpc::TLoggerPtr logger) {
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);
    using namespace Ydb;

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, CB)                                                                         \
    MakeIntrusive<TGRpcRequest<Console::NAME##Request, Console::NAME##Response, TGRpcConsoleService>> \
        (this, &Service_, CQ_,                                                                        \
            [this](NGrpc::IRequestContextBase *ctx) {                                                 \
                NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer());                      \
                ActorSystem_->Send(GRpcRequestProxyId_,                                               \
                    new TGrpcRequestOperationCall<Console::NAME##Request, Console::NAME##Response>    \
                        (ctx, &CB, TRequestAuxSettings{RLSWITCH(TRateLimiterMode::Rps), nullptr}));   \
            }, &Console::V1::ConsoleService::AsyncService::Request ## NAME,                           \
            #NAME, logger, getCounterBlock("console", #NAME))->Run();

    ADD_REQUEST(ApplyConfig, DoApplyConfigRequest)
    ADD_REQUEST(DropConfig, DoDropConfigRequest)
    ADD_REQUEST(AddVolatileConfig, DoAddVolatileConfigRequest)
    ADD_REQUEST(RemoveVolatileConfig, DoRemoveVolatileConfigRequest)
    ADD_REQUEST(GetConfig, DoGetConfigRequest)
    ADD_REQUEST(ResolveConfig, DoResolveConfigRequest)
    ADD_REQUEST(ResolveAllConfig, DoResolveAllConfigRequest)

#undef ADD_REQUEST
}

} // namespace NGRpcService
} // namespace NKikimr
