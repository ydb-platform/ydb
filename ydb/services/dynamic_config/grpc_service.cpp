#include "grpc_service.h"

#include <ydb/core/grpc_services/service_dynamic_config.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>

namespace NKikimr {
namespace NGRpcService {

void TGRpcDynamicConfigService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);
    using namespace Ydb;

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, CB)                                                                         \
    MakeIntrusive<TGRpcRequest<DynamicConfig::NAME##Request, DynamicConfig::NAME##Response, TGRpcDynamicConfigService>> \
        (this, &Service_, CQ_,                                                                        \
            [this](NYdbGrpc::IRequestContextBase *ctx) {                                                 \
                NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer());                      \
                ActorSystem_->Send(GRpcRequestProxyId_,                                               \
                    new TGrpcRequestOperationCall<DynamicConfig::NAME##Request, DynamicConfig::NAME##Response>    \
                        (ctx, &CB, TRequestAuxSettings{RLSWITCH(TRateLimiterMode::Rps), nullptr}));   \
            }, &DynamicConfig::V1::DynamicConfigService::AsyncService::Request ## NAME,                           \
            #NAME, logger, getCounterBlock("console", #NAME))->Run();

    ADD_REQUEST(SetConfig, DoSetConfigRequest)
    ADD_REQUEST(ReplaceConfig, DoReplaceConfigRequest)
    ADD_REQUEST(DropConfig, DoDropConfigRequest)
    ADD_REQUEST(AddVolatileConfig, DoAddVolatileConfigRequest)
    ADD_REQUEST(RemoveVolatileConfig, DoRemoveVolatileConfigRequest)
    ADD_REQUEST(GetConfig, DoGetConfigRequest)
    ADD_REQUEST(GetMetadata, DoGetMetadataRequest)
    ADD_REQUEST(GetNodeLabels, DoGetNodeLabelsRequest)
    ADD_REQUEST(ResolveConfig, DoResolveConfigRequest)
    ADD_REQUEST(ResolveAllConfig, DoResolveAllConfigRequest)
    ADD_REQUEST(FetchStartupConfig, DoFetchStartupConfigRequest)
    ADD_REQUEST(GetConfigurationVersion, DoGetConfigurationVersionRequest);

#undef ADD_REQUEST
}

} // namespace NGRpcService
} // namespace NKikimr
