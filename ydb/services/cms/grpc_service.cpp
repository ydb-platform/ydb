#include "grpc_service.h"

#include <ydb/core/grpc_services/service_cms.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>

namespace NKikimr {
namespace NGRpcService {

void TGRpcCmsService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);
    using namespace Ydb;

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, CB, TCALL) \
    MakeIntrusive<TGRpcRequest<Cms::NAME##Request, Cms::NAME##Response, TGRpcCmsService>>          \
        (this, &Service_, CQ_,                                                                     \
            [this](NYdbGrpc::IRequestContextBase *ctx) {                                              \
                NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer());                   \
                ActorSystem_->Send(GRpcRequestProxyId_,                                            \
                    new TCALL<Cms::NAME##Request, Cms::NAME##Response>         \
                        (ctx, &CB, TRequestAuxSettings{RLSWITCH(TRateLimiterMode::Rps), nullptr}));          \
            }, &Cms::V1::CmsService::AsyncService::Request ## NAME,                             \
            #NAME, logger, getCounterBlock("cms", #NAME))->Run();

    ADD_REQUEST(CreateDatabase, DoCreateTenantRequest, TGrpcRequestOperationCall)
    ADD_REQUEST(AlterDatabase, DoAlterTenantRequest, TGrpcRequestOperationCall)
    ADD_REQUEST(GetDatabaseStatus, DoGetTenantStatusRequest, TGrpcRequestOperationCall)
    ADD_REQUEST(ListDatabases, DoListTenantsRequest, TGrpcRequestOperationCall)
    ADD_REQUEST(RemoveDatabase, DoRemoveTenantRequest, TGrpcRequestOperationCall)
    ADD_REQUEST(DescribeDatabaseOptions, DoDescribeTenantOptionsRequest, TGrpcRequestOperationCall)
    ADD_REQUEST(GetScaleRecommendation, DoGetScaleRecommendationRequest, TGrpcRequestNoOperationCall)

#undef ADD_REQUEST
}

} // namespace NGRpcService
} // namespace NKikimr
