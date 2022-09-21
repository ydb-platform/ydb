#include "grpc_service.h"

#include <ydb/core/grpc_services/service_cms.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>

namespace NKikimr {
namespace NGRpcService {

void TGRpcCmsService::SetupIncomingRequests(NGrpc::TLoggerPtr logger) {
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);
    using namespace Ydb;

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, CB) \
    MakeIntrusive<TGRpcRequest<Cms::NAME##Request, Cms::NAME##Response, TGRpcCmsService>>          \
        (this, &Service_, CQ_,                                                                     \
            [this](NGrpc::IRequestContextBase *ctx) {                                              \
                NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer());                   \
                ActorSystem_->Send(GRpcRequestProxyId_,                                            \
                    new TGrpcRequestOperationCall<Cms::NAME##Request, Cms::NAME##Response>         \
                        (ctx, &CB, TRequestAuxSettings{RLSWITCH(TRateLimiterMode::Rps), nullptr}));          \
            }, &Cms::V1::CmsService::AsyncService::Request ## NAME,                             \
            #NAME, logger, getCounterBlock("cms", #NAME))->Run();

    ADD_REQUEST(CreateDatabase, DoCreateTenantRequest)
    ADD_REQUEST(AlterDatabase, DoAlterTenantRequest)
    ADD_REQUEST(GetDatabaseStatus, DoGetTenantStatusRequest)
    ADD_REQUEST(ListDatabases, DoListTenantsRequest)
    ADD_REQUEST(RemoveDatabase, DoRemoveTenantRequest)
    ADD_REQUEST(DescribeDatabaseOptions, DoDescribeTenantOptionsRequest)

#undef ADD_REQUEST
}

} // namespace NGRpcService
} // namespace NKikimr
