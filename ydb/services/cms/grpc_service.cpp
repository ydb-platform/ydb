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
#define ADD_REQUEST(NAME, CB, TCALL, AUDIT_MODE)                                                                \
    MakeIntrusive<TGRpcRequest<Cms::NAME##Request, Cms::NAME##Response, TGRpcCmsService>>                       \
        (this, &Service_, CQ_,                                                                                  \
            [this](NYdbGrpc::IRequestContextBase *ctx) {                                                        \
                NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer());                                \
                ActorSystem_->Send(GRpcRequestProxyId_,                                                         \
                    new TCALL<Cms::NAME##Request, Cms::NAME##Response>                                          \
                        (ctx, &CB, TRequestAuxSettings{RLSWITCH(TRateLimiterMode::Rps), nullptr, AUDIT_MODE})); \
            }, &Cms::V1::CmsService::AsyncService::Request ## NAME,                                             \
            #NAME, logger, getCounterBlock("cms", #NAME))->Run();

    ADD_REQUEST(CreateDatabase, DoCreateTenantRequest, TGrpcRequestOperationCall, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin))
    ADD_REQUEST(AlterDatabase, DoAlterTenantRequest, TGrpcRequestOperationCall, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin))
    ADD_REQUEST(GetDatabaseStatus, DoGetTenantStatusRequest, TGrpcRequestOperationCall, TAuditMode::NonModifying())
    ADD_REQUEST(ListDatabases, DoListTenantsRequest, TGrpcRequestOperationCall, TAuditMode::NonModifying())
    ADD_REQUEST(RemoveDatabase, DoRemoveTenantRequest, TGrpcRequestOperationCall, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin))
    ADD_REQUEST(DescribeDatabaseOptions, DoDescribeTenantOptionsRequest, TGrpcRequestOperationCall, TAuditMode::NonModifying())
    ADD_REQUEST(GetScaleRecommendation, DoGetScaleRecommendationRequest, TGrpcRequestNoOperationCall, TAuditMode::NonModifying())

#undef ADD_REQUEST
}

} // namespace NGRpcService
} // namespace NKikimr
