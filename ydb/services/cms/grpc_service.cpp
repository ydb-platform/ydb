#include "grpc_service.h"

#include <ydb/core/grpc_services/service_cms.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>

namespace NKikimr {
namespace NGRpcService {

void TGRpcCmsService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace Ydb::Cms;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef SETUP_CMS_METHOD
#error SETUP_CMS_METHOD macro already defined
#endif

#define SETUP_CMS_METHOD(methodName, methodCallback, rlMode, requestType, auditMode, operationCallClass) \
    SETUP_RUNTIME_EVENT_METHOD(methodName,                                                               \
        YDB_API_DEFAULT_REQUEST_TYPE(methodName),                                                        \
        YDB_API_DEFAULT_RESPONSE_TYPE(methodName),                                                       \
        methodCallback,                                                                                  \
        rlMode,                                                                                          \
        requestType,                                                                                     \
        YDB_API_DEFAULT_COUNTER_BLOCK(cms, methodName),                                                  \
        auditMode,                                                                                       \
        COMMON,                                                                                          \
        operationCallClass,                                                                              \
        GRpcRequestProxyId_,                                                                             \
        CQ_,                                                                                             \
        nullptr,                                                                                         \
        nullptr)

    SETUP_CMS_METHOD(CreateDatabase, DoCreateTenantRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin), TGrpcRequestOperationCall);
    SETUP_CMS_METHOD(AlterDatabase, DoAlterTenantRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin), TGrpcRequestOperationCall);
    SETUP_CMS_METHOD(GetDatabaseStatus, DoGetTenantStatusRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::NonModifying(), TGrpcRequestOperationCall);
    SETUP_CMS_METHOD(ListDatabases, DoListTenantsRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::NonModifying(), TGrpcRequestOperationCall);
    SETUP_CMS_METHOD(RemoveDatabase, DoRemoveTenantRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin), TGrpcRequestOperationCall);
    SETUP_CMS_METHOD(DescribeDatabaseOptions, DoDescribeTenantOptionsRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::NonModifying(), TGrpcRequestOperationCall);
    SETUP_CMS_METHOD(GetScaleRecommendation, DoGetScaleRecommendationRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::NonModifying(), TGrpcRequestNoOperationCall);

#undef SETUP_CMS_METHOD
}

} // namespace NGRpcService
} // namespace NKikimr
