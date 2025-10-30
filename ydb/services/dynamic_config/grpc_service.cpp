#include "grpc_service.h"

#include <ydb/core/grpc_services/service_dynamic_config.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>

namespace NKikimr {
namespace NGRpcService {

void TGRpcDynamicConfigService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace Ydb::DynamicConfig;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef SETUP_CFG_METHOD
#error SETUP_CFG_METHOD macro already defined
#endif

#define SETUP_CFG_METHOD(methodName, methodCallback, rlMode, requestType, auditMode) \
    SETUP_METHOD(methodName, methodCallback, rlMode, requestType, console, auditMode)

    SETUP_CFG_METHOD(SetConfig, DoSetConfigRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));
    SETUP_CFG_METHOD(ReplaceConfig, DoReplaceConfigRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));
    SETUP_CFG_METHOD(DropConfig, DoDropConfigRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));
    SETUP_CFG_METHOD(AddVolatileConfig, DoAddVolatileConfigRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));
    SETUP_CFG_METHOD(RemoveVolatileConfig, DoRemoveVolatileConfigRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));
    SETUP_CFG_METHOD(GetConfig, DoGetConfigRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::NonModifying());
    SETUP_CFG_METHOD(GetMetadata, DoGetMetadataRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::NonModifying());
    SETUP_CFG_METHOD(GetNodeLabels, DoGetNodeLabelsRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::NonModifying());
    SETUP_CFG_METHOD(ResolveConfig, DoResolveConfigRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::NonModifying());
    SETUP_CFG_METHOD(ResolveAllConfig, DoResolveAllConfigRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::NonModifying());
    SETUP_CFG_METHOD(FetchStartupConfig, DoFetchStartupConfigRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::NonModifying());
    SETUP_CFG_METHOD(GetConfigurationVersion, DoGetConfigurationVersionRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::NonModifying());

#undef SETUP_CFG_METHOD
}

} // namespace NGRpcService
} // namespace NKikimr
