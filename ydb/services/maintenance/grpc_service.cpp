#include "grpc_service.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/grpc_services/service_maintenance.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>

namespace NKikimr::NGRpcService {

void TGRpcMaintenanceService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace Ydb::Maintenance;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef SETUP_MAINTENANCE_METHOD
#error SETUP_MAINTENANCE_METHOD macro already defined
#endif

#define SETUP_MAINTENANCE_METHOD(methodName, inputType, outputType, methodCallback, rlMode, requestType, auditMode) \
    SETUP_RUNTIME_EVENT_METHOD(methodName,                      \
        inputType,                                              \
        outputType,                                             \
        methodCallback,                                         \
        rlMode,                                                 \
        requestType,                                            \
        YDB_API_DEFAULT_COUNTER_BLOCK(maintenance, methodName), \
        auditMode,                                              \
        COMMON,                                                 \
        ::NKikimr::NGRpcService::TGrpcRequestOperationCall,     \
        GRpcRequestProxyId_,                                    \
        CQ_,                                                    \
        nullptr,                                                \
        nullptr)

    SETUP_MAINTENANCE_METHOD(ListClusterNodes, ListClusterNodesRequest, ListClusterNodesResponse, DoListClusterNodes, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::NonModifying());
    SETUP_MAINTENANCE_METHOD(CreateMaintenanceTask, CreateMaintenanceTaskRequest, MaintenanceTaskResponse, DoCreateMaintenanceTask, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));
    SETUP_MAINTENANCE_METHOD(RefreshMaintenanceTask, RefreshMaintenanceTaskRequest, MaintenanceTaskResponse, DoRefreshMaintenanceTask, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));
    SETUP_MAINTENANCE_METHOD(GetMaintenanceTask, GetMaintenanceTaskRequest, GetMaintenanceTaskResponse, DoGetMaintenanceTask, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::NonModifying());
    SETUP_MAINTENANCE_METHOD(ListMaintenanceTasks, ListMaintenanceTasksRequest, ListMaintenanceTasksResponse, DoListMaintenanceTasks, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::NonModifying());
    SETUP_MAINTENANCE_METHOD(DropMaintenanceTask, DropMaintenanceTaskRequest, ManageMaintenanceTaskResponse, DoDropMaintenanceTask, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));
    SETUP_MAINTENANCE_METHOD(CompleteAction, CompleteActionRequest, ManageActionResponse, DoCompleteAction, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));

#undef SETUP_MAINTENANCE_METHOD
}

}
