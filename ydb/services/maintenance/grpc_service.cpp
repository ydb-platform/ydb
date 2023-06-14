#include "grpc_service.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/grpc_services/service_maintenance.h>

namespace NKikimr::NGRpcService {

void TGRpcMaintenanceService::SetupIncomingRequests(NGrpc::TLoggerPtr logger) {
    Y_UNUSED(logger);

    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);
    using namespace Ydb;

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif

#define ADD_REQUEST(NAME, REQUEST, RESPONSE, CB) \
    MakeIntrusive<TGRpcRequest<Maintenance::REQUEST, Maintenance::RESPONSE, TGRpcMaintenanceService>> \
        (this, &Service_, CQ_,                                                                        \
            [this](NGrpc::IRequestContextBase *ctx) {                                                 \
                NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer());                      \
                ActorSystem_->Send(GRpcRequestProxyId_,                                               \
                    new TGrpcRequestOperationCall<Maintenance::REQUEST, Maintenance::RESPONSE>        \
                        (ctx, &CB, TRequestAuxSettings{RLSWITCH(TRateLimiterMode::Rps), nullptr}));   \
            }, &Maintenance::V1::MaintenanceService::AsyncService::Request ## NAME,                   \
            #NAME, logger, getCounterBlock("maintenance", #NAME))->Run();

    ADD_REQUEST(ListClusterNodes, ListClusterNodesRequest, ListClusterNodesResponse, DoListClusterNodes);
    ADD_REQUEST(CreateMaintenanceTask, CreateMaintenanceTaskRequest, MaintenanceTaskResponse, DoCreateMaintenanceTask);
    ADD_REQUEST(RefreshMaintenanceTask, RefreshMaintenanceTaskRequest, MaintenanceTaskResponse, DoRefreshMaintenanceTask);
    ADD_REQUEST(GetMaintenanceTask, GetMaintenanceTaskRequest, GetMaintenanceTaskResponse, DoGetMaintenanceTask);
    ADD_REQUEST(ListMaintenanceTasks, ListMaintenanceTasksRequest, ListMaintenanceTasksResponse, DoListMaintenanceTasks);
    ADD_REQUEST(DropMaintenanceTask, DropMaintenanceTaskRequest, ManageMaintenanceTaskResponse, DoDropMaintenanceTask);
    ADD_REQUEST(CompleteAction, CompleteActionRequest, ManageActionResponse, DoCompleteAction);

#undef ADD_REQUEST
}

}
