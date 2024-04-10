#include "service_maintenance.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/cms/cms.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/grpc_services/rpc_request_base.h>
#include <ydb/public/api/protos/draft/ydb_maintenance.pb.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NGRpcService {

using namespace Ydb;

using TEvListClusterNodes = TGrpcRequestOperationCall<Maintenance::ListClusterNodesRequest, Maintenance::ListClusterNodesResponse>;
using TEvCreateMaintenanceTask = TGrpcRequestOperationCall<Maintenance::CreateMaintenanceTaskRequest, Maintenance::MaintenanceTaskResponse>;
using TEvRefreshMaintenanceTask = TGrpcRequestOperationCall<Maintenance::RefreshMaintenanceTaskRequest, Maintenance::MaintenanceTaskResponse>;
using TEvGetMaintenanceTask = TGrpcRequestOperationCall<Maintenance::GetMaintenanceTaskRequest, Maintenance::GetMaintenanceTaskResponse>;
using TEvListMaintenanceTasks = TGrpcRequestOperationCall<Maintenance::ListMaintenanceTasksRequest, Maintenance::ListMaintenanceTasksResponse>;
using TEvDropMaintenanceTask = TGrpcRequestOperationCall<Maintenance::DropMaintenanceTaskRequest, Maintenance::ManageMaintenanceTaskResponse>;
using TEvCompleteAction = TGrpcRequestOperationCall<Maintenance::CompleteActionRequest, Maintenance::ManageActionResponse>;

template <typename TEvRequest, typename TEvCmsRequest, typename TEvCmsResponse>
class TMaintenanceRPC: public TRpcRequestActor<TMaintenanceRPC<TEvRequest, TEvCmsRequest, TEvCmsResponse>, TEvRequest, true> {
    using TThis = TMaintenanceRPC<TEvRequest, TEvCmsRequest, TEvCmsResponse>;
    using TBase = TRpcRequestActor<TThis, TEvRequest, true>;

    bool CheckAccess() const {
        if (AppData()->AdministrationAllowedSIDs.empty()) {
            return true;
        }

        if (!this->UserToken) {
            return false;
        }

        for (const auto& sid : AppData()->AdministrationAllowedSIDs) {
            if (this->UserToken->IsExist(sid)) {
                return true;
            }
        }

        return false;
    }

    void SendRequest() {
        auto ev = MakeHolder<TEvCmsRequest>();
        ev->Record.MutableRequest()->CopyFrom(*this->GetProtoRequest());

        if constexpr (std::is_same_v<TEvCmsRequest, NCms::TEvCms::TEvCreateMaintenanceTaskRequest>) {
            if (this->UserToken) {
                ev->Record.SetUserSID(this->UserToken->GetUserSID());
            }
        }

        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = {.RetryLimitCount = 10};
        CmsPipe = this->RegisterWithSameMailbox(NTabletPipe::CreateClient(this->SelfId(), MakeCmsID(), pipeConfig));

        NTabletPipe::SendData(this->SelfId(), CmsPipe, ev.Release());
    }

    void Handle(typename TEvCmsResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        Ydb::Operations::Operation operation;
        operation.set_ready(true);
        operation.set_status(record.GetStatus());
        if (record.IssuesSize()) {
            operation.mutable_issues()->CopyFrom(record.GetIssues());
        }

        if constexpr (!std::is_same_v<TEvCmsResponse, NCms::TEvCms::TEvManageMaintenanceTaskResponse>) {
            operation.mutable_result()->PackFrom(record.GetResult());
        }

        this->Reply(operation);
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            Unavailable();
        }
    }

    void Unavailable() {
        this->Reply(Ydb::StatusIds::UNAVAILABLE, "CMS is unavailable");
    }

    void PassAway() override {
        NTabletPipe::CloseAndForgetClient(this->SelfId(), CmsPipe);
        TBase::PassAway();
    }

public:
    using TBase::TBase;

    void Bootstrap() {
        if (!CheckAccess()) {
            auto error = TStringBuilder() << "Access denied";
            if (this->UserToken) {
                error << ": '" << this->UserToken->GetUserSID() << "' is not an admin";
            }

            this->Reply(Ydb::StatusIds::UNAUTHORIZED, NKikimrIssues::TIssuesIds::ACCESS_DENIED, error);
        } else {
            SendRequest();
            this->Become(&TThis::StateWork);
        }
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvCmsResponse, Handle)
        hFunc(TEvTabletPipe::TEvClientConnected, Handle)
        sFunc(TEvTabletPipe::TEvClientDestroyed, Unavailable)
    )

private:
    TActorId CmsPipe;
};

void DoListClusterNodes(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TMaintenanceRPC<TEvListClusterNodes,
                                        NCms::TEvCms::TEvListClusterNodesRequest,
                                        NCms::TEvCms::TEvListClusterNodesResponse>(p.release()));
}

void DoCreateMaintenanceTask(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TMaintenanceRPC<TEvCreateMaintenanceTask,
                                        NCms::TEvCms::TEvCreateMaintenanceTaskRequest,
                                        NCms::TEvCms::TEvMaintenanceTaskResponse>(p.release()));
}

void DoRefreshMaintenanceTask(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TMaintenanceRPC<TEvRefreshMaintenanceTask,
                                        NCms::TEvCms::TEvRefreshMaintenanceTaskRequest,
                                        NCms::TEvCms::TEvMaintenanceTaskResponse>(p.release()));
}

void DoGetMaintenanceTask(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TMaintenanceRPC<TEvGetMaintenanceTask,
                                        NCms::TEvCms::TEvGetMaintenanceTaskRequest,
                                        NCms::TEvCms::TEvGetMaintenanceTaskResponse>(p.release()));
}

void DoListMaintenanceTasks(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TMaintenanceRPC<TEvListMaintenanceTasks,
                                        NCms::TEvCms::TEvListMaintenanceTasksRequest,
                                        NCms::TEvCms::TEvListMaintenanceTasksResponse>(p.release()));
}

void DoDropMaintenanceTask(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TMaintenanceRPC<TEvDropMaintenanceTask,
                                        NCms::TEvCms::TEvDropMaintenanceTaskRequest,
                                        NCms::TEvCms::TEvManageMaintenanceTaskResponse>(p.release()));
}

void DoCompleteAction(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TMaintenanceRPC<TEvCompleteAction,
                                        NCms::TEvCms::TEvCompleteActionRequest,
                                        NCms::TEvCms::TEvManageActionResponse>(p.release()));
}

}
