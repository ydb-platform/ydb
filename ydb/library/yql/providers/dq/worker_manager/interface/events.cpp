#include "events.h"

namespace NYql::NCommonAttrs {
    extern TString OPERATIONID_ATTR;
    extern TString JOBID_ATTR;
}

namespace NYql::NDqs {
    TEvAllocateWorkersRequest::TEvAllocateWorkersRequest(
        ui32 count,
        const TString& user,
        const TMaybe<ui64>& globalResourceId)
    {
        Record.SetCount(count);
        Record.SetUser(user);
        if (globalResourceId) {
            Record.SetResourceId(*globalResourceId);
        }
        Record.SetIsForwarded(false);
    }

    TEvAllocateWorkersResponse::TEvAllocateWorkersResponse() {
        Record.MutableError()->SetMessage("Unknown error");
    }

    TEvAllocateWorkersResponse::TEvAllocateWorkersResponse(const TString& error, NYql::NDqProto::StatusIds::StatusCode statusCode) {
        Record.MutableError()->SetMessage(error);
        Record.MutableError()->SetStatusCode(statusCode);
    }

    TEvAllocateWorkersResponse::TEvAllocateWorkersResponse(ui64 resourceId, const TVector<NActors::TActorId>& ids) {
        auto& group = *Record.MutableWorkers();
        group.SetResourceId(resourceId);
        for (const auto& actorId : ids) {
            NActors::ActorIdToProto(actorId, group.AddWorkerActor());
        }
    }

    TEvAllocateWorkersResponse::TEvAllocateWorkersResponse(ui64 resourceId, const TVector<ui32>& nodes) {
        auto& group = *Record.MutableNodes();
        group.SetResourceId(resourceId);
        for (const auto& node : nodes) {
            auto* worker = group.AddWorker();
            worker->SetNodeId(node);
        }
    }

    TEvAllocateWorkersResponse::TEvAllocateWorkersResponse(ui64 resourceId, const TVector<TWorkerInfo::TPtr>& infos) {
        auto& group = *Record.MutableNodes();
        group.SetResourceId(resourceId);
        for (const auto& workerInfo : infos) {
            auto* worker = group.AddWorker();
            *worker->MutableGuid() = GetGuidAsString(workerInfo->WorkerId); // for debug only
            worker->SetNodeId(workerInfo->NodeId);
            worker->SetClusterName(workerInfo->ClusterName);

            for (const auto& [k, v] : workerInfo->Attributes) {
                auto* attr = worker->AddAttribute();
                attr->SetKey(k);
                attr->SetValue(v);

                if (k == NCommonAttrs::JOBID_ATTR) {
                    worker->SetJobId(v);
                }
                if (k == NCommonAttrs::OPERATIONID_ATTR) {
                    worker->SetOperationId(v);
                }
            }
        }
    }

    TEvFreeWorkersNotify::TEvFreeWorkersNotify(ui64 resourceId) {
        Record.SetResourceId(resourceId);
    }

    TEvRegisterNode::TEvRegisterNode(const Yql::DqsProto::RegisterNodeRequest& request) {
        *Record.MutableRequest() = request;
        Record.SetIsForwarded(false);
    }

    TEvRegisterNodeResponse::TEvRegisterNodeResponse(const TVector<NActors::TEvInterconnect::TNodeInfo>& nodes, ui32 epoch)
    {
        auto* response = Record.MutableResponse();
        for (const auto& node : nodes) {
            auto* n = response->AddNodes();
            n->SetPort(node.Port);
            n->SetAddress(node.Address);
            n->SetNodeId(node.NodeId);
        }
        response->SetEpoch(epoch);
    }

    TEvJobStop::TEvJobStop(const Yql::DqsProto::JobStopRequest& request) {
        *Record.MutableRequest() = request;
        Record.SetIsForwarded(false);
    }

    TEvOperationStop::TEvOperationStop(const Yql::DqsProto::OperationStopRequest& request) {
        *Record.MutableRequest() = request;
        Record.SetIsForwarded(false);
    }

    TEvIsReady::TEvIsReady(const Yql::DqsProto::IsReadyRequest &request) {
        *Record.MutableRequest() = request;
        Record.SetIsForwarded(false);
    }

    TEvConfigureFailureInjectorRequest::TEvConfigureFailureInjectorRequest(const Yql::DqsProto::ConfigureFailureInjectorRequest& request) {
        *Record.MutableRequest() = request;
    }

    TEvQueryStatus::TEvQueryStatus(const Yql::DqsProto::QueryStatusRequest& request) {
        *Record.MutableRequest() = request;
    }
} // namespace NYql::NDqs
