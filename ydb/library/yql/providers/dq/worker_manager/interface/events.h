#pragma once

#include <ydb/library/yql/providers/dq/api/protos/dqs.pb.h>
#include "worker_info.h"
#include <ydb/library/yql/dq/common/dq_common.h>

#include <ydb/library/yql/utils/log/log.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/interconnect/interconnect.h>

#include <util/generic/guid.h>

#include <utility>

namespace NYql::NDqs {

using TDqResManEvents = NDq::TBaseDqResManEvents<NActors::TEvents::EEventSpace::ES_USERSPACE>;

    struct TEvAllocateWorkersRequest : NActors::TEventPB<TEvAllocateWorkersRequest, NYql::NDqProto::TAllocateWorkersRequest,
                                                         TDqResManEvents::ES_ALLOCATE_WORKERS_REQUEST> {
        TEvAllocateWorkersRequest() = default;

        explicit TEvAllocateWorkersRequest(
            ui32 count,
            const TString& user,
            const TMaybe<ui64>& globalResourceId = TMaybe<ui64>());
    };

    struct TEvAllocateWorkersResponse
        : NActors::TEventPB<TEvAllocateWorkersResponse, NYql::NDqProto::TAllocateWorkersResponse,
                            TDqResManEvents::ES_ALLOCATE_WORKERS_RESPONSE> {
        TEvAllocateWorkersResponse();
        explicit TEvAllocateWorkersResponse(const TString& error, NYql::NDqProto::StatusIds::StatusCode statusCode);
        explicit TEvAllocateWorkersResponse(ui64 resourceId, const TVector<NActors::TActorId>& ids);
        explicit TEvAllocateWorkersResponse(ui64 resourceId, const TVector<TWorkerInfo::TPtr>& workerInfos);
        explicit TEvAllocateWorkersResponse(ui64 resourceId, const TVector<ui32>& nodes);
    };

    struct TEvFreeWorkersNotify : NActors::TEventPB<TEvFreeWorkersNotify, NYql::NDqProto::TFreeWorkersNotify,
                                                    TDqResManEvents::ES_FREE_WORKERS_NOTIFICATION> {
        TEvFreeWorkersNotify() = default;
        explicit TEvFreeWorkersNotify(ui64 id);
    };

    struct TEvRegisterNode
        : NActors::TEventPB<TEvRegisterNode, NYql::NDqProto::TEvRegisterNode, TDqResManEvents::ES_REGISTER_NODE> {

        TEvRegisterNode() = default;
        TEvRegisterNode(const Yql::DqsProto::RegisterNodeRequest& request);
    };

    struct TEvRegisterNodeResponse
        : NActors::TEventPB<TEvRegisterNodeResponse, NYql::NDqProto::TEvRegisterNodeResponse, TDqResManEvents::ES_REGISTER_NODE_RESPONSE> {
        TEvRegisterNodeResponse() = default;

        TEvRegisterNodeResponse(const TVector<NActors::TEvInterconnect::TNodeInfo>& nodes, ui32 epoch);
    };

    struct TEvJobStop
        : NActors::TEventPB<TEvJobStop, NYql::NDqProto::TEvJobStop, TDqResManEvents::ES_JOB_STOP> {

        TEvJobStop() = default;
        TEvJobStop(const Yql::DqsProto::JobStopRequest& request);
    };

    struct TEvOperationStop
        : NActors::TEventPB<TEvOperationStop, NYql::NDqProto::TEvOperationStop, TDqResManEvents::ES_OPERATION_STOP> {

        TEvOperationStop() = default;
        TEvOperationStop(const Yql::DqsProto::OperationStopRequest& request);
    };

    struct TEvOperationStopResponse
        : NActors::TEventPB<TEvOperationStopResponse, NYql::NDqProto::TEvOperationStopResponse, TDqResManEvents::ES_OPERATION_STOP_RESPONSE> {

        TEvOperationStopResponse() = default;
    };

    struct TEvClusterStatus
        : NActors::TEventPB<TEvClusterStatus, NYql::NDqProto::TEvClusterStatus, TDqResManEvents::ES_CLUSTER_STATUS> {

        TEvClusterStatus() = default;
    };

    struct TEvClusterStatusResponse
        : NActors::TEventPB<TEvClusterStatusResponse, NYql::NDqProto::TEvClusterStatusResponse, TDqResManEvents::ES_CLUSTER_STATUS_RESPONSE> {

        TEvClusterStatusResponse() = default;
    };

    struct TEvQueryStatus
        : NActors::TEventPB<TEvQueryStatus, NYql::NDqProto::TEvQueryStatus, TDqResManEvents::ES_QUERY_STATUS> {

        TEvQueryStatus() = default;
        TEvQueryStatus(const Yql::DqsProto::QueryStatusRequest& request);
    };

    struct TEvQueryStatusResponse
        : NActors::TEventPB<TEvQueryStatusResponse, NYql::NDqProto::TEvQueryStatusResponse, TDqResManEvents::ES_QUERY_STATUS_RESPONSE> {

        TEvQueryStatusResponse() = default;
    };

    struct TEvIsReady
            : NActors::TEventPB<TEvIsReady, NYql::NDqProto::TEvIsReady, TDqResManEvents::ES_IS_READY> {

        TEvIsReady() = default;
        TEvIsReady(const Yql::DqsProto::IsReadyRequest& request);
    };

    struct TEvIsReadyResponse
            : NActors::TEventPB<TEvIsReadyResponse, NYql::NDqProto::TEvIsReadyResponse, TDqResManEvents::ES_IS_READY_RESPONSE> {

        TEvIsReadyResponse() = default;
    };

    struct TEvRoutesRequest
            : NActors::TEventPB<TEvRoutesRequest, NYql::NDqProto::TEvRoutesRequest, TDqResManEvents::ES_ROUTES> {

        TEvRoutesRequest() = default;
    };

    struct TEvRoutesResponse
            : NActors::TEventPB<TEvRoutesResponse, NYql::NDqProto::TEvRoutesResponse, TDqResManEvents::ES_ROUTES_RESPONSE> {

        TEvRoutesResponse() = default;
    };

    struct TEvGetMasterRequest
            : NActors::TEventPB<TEvGetMasterRequest, NYql::NDqProto::TEvGetMasterRequest, TDqResManEvents::ES_GET_MASTER> {

        TEvGetMasterRequest() = default;
    };

    struct TEvGetMasterResponse
            : NActors::TEventPB<TEvGetMasterResponse, NYql::NDqProto::TEvGetMasterResponse, TDqResManEvents::ES_GET_MASTER_RESPONSE> {

        TEvGetMasterResponse() = default;
    };

    struct TEvConfigureFailureInjectorRequest
            : NActors::TEventPB<TEvConfigureFailureInjectorRequest, NYql::NDqProto::TEvConfigureFailureInjectorRequest, TDqResManEvents::ES_CONFIGURE_FAILURE_INJECTOR> {

        TEvConfigureFailureInjectorRequest() = default;
        TEvConfigureFailureInjectorRequest(const Yql::DqsProto::ConfigureFailureInjectorRequest& request);
    };

    struct TEvConfigureFailureInjectorResponse
            : NActors::TEventPB<TEvConfigureFailureInjectorResponse, NYql::NDqProto::TEvConfigureFailureInjectorRequestResponse, TDqResManEvents::ES_CONFIGURE_FAILURE_INJECTOR_RESPONSE> {

        TEvConfigureFailureInjectorResponse() = default;
    };

    inline NActors::TActorId MakeWorkerManagerActorID(ui32 nodeId) {
        char x[12] = {'r', 'e', 's', 'm', 'a', 'n'};
        memcpy(x + 7, &nodeId, sizeof(ui32));
        return NActors::TActorId(nodeId, TStringBuf(x, 12));
    }
}
