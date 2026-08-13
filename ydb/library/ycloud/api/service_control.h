#pragma once

#include "events.h"

#include <ydb/core/base/defs.h>
#include <ydb/core/base/events.h>
#include <ydb/public/api/client/yc_private/operation/operation.pb.h>
#include <ydb/public/api/client/yc_private/iam/operation_service.grpc.pb.h>
#include <ydb/public/api/client/yc_private/iam/service_control_service.grpc.pb.h>

namespace NCloud {

struct TEvServiceControl {
    enum EEv {
        EvEnsureEnabledRequest = EventSpaceBegin(NKikimr::TKikimrEvents::ES_SERVICE_CONTROL),
        EvSetupDelegationRequest,
        EvRevokeDelegationRequest,
        EvGetOperationRequest,

        EvEnsureEnabledResponse = EventSpaceBegin(NKikimr::TKikimrEvents::ES_SERVICE_CONTROL) + 512,
        EvSetupDelegationResponse,
        EvRevokeDelegationResponse,
        EvGetOperationResponse,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NKikimr::TKikimrEvents::ES_SERVICE_CONTROL));

    using TEnsureEnabledRequest = yandex::cloud::priv::iam::v1::EnsureServicesEnabledRequest;
    using TSetupDelegationRequest = yandex::cloud::priv::iam::v1::SetupDelegationRequest;
    using TRevokeDelegationRequest = yandex::cloud::priv::iam::v1::RevokeDelegationRequest;
    using TGetOperationRequest = yandex::cloud::priv::iam::v1::GetOperationRequest;
    using TOperation = ydb::yc::priv::operation::Operation;

    struct TEvEnsureEnabledRequest
        : TEvGrpcProtoRequest<TEvEnsureEnabledRequest, EvEnsureEnabledRequest, TEnsureEnabledRequest> {};
    struct TEvSetupDelegationRequest
        : TEvGrpcProtoRequest<TEvSetupDelegationRequest, EvSetupDelegationRequest, TSetupDelegationRequest> {};
    struct TEvRevokeDelegationRequest
        : TEvGrpcProtoRequest<TEvRevokeDelegationRequest, EvRevokeDelegationRequest, TRevokeDelegationRequest> {};
    struct TEvGetOperationRequest
        : TEvGrpcProtoRequest<TEvGetOperationRequest, EvGetOperationRequest, TGetOperationRequest> {};

    struct TEvEnsureEnabledResponse
        : TEvGrpcProtoResponse<TEvEnsureEnabledResponse, EvEnsureEnabledResponse, TOperation> {};
    struct TEvSetupDelegationResponse
        : TEvGrpcProtoResponse<TEvSetupDelegationResponse, EvSetupDelegationResponse, TOperation> {};
    struct TEvRevokeDelegationResponse
        : TEvGrpcProtoResponse<TEvRevokeDelegationResponse, EvRevokeDelegationResponse, TOperation> {};
    struct TEvGetOperationResponse
        : TEvGrpcProtoResponse<TEvGetOperationResponse, EvGetOperationResponse, TOperation> {};
};

} // namespace NCloud
