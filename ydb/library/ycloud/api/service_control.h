#pragma once

#include "events.h"

#include <ydb/core/base/defs.h>
#include <ydb/core/base/events.h>
#include <ydb/public/api/client/yc_private/operation/operation.pb.h>
#include <ydb/public/api/client/yc_private/servicecontrol/service_control_service.grpc.pb.h>

namespace NCloud {

struct TEvServiceControl {
    enum EEv {
        EvEnsureEnabledRequest = EventSpaceBegin(NKikimr::TKikimrEvents::ES_SERVICE_CONTROL),
        EvSetupDelegationRequest,
        EvRevokeDelegationRequest,

        EvEnsureEnabledResponse = EventSpaceBegin(NKikimr::TKikimrEvents::ES_SERVICE_CONTROL) + 512,
        EvSetupDelegationResponse,
        EvRevokeDelegationResponse,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NKikimr::TKikimrEvents::ES_SERVICE_CONTROL));

    using TEnsureEnabledRequest = yandex::cloud::priv::servicecontrol::v1::EnsureEnabledRequest;
    using TSetupDelegationRequest = yandex::cloud::priv::servicecontrol::v1::SetupDelegationRequest;
    using TRevokeDelegationRequest = yandex::cloud::priv::servicecontrol::v1::RevokeDelegationRequest;
    using TOperation = ydb::yc::priv::operation::Operation;

    struct TEvEnsureEnabledRequest
        : TEvGrpcProtoRequest<TEvEnsureEnabledRequest, EvEnsureEnabledRequest, TEnsureEnabledRequest> {};
    struct TEvSetupDelegationRequest
        : TEvGrpcProtoRequest<TEvSetupDelegationRequest, EvSetupDelegationRequest, TSetupDelegationRequest> {};
    struct TEvRevokeDelegationRequest
        : TEvGrpcProtoRequest<TEvRevokeDelegationRequest, EvRevokeDelegationRequest, TRevokeDelegationRequest> {};

    struct TEvEnsureEnabledResponse
        : TEvGrpcProtoResponse<TEvEnsureEnabledResponse, EvEnsureEnabledResponse, TOperation> {};
    struct TEvSetupDelegationResponse
        : TEvGrpcProtoResponse<TEvSetupDelegationResponse, EvSetupDelegationResponse, TOperation> {};
    struct TEvRevokeDelegationResponse
        : TEvGrpcProtoResponse<TEvRevokeDelegationResponse, EvRevokeDelegationResponse, TOperation> {};
};

} // namespace NCloud
