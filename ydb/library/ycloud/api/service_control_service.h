#pragma once
#include <ydb/core/base/defs.h>
#include <ydb/core/base/events.h>
#include <ydb/public/api/client/yc_private/iam/service_control_service.grpc.pb.h>
#include <ydb/public/api/client/yc_private/operation/operation.pb.h>
#include "events.h"

namespace NCloud {
    using namespace NKikimr;

    struct TEvServiceControlService {
        enum EEv {
            // requests
            EvSetupDelegationRequest = EventSpaceBegin(TKikimrEvents::ES_SERVICE_CONTROL_SERVICE),
            EvRevokeDelegationRequest,

            // replies
            EvSetupDelegationResponse = EventSpaceBegin(TKikimrEvents::ES_SERVICE_CONTROL_SERVICE) + 1024,
            EvRevokeDelegationResponse,

            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_SERVICE_CONTROL_SERVICE), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_SERVICE_CONTROL_SERVICE)");

        struct TEvSetupDelegationRequest : TEvGrpcProtoRequest<TEvSetupDelegationRequest, EvSetupDelegationRequest, yandex::cloud::priv::iam::v1::SetupDelegationRequest> {};
        struct TEvSetupDelegationResponse : TEvGrpcProtoResponse<TEvSetupDelegationResponse, EvSetupDelegationResponse, ydb::yc::priv::operation::Operation> {};
        struct TEvRevokeDelegationRequest : TEvGrpcProtoRequest<TEvRevokeDelegationRequest, EvRevokeDelegationRequest, yandex::cloud::priv::iam::v1::RevokeDelegationRequest> {};
        struct TEvRevokeDelegationResponse : TEvGrpcProtoResponse<TEvRevokeDelegationResponse, EvRevokeDelegationResponse, ydb::yc::priv::operation::Operation> {};
    };
}
