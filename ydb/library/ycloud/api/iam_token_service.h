#pragma once
#include <ydb/core/base/defs.h>
#include <ydb/core/base/events.h>
#include <ydb/public/api/client/yc_private/iam/iam_token_service.grpc.pb.h>
#include "events.h"

namespace NCloud {
    using namespace NKikimr;

    class TIamTokenService;

    struct TEvIamTokenService {
        enum EEv {
            // requests
            EvCreateForServiceAccountRequest = EventSpaceBegin(TKikimrEvents::ES_IAM_TOKEN_SERVICE),
            EvCreateForServiceRequest,

            // replies
            EvCreateResponse = EventSpaceBegin(TKikimrEvents::ES_IAM_TOKEN_SERVICE) + 1024,
            EvCreateForServiceResponse,

            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_IAM_TOKEN_SERVICE), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_SERVICE_ACCOUNT_SERVICE)");

        struct TEvCreateForServiceAccountRequest : TEvGrpcProtoRequest<TEvCreateForServiceAccountRequest, EvCreateForServiceAccountRequest, yandex::cloud::priv::iam::v1::CreateIamTokenForServiceAccountRequest> {};
        // Issues a token for target_service_account_id on behalf of the service identified by the system
        // service account token in Token (Yandex Cloud "system service accounts" delegation scheme).
        struct TEvCreateForServiceRequest : TEvGrpcProtoRequest<TEvCreateForServiceRequest, EvCreateForServiceRequest, yandex::cloud::priv::iam::v1::CreateIamTokenForServiceRequest> {};
        struct TEvCreateResponse : TEvGrpcProtoResponse<TEvCreateResponse, EvCreateResponse, yandex::cloud::priv::iam::v1::CreateIamTokenResponse> {};
        struct TEvCreateForServiceResponse : TEvGrpcProtoResponse<TEvCreateForServiceResponse, EvCreateForServiceResponse, yandex::cloud::priv::iam::v1::CreateIamTokenResponse> {};
    };
}
