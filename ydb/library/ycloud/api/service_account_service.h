#pragma once
#include <ydb/core/base/defs.h>
#include <ydb/core/base/events.h>
#include <ydb/public/api/client/yc_private/iam/service_account_service.grpc.pb.h>
#include "events.h"

namespace NCloud {
    using namespace NKikimr;

    struct TEvServiceAccountService {
        enum EEv {
            // requests
            EvGetServiceAccountRequest = EventSpaceBegin(TKikimrEvents::ES_SERVICE_ACCOUNT_SERVICE),
            EvIssueTokenRequest,

            // replies
            EvGetServiceAccountResponse = EventSpaceBegin(TKikimrEvents::ES_SERVICE_ACCOUNT_SERVICE) + 1024,
            EvIssueTokenResponse,

            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_SERVICE_ACCOUNT_SERVICE), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_SERVICE_ACCOUNT_SERVICE)");

        struct TEvGetServiceAccountRequest : TEvGrpcProtoRequest<TEvGetServiceAccountRequest, EvGetServiceAccountRequest, yandex::cloud::priv::iam::v1::GetServiceAccountRequest> {};
        struct TEvGetServiceAccountResponse : TEvGrpcProtoResponse<TEvGetServiceAccountResponse, EvGetServiceAccountResponse, yandex::cloud::priv::iam::v1::ServiceAccount> {};

        struct TEvIssueTokenRequest : TEvGrpcProtoRequest<TEvIssueTokenRequest, EvIssueTokenRequest, yandex::cloud::priv::iam::v1::IssueTokenRequest> {};
        struct TEvIssueTokenResponse : TEvGrpcProtoResponse<TEvIssueTokenResponse, EvIssueTokenResponse, yandex::cloud::priv::iam::v1::IamToken> {};
    };
}
