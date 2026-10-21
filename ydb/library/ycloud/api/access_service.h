#pragma once
#include <ydb/core/base/defs.h>
#include <ydb/core/base/events.h>
#include <ydb/public/api/client/yc_private/accessservice/access_service.grpc.pb.h>
#include "events.h"

namespace NCloud {
    using namespace NKikimr;

    struct TEvAccessService {
        enum EEv {
            // requests
            // The first two slots are reserved for removed V1 events.
            EvBulkAuthorizeRequest = EventSpaceBegin(TKikimrEvents::ES_ACCESS_SERVICE) + 2,
            EvAuthenticateRequest,
            EvAuthorizeRequest,

            // replies
            // The first two slots are reserved for removed V1 events.
            EvBulkAuthorizeResponse = EventSpaceBegin(TKikimrEvents::ES_ACCESS_SERVICE) + 512 + 2,
            EvAuthenticateResponse,
            EvAuthorizeResponse,

            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_ACCESS_SERVICE), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_ACCESS_SERVICE)");

        struct TEvAuthenticateRequest : TEvGrpcProtoRequest<TEvAuthenticateRequest, EvAuthenticateRequest, yandex::cloud::priv::accessservice::v2::AuthenticateRequest> {};
        struct TEvAuthenticateResponse : TEvGrpcProtoResponse<TEvAuthenticateResponse, EvAuthenticateResponse, yandex::cloud::priv::accessservice::v2::AuthenticateResponse> {};

        struct TEvAuthorizeRequest : TEvGrpcProtoRequest<TEvAuthorizeRequest, EvAuthorizeRequest, yandex::cloud::priv::accessservice::v2::AuthorizeRequest> {};
        struct TEvAuthorizeResponse : TEvGrpcProtoResponse<TEvAuthorizeResponse, EvAuthorizeResponse, yandex::cloud::priv::accessservice::v2::AuthorizeResponse> {};

        struct TEvBulkAuthorizeRequest : TEvGrpcProtoRequest<TEvBulkAuthorizeRequest, EvBulkAuthorizeRequest, yandex::cloud::priv::accessservice::v2::BulkAuthorizeRequest> {};
        struct TEvBulkAuthorizeResponse : TEvGrpcProtoResponse<TEvBulkAuthorizeResponse, EvBulkAuthorizeResponse, yandex::cloud::priv::accessservice::v2::BulkAuthorizeResponse> {};

    };
}
