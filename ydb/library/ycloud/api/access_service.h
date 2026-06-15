#pragma once
#include <ydb/core/base/defs.h>
#include <ydb/core/base/events.h>
#include <ydb/public/api/client/yc_private/servicecontrol/access_service.grpc.pb.h>
#include <ydb/public/api/client/yc_private/accessservice/access_service.grpc.pb.h>
#include "events.h"

namespace NCloud {
    using namespace NKikimr;

    struct TEvAccessService {
        enum EEv {
            // requests
            // V1
            EvAuthenticateRequest = EventSpaceBegin(TKikimrEvents::ES_ACCESS_SERVICE),
            EvAuthorizeRequest,
            // V2
            EvBulkAuthorizeRequest,
            EvAuthenticateRequestV2,
            EvAuthorizeRequestV2,

            // replies
            // V1
            EvAuthenticateResponse = EventSpaceBegin(TKikimrEvents::ES_ACCESS_SERVICE) + 512,
            EvAuthorizeResponse,
            // V2
            EvBulkAuthorizeResponse,
            EvAuthenticateResponseV2,
            EvAuthorizeResponseV2,

            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_ACCESS_SERVICE), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_ACCESS_SERVICE)");

        // https://a.yandex-team.ru/arc/trunk/arcadia/cloud/servicecontrol/proto/servicecontrol/v1/access_service.proto

        struct TEvAuthenticateRequest : TEvGrpcProtoRequest<TEvAuthenticateRequest, EvAuthenticateRequest, yandex::cloud::priv::servicecontrol::v1::AuthenticateRequest> {};
        struct TEvAuthenticateResponse : TEvGrpcProtoResponse<TEvAuthenticateResponse, EvAuthenticateResponse, yandex::cloud::priv::servicecontrol::v1::AuthenticateResponse> {};

        struct TEvAuthorizeRequest : TEvGrpcProtoRequest<TEvAuthorizeRequest, EvAuthorizeRequest, yandex::cloud::priv::servicecontrol::v1::AuthorizeRequest> {};
        struct TEvAuthorizeResponse : TEvGrpcProtoResponse<TEvAuthorizeResponse, EvAuthorizeResponse, yandex::cloud::priv::servicecontrol::v1::AuthorizeResponse> {};

        struct TEvBulkAuthorizeRequest : TEvGrpcProtoRequest<TEvBulkAuthorizeRequest, EvBulkAuthorizeRequest, yandex::cloud::priv::accessservice::v2::BulkAuthorizeRequest> {};
        struct TEvBulkAuthorizeResponse : TEvGrpcProtoResponse<TEvBulkAuthorizeResponse, EvBulkAuthorizeResponse, yandex::cloud::priv::accessservice::v2::BulkAuthorizeResponse> {};

        // https://nda.ya.ru/t/4kNpAMgS7XkAoC

        // V2
        struct TEvAuthenticateRequestV2 : TEvGrpcProtoRequest<TEvAuthenticateRequestV2, EvAuthenticateRequestV2, yandex::cloud::priv::accessservice::v2::AuthenticateRequest> {};
        struct TEvAuthenticateResponseV2 : TEvGrpcProtoResponse<TEvAuthenticateResponseV2, EvAuthenticateResponseV2, yandex::cloud::priv::accessservice::v2::AuthenticateResponse> {};

        struct TEvAuthorizeRequestV2 : TEvGrpcProtoRequest<TEvAuthorizeRequestV2, EvAuthorizeRequestV2, yandex::cloud::priv::accessservice::v2::AuthorizeRequest> {};
        struct TEvAuthorizeResponseV2 : TEvGrpcProtoResponse<TEvAuthorizeResponseV2, EvAuthorizeResponseV2, yandex::cloud::priv::accessservice::v2::AuthorizeResponse> {};
    };
}
