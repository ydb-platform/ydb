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
            EvAuthenticateRequestV1 = EventSpaceBegin(TKikimrEvents::ES_ACCESS_SERVICE),
            EvAuthorizeRequestV1,
            // V2
            EvBulkAuthorizeRequestV2,
            EvAuthenticateRequestV2,
            EvAuthorizeRequestV2,

            // replies
            // V1
            EvAuthenticateResponseV1 = EventSpaceBegin(TKikimrEvents::ES_ACCESS_SERVICE) + 512,
            EvAuthorizeResponseV1,
            // V2
            EvBulkAuthorizeResponseV2,
            EvAuthenticateResponseV2,
            EvAuthorizeResponseV2,

            EvEnd
        };

        static_assert(
            EvEnd < EventSpaceEnd(TKikimrEvents::ES_ACCESS_SERVICE),
            "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_ACCESS_SERVICE)");

        // https://nda.ya.ru/t/FYSHHBXq7XkAyW

        // V1
        struct TEvAuthenticateRequestV1
            : TEvGrpcProtoRequest<
                  TEvAuthenticateRequestV1,
                  EvAuthenticateRequestV1,
                  yandex::cloud::priv::servicecontrol::v1::AuthenticateRequest> {};
        struct TEvAuthenticateResponseV1
            : TEvGrpcProtoResponse<
                  TEvAuthenticateResponseV1,
                  EvAuthenticateResponseV1,
                  yandex::cloud::priv::servicecontrol::v1::AuthenticateResponse> {};

        struct TEvAuthorizeRequestV1
            : TEvGrpcProtoRequest<
                  TEvAuthorizeRequestV1,
                  EvAuthorizeRequestV1,
                  yandex::cloud::priv::servicecontrol::v1::AuthorizeRequest> {};
        struct TEvAuthorizeResponseV1
            : TEvGrpcProtoResponse<
                  TEvAuthorizeResponseV1,
                  EvAuthorizeResponseV1,
                  yandex::cloud::priv::servicecontrol::v1::AuthorizeResponse> {};

        // https://nda.ya.ru/t/4kNpAMgS7XkAoC

        // V2
        struct TEvBulkAuthorizeRequestV2
            : TEvGrpcProtoRequest<
                  TEvBulkAuthorizeRequestV2,
                  EvBulkAuthorizeRequestV2,
                  yandex::cloud::priv::accessservice::v2::BulkAuthorizeRequest> {};
        struct TEvBulkAuthorizeResponseV2
            : TEvGrpcProtoResponse<
                  TEvBulkAuthorizeResponseV2,
                  EvBulkAuthorizeResponseV2,
                  yandex::cloud::priv::accessservice::v2::BulkAuthorizeResponse> {};

        struct TEvAuthenticateRequestV2
            : TEvGrpcProtoRequest<
                  TEvAuthenticateRequestV2,
                  EvAuthenticateRequestV2,
                  yandex::cloud::priv::accessservice::v2::AuthenticateRequest> {};
        struct TEvAuthenticateResponseV2
            : TEvGrpcProtoResponse<
                  TEvAuthenticateResponseV2,
                  EvAuthenticateResponseV2,
                  yandex::cloud::priv::accessservice::v2::AuthenticateResponse> {};

        struct TEvAuthorizeRequestV2
            : TEvGrpcProtoRequest<
                  TEvAuthorizeRequestV2,
                  EvAuthorizeRequestV2,
                  yandex::cloud::priv::accessservice::v2::AuthorizeRequest> {};
        struct TEvAuthorizeResponseV2
            : TEvGrpcProtoResponse<
                  TEvAuthorizeResponseV2,
                  EvAuthorizeResponseV2,
                  yandex::cloud::priv::accessservice::v2::AuthorizeResponse> {};
    };

} // namespace NCloud
