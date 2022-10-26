#pragma once
#include <ydb/core/base/defs.h>
#include <ydb/core/base/events.h>
#include <ydb/public/api/client/yc_private/servicecontrol/access_service.grpc.pb.h>
#include "events.h"

namespace NCloud {
    using namespace NKikimr;

    struct TEvAccessService {
        enum EEv {
            // requests
            EvAuthenticateRequest = EventSpaceBegin(TKikimrEvents::ES_ACCESS_SERVICE),
            EvAuthorizeRequest,

            // replies
            EvAuthenticateResponse = EventSpaceBegin(TKikimrEvents::ES_ACCESS_SERVICE) + 512,
            EvAuthorizeResponse,

            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_ACCESS_SERVICE), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_ACCESS_SERVICE)");

        // https://a.yandex-team.ru/arc/trunk/arcadia/cloud/servicecontrol/proto/servicecontrol/v1/access_service.proto

        struct TEvAuthenticateRequest : TEvGrpcProtoRequest<TEvAuthenticateRequest, EvAuthenticateRequest, yandex::cloud::priv::servicecontrol::v1::AuthenticateRequest> {};
        struct TEvAuthenticateResponse : TEvGrpcProtoResponse<TEvAuthenticateResponse, EvAuthenticateResponse, yandex::cloud::priv::servicecontrol::v1::AuthenticateResponse> {};

        struct TEvAuthorizeRequest : TEvGrpcProtoRequest<TEvAuthorizeRequest, EvAuthorizeRequest, yandex::cloud::priv::servicecontrol::v1::AuthorizeRequest> {};
        struct TEvAuthorizeResponse : TEvGrpcProtoResponse<TEvAuthorizeResponse, EvAuthorizeResponse, yandex::cloud::priv::servicecontrol::v1::AuthorizeResponse> {};
   };
}
