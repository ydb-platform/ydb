#pragma once
#include "events.h"
#include <ydb/core/base/defs.h>
#include <ydb/core/base/events.h>
#include <ydb/public/api/client/nc_private/iam/v1/access_service.grpc.pb.h>

namespace NNebiusCloud {
    using namespace NKikimr;

    struct TEvAccessService {
        enum EEv {
            // requests
            EvAuthenticateRequest = EventSpaceBegin(TKikimrEvents::ES_NEBIUS_ACCESS_SERVICE),
            EvAuthorizeRequest,

            // replies
            EvAuthenticateResponse = EventSpaceBegin(TKikimrEvents::ES_NEBIUS_ACCESS_SERVICE) + 512,
            EvAuthorizeResponse,

            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_NEBIUS_ACCESS_SERVICE), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_NEBIUS_ACCESS_SERVICE)");

        // https://github.com/ydb-platform/ydb/tree/main/ydb/public/api/client/nc_private/iam/v1/access_service.proto

        struct TEvAuthenticateRequest : TEvGrpcProtoRequest<TEvAuthenticateRequest, EvAuthenticateRequest, nebius::iam::v1::AuthenticateRequest> {};
        struct TEvAuthenticateResponse : TEvGrpcProtoResponse<TEvAuthenticateResponse, EvAuthenticateResponse, nebius::iam::v1::AuthenticateResponse> {};

        struct TEvAuthorizeRequest : TEvGrpcProtoRequest<TEvAuthorizeRequest, EvAuthorizeRequest, nebius::iam::v1::AuthorizeRequest> {};
        struct TEvAuthorizeResponse : TEvGrpcProtoResponse<TEvAuthorizeResponse, EvAuthorizeResponse, nebius::iam::v1::AuthorizeResponse> {};
   };
}
