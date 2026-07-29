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
            EvBulkAuthorizeRequestV2,
            EvAuthenticateRequestV2,
            EvAuthorizeRequestV2,

            // replies
            // V1
            EvAuthenticateResponse = EventSpaceBegin(TKikimrEvents::ES_ACCESS_SERVICE) + 512,
            EvAuthorizeResponse,
            // V2
            EvBulkAuthorizeResponseV2,
            EvAuthenticateResponseV2,
            EvAuthorizeResponseV2,

            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_ACCESS_SERVICE), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_ACCESS_SERVICE)");

        struct IAuthenticateResponse {
            virtual ~IAuthenticateResponse() = default;

            virtual const NYdbGrpc::TGrpcStatus& GetStatus() const = 0;
            virtual bool HasSubject() const = 0;
            virtual TString GetSubjectType() const = 0;
        };

        template <typename TSubject>
        static TString GetSubjectType(const TSubject& subject) {
            switch (subject.type_case()) {
                case TSubject::TYPE_NOT_SET:
                case TSubject::kAnonymousAccount:
                    return "unknown";
                case TSubject::kUserAccount:
                    return subject.user_account().federation_id() ? "federated_account" : "user_account";
                case TSubject::kServiceAccount:
                    return "service_account";
                default:
                    return "unknown";
            }
        }

        template <typename TEv, ui32 TEventType, typename TProtoMessage>
        struct TEvAuthenticateResponseBase
            : TEvGrpcProtoResponse<TEv, TEventType, TProtoMessage>
            , IAuthenticateResponse
        {
            const NYdbGrpc::TGrpcStatus& GetStatus() const override {
                return this->Status;
            }

            bool HasSubject() const override {
                return this->Response.has_subject();
            }

            TString GetSubjectType() const override {
                return TEvAccessService::GetSubjectType(this->Response.subject());
            }
        };

        // V1
        struct TEvAuthenticateRequest : TEvGrpcProtoRequest<TEvAuthenticateRequest, EvAuthenticateRequest, yandex::cloud::priv::servicecontrol::v1::AuthenticateRequest> {};
        struct TEvAuthenticateResponse : TEvAuthenticateResponseBase<TEvAuthenticateResponse, EvAuthenticateResponse, yandex::cloud::priv::servicecontrol::v1::AuthenticateResponse> {};

        struct TEvAuthorizeRequest : TEvGrpcProtoRequest<TEvAuthorizeRequest, EvAuthorizeRequest, yandex::cloud::priv::servicecontrol::v1::AuthorizeRequest> {};
        struct TEvAuthorizeResponse : TEvGrpcProtoResponse<TEvAuthorizeResponse, EvAuthorizeResponse, yandex::cloud::priv::servicecontrol::v1::AuthorizeResponse> {};

        // V2
        struct TEvAuthenticateRequestV2 : TEvGrpcProtoRequest<TEvAuthenticateRequestV2, EvAuthenticateRequestV2, yandex::cloud::priv::accessservice::v2::AuthenticateRequest> {};
        struct TEvAuthenticateResponseV2 : TEvAuthenticateResponseBase<TEvAuthenticateResponseV2, EvAuthenticateResponseV2, yandex::cloud::priv::accessservice::v2::AuthenticateResponse> {};

        struct TEvAuthorizeRequestV2 : TEvGrpcProtoRequest<TEvAuthorizeRequestV2, EvAuthorizeRequestV2, yandex::cloud::priv::accessservice::v2::AuthorizeRequest> {};
        struct TEvAuthorizeResponseV2 : TEvGrpcProtoResponse<TEvAuthorizeResponseV2, EvAuthorizeResponseV2, yandex::cloud::priv::accessservice::v2::AuthorizeResponse> {};

        struct TEvBulkAuthorizeRequestV2 : TEvGrpcProtoRequest<TEvBulkAuthorizeRequestV2, EvBulkAuthorizeRequestV2, yandex::cloud::priv::accessservice::v2::BulkAuthorizeRequest> {};
        struct TEvBulkAuthorizeResponseV2 : TEvGrpcProtoResponse<TEvBulkAuthorizeResponseV2, EvBulkAuthorizeResponseV2, yandex::cloud::priv::accessservice::v2::BulkAuthorizeResponse> {};

    };
}
