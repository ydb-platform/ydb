#pragma once
#include "cracked_page.h"
#include "extension_context.h"
#include "context.h"
#include "oidc_settings.h"
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/public/sdk/cpp/src/library/grpc/client/grpc_client_low.h>
#include <ydb/mvp/core/core_ydb.h>
#include <ydb/public/api/client/yc_private/oauth/session_service.grpc.pb.h>
#include <ydb/public/api/client/nc_private/iam/v1/profile_service.grpc.pb.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>

namespace NMVP::NOIDC {

struct TOpenIdConnectSettings;

constexpr TStringBuf IAM_TOKEN_SCHEME = "Bearer ";
constexpr TStringBuf IAM_TOKEN_SCHEME_LOWER = "bearer ";
constexpr TStringBuf AUTHORIZATION_HEADER = "Authorization";
constexpr TStringBuf X_FORWARDED_FOR_HEADER = "X-Forwarded-For";
constexpr TStringBuf LOCATION_HEADER = "Location";

constexpr TStringBuf USER_SID = "UserSID";
constexpr TStringBuf ORIGINAL_USER_TOKEN = "OriginalUserToken";
constexpr TStringBuf EXTENDED_INFO = "ExtendedInfo";
constexpr TStringBuf EXTENDED_ERRORS = "ExtendedErrors";

struct TRestoreOidcContextResult {
    struct TStatus {
        bool IsSuccess = true;
        bool IsErrorRetryable = false;
        TString ErrorMessage;
    };

    TContext Context;
    TStatus Status;

    TRestoreOidcContextResult(const TStatus& status = {.IsSuccess = true, .IsErrorRetryable = false, .ErrorMessage = ""}, const TContext& context = TContext());

    bool IsSuccess() const;
};

struct TCheckStateResult {
    bool Success = true;
    TString ErrorMessage;

    TCheckStateResult(bool success = true, const TString& errorMessage = "");

    bool IsSuccess() const;
};

TString HmacSHA256(TStringBuf key, TStringBuf data);
TString HmacSHA1(TStringBuf key, TStringBuf data);
void SetHeader(NYdbGrpc::TCallMeta& meta, const TString& name, const TString& value);
NHttp::THttpOutgoingResponsePtr GetHttpOutgoingResponsePtr(const NHttp::THttpIncomingRequestPtr& request, const TOpenIdConnectSettings& settings);
TString CreateNameYdbOidcCookie(TStringBuf key, TStringBuf state);
TString CreateNameSessionCookie(TStringBuf key);
TString CreateNameImpersonatedCookie(TStringBuf key);
const TString& GetAuthCallbackUrl();
TString CreateSecureCookie(const TString& name, const TString& value, const ui32 expiredSeconds);
TString ClearSecureCookie(const TString& name);
void SetCORS(const NHttp::THttpIncomingRequestPtr& request, NHttp::THeadersBuilder* const headers);
TRestoreOidcContextResult RestoreOidcContext(const NHttp::TCookies& cookies, const TString& key);
TCheckStateResult CheckState(const TString& state, const TString& key);
TString DecodeToken(const TStringBuf& cookie);
TStringBuf GetCookie(const NHttp::TCookies& cookies, const TString& cookieName);
TString GetAddressWithoutPort(const TString& address);


struct TProxiedRequestParams {
    const NHttp::THttpIncomingRequestPtr Request;
    TStringBuf AuthHeader;
    bool Secure = false;
    TCrackedPage ProtectedPage;
    const TOpenIdConnectSettings Settings;
};

NHttp::THttpOutgoingRequestPtr CreateProxiedRequest(const TProxiedRequestParams& param);
NHttp::THttpOutgoingResponsePtr CreateResponseForbiddenHost(const NHttp::THttpIncomingRequestPtr request, const TCrackedPage& protectedPage);
NHttp::THttpOutgoingResponsePtr CreateResponseForNotExistingResponseFromProtectedResource(const NHttp::THttpIncomingRequestPtr request, const TString& errorMessage);

template <typename TSessionService>
std::unique_ptr<NYdbGrpc::TServiceConnection<TSessionService>> CreateGRpcServiceConnection(const TString& endpoint) {
    TStringBuf scheme = "grpc";
    TStringBuf host;
    TStringBuf uri;
    NHttp::CrackURL(endpoint, scheme, host, uri);
    NYdbGrpc::TGRpcClientConfig config;
    config.Locator = host;
    config.EnableSsl = (scheme == "grpcs");
    static NYdbGrpc::TGRpcClientLow client;
    SetGrpcKeepAlive(config);
    return client.CreateGRpcServiceConnection<TSessionService>(config);
}

struct TEvPrivate {
    enum EEv {
        EvCheckSessionResponse = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvCreateSessionResponse,
        EvErrorResponse,
        EvGetProfileResponse,
        EvExtensionRequest,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    struct TEvCheckSessionResponse : NActors::TEventLocal<TEvCheckSessionResponse, EvCheckSessionResponse> {
        yandex::cloud::priv::oauth::v1::CheckSessionResponse Response;

        TEvCheckSessionResponse(yandex::cloud::priv::oauth::v1::CheckSessionResponse&& response)
            : Response(response)
        {}
    };

    struct TEvCreateSessionResponse : NActors::TEventLocal<TEvCreateSessionResponse, EvCreateSessionResponse> {
        yandex::cloud::priv::oauth::v1::CreateSessionResponse Response;

        TEvCreateSessionResponse(yandex::cloud::priv::oauth::v1::CreateSessionResponse&& response)
            : Response(response)
        {}
    };

    struct TEvGetProfileResponse : NActors::TEventLocal<TEvGetProfileResponse, EvGetProfileResponse> {
        nebius::iam::v1::GetProfileResponse Response;

        TEvGetProfileResponse(nebius::iam::v1::GetProfileResponse&& response)
            : Response(response)
        {}
    };

    struct TEvErrorResponse : NActors::TEventLocal<TEvErrorResponse, EvErrorResponse> {
        TString Status;
        TString Message;
        TString Details;

        TEvErrorResponse(const TString& error)
            : Status("503")
            , Message(error)
        {}

        TEvErrorResponse(const TString& status, const TString& error)
            : Status(status)
            , Message(error)
        {}

        TEvErrorResponse(const NYdbGrpc::TGrpcStatus& status) {
            switch(status.GRpcStatusCode) {
            case grpc::StatusCode::NOT_FOUND:
                Status = "404";
                break;
            case grpc::StatusCode::INVALID_ARGUMENT:
                Status = "400";
                break;
            case grpc::StatusCode::DEADLINE_EXCEEDED:
                Status = "504";
                break;
            case grpc::StatusCode::RESOURCE_EXHAUSTED:
                Status = "429";
                break;
            case grpc::StatusCode::PERMISSION_DENIED:
                Status = "403";
                break;
            case grpc::StatusCode::UNAUTHENTICATED:
                Status = "401";
                break;
            case grpc::StatusCode::INTERNAL:
                Status = "500";
                break;
            case grpc::StatusCode::FAILED_PRECONDITION:
                Status = "412";
                break;
            case grpc::StatusCode::UNAVAILABLE:
            default:
                Status = "503";
                break;
            }
            Message = status.Msg;
            Details = status.Details;
        }
    };

    struct TEvExtensionRequest : NActors::TEventLocal<TEvExtensionRequest, EvExtensionRequest> {
        TIntrusivePtr<TExtensionContext> Context;

        TEvExtensionRequest(TIntrusivePtr<TExtensionContext>&& context)
            : Context(std::move(context))
        {}
    };
};

} // NMVP::NOIDC
