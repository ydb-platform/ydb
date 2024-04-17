#pragma once

#include <ydb/public/api/client/yc_private/oauth/session_service.grpc.pb.h>
#include <ydb/mvp/core/protos/mvp.pb.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/library/grpc/client/grpc_client_low.h>
#include <library/cpp/string_utils/base64/base64.h>

struct TOpenIdConnectSettings {
    static const inline TString CLIENT_ID = "yc.oauth.ydb-viewer";
    static const inline TString YDB_OIDC_COOKIE = "ydb_oidc_cookie";
    TString SessionServiceEndpoint;
    TString SessionServiceTokenName;
    TString AuthorizationServerAddress;
    TString ClientSecret;
    std::vector<TString> AllowedProxyHosts;

    TString GetAuthorizationString() const {
        return Base64Encode(CLIENT_ID + ":" + ClientSecret);
    }
};

TString HmacSHA256(TStringBuf key, TStringBuf data);
void SetHeader(NYdbGrpc::TCallMeta& meta, const TString& name, const TString& value);
TString GenerateCookie(TStringBuf state, TStringBuf redirectUrl, const TString& secret, bool isAjaxRequest);
NHttp::THttpOutgoingResponsePtr GetHttpOutgoingResponsePtr(TStringBuf eventDetails, const NHttp::THttpIncomingRequestPtr& request, const TOpenIdConnectSettings& settings, NHttp::THeadersBuilder& responseHeaders, bool isAjaxRequest = false);
NHttp::THttpOutgoingResponsePtr GetHttpOutgoingResponsePtr(TStringBuf eventDetails, const NHttp::THttpIncomingRequestPtr& request, const TOpenIdConnectSettings& settings, bool isAjaxRequest = false);
bool DetectAjaxRequest(const NHttp::THeaders& headers);
TString CreateNameYdbOidcCookie(TStringBuf key, TStringBuf state);
const TString& GetAuthCallbackUrl();

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
    return client.CreateGRpcServiceConnection<TSessionService>(config);
}

struct TEvPrivate {
    enum EEv {
        EvCheckSessionResponse = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvCreateSessionResponse,
        EvErrorResponse,
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
};
