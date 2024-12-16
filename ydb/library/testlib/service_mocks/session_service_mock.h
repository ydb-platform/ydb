#pragma once

#include <ydb/public/api/client/yc_private/oauth/session_service.grpc.pb.h>
#include <ydb/public/api/client/yc_private/iam/iam_token_service.grpc.pb.h>
#include <util/generic/hash_set.h>
#include <util/generic/hash.h>

class TSessionServiceMock : public yandex::cloud::priv::oauth::v1::SessionService::Service {
    yandex::cloud::priv::oauth::v1::AuthorizationRequired AuthorizationRequiredMessage;

    THashMap<TString, TString> ParseCookie(TStringBuf cookie) {
        THashMap<TString, TString> parsedCookies;
        for (TStringBuf param = cookie.NextTok(';'); !param.empty(); param = cookie.NextTok(';')) {
            param.SkipPrefix(" ");
            TStringBuf name = param.NextTok('=');
            parsedCookies[name] = param;
        }
        return parsedCookies;
    }

public:
    std::pair<const TString, TString> AllowedCookies {"yc_session", "allowed_session_cookie"};
    bool IsTokenAllowed {true};
    bool IsOpenIdScopeMissed {false};
    THashSet<TString> AllowedAccessTokens;

    TSessionServiceMock() {
        AuthorizationRequiredMessage.Setauthorize_url("https://auth.test.net/oauth/authorize");
    }

    grpc::Status Check(grpc::ServerContext*,
                       const yandex::cloud::priv::oauth::v1::CheckSessionRequest* request,
                       yandex::cloud::priv::oauth::v1::CheckSessionResponse* response) override {
        if (!IsTokenAllowed) {
            return grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "Authorization IAM token are invalid or may have expired");
        }
        const THashMap<TString, TString> cookies = ParseCookie(request->Getcookie_header());
        auto it = cookies.find(AllowedCookies.first);
        if (it != cookies.cend()) {
            if (it->second == AllowedCookies.second) {
                auto iam_token = response->Mutableiam_token();
                iam_token->Setiam_token("protected_page_iam_token");
                return grpc::Status(grpc::StatusCode::OK, "Cookie is corrected");
            }
        }
        const TString errorDetailsPrefix = "Error details perfix\n";
        const TString errorDetailsSuffix = "\nError details suffix";
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                            "The provided cookies are invalid or may have expired",
                            errorDetailsPrefix + AuthorizationRequiredMessage.SerializeAsString() + errorDetailsSuffix);
    }

    grpc::Status Create(grpc::ServerContext*,
                        const yandex::cloud::priv::oauth::v1::CreateSessionRequest* request,
                        yandex::cloud::priv::oauth::v1::CreateSessionResponse* response) override {
        if (!IsTokenAllowed) {
            return grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "Authorization IAM token are invalid or may have expired");
        }
        if (IsOpenIdScopeMissed) {
            return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, "Openid scope is missed for specified access_token");
        }
        if (AllowedAccessTokens.count(request->Getaccess_token()) > 0) {
            response->Addset_cookie_header(AllowedCookies.first + "=" + AllowedCookies.second + "; SameSite=Lax");
            return grpc::Status(grpc::StatusCode::OK, "Cookie was created");
        }
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "The provided access_token is invalid or may have expired", AuthorizationRequiredMessage.SerializeAsString());
    }
};
