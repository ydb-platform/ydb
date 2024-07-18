#include <contrib/libs/jwt-cpp/include/jwt-cpp/jwt.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/mvp/core/core_ydb.h>
#include <ydb/public/api/grpc/ydb_auth_v1.grpc.pb.h>
#include "mvp_tokens.h"
#include <ydb/public/api/client/nc_private/iam/token_service.grpc.pb.h>
#include <ydb/public/api/client/nc_private/iam/token_exchange_service.grpc.pb.h>

namespace NMVP {

TMvpTokenator* TMvpTokenator::CreateTokenator(const NMvp::TTokensConfig& tokensConfig,  const NActors::TActorId& httpProxy, const NMVP::EAuthProfile authProfile) {
    return new TMvpTokenator(tokensConfig, httpProxy, authProfile);
}

TString TMvpTokenator::GetToken(const TString& name) {
    TString token;
    {
        auto guard = Guard(TokensLock);
        auto it = Tokens.find(name);
        if (it != Tokens.end()) {
            token = it->second;
        }
    }
    return token;
}

TMvpTokenator::TMvpTokenator(NMvp::TTokensConfig tokensConfig, const NActors::TActorId& httpProxy, const NMVP::EAuthProfile authProfile)
    : HttpProxy(httpProxy)
    , AuthProfile(authProfile)
{
    if (tokensConfig.HasStaffApiUserTokenInfo()) {
        UpdateStaffApiUserToken(&tokensConfig.staffapiusertokeninfo());
    }
    for (const NMvp::TJwtInfo& jwtInfo : tokensConfig.jwtinfo()) {
        TokenConfigs.JwtTokenConfigs[jwtInfo.name()] = jwtInfo;
    }
    for (const NMvp::TOAuthInfo& oauthInfo : tokensConfig.oauthinfo()) {
        TokenConfigs.OauthTokenConfigs[oauthInfo.name()] = oauthInfo;
    }
    for (const NMvp::TMetadataTokenInfo& metadataTokenInfo : tokensConfig.metadatatokeninfo()) {
        TokenConfigs.MetadataTokenConfigs[metadataTokenInfo.name()] = metadataTokenInfo;
    }
    for (const NMvp::TStaticCredentialsInfo& staticCredentialsInfo : tokensConfig.staticcredentialsinfo()) {
        TokenConfigs.StaticCredentialsConfigs[staticCredentialsInfo.name()] = staticCredentialsInfo;
    }
}

void TMvpTokenator::Bootstrap() {
    for (const auto& [name, config] : TokenConfigs.JwtTokenConfigs) {
        Send(SelfId(), new TEvPrivate::TEvRefreshToken(name));
    }
    for (const auto& [name, config] : TokenConfigs.OauthTokenConfigs) {
        Send(SelfId(), new TEvPrivate::TEvRefreshToken(name));
    }
    for (const auto& [name, config] : TokenConfigs.MetadataTokenConfigs) {
        Send(SelfId(), new TEvPrivate::TEvRefreshToken(name));
    }
    for (const auto& [name, config] : TokenConfigs.StaticCredentialsConfigs) {
        Send(SelfId(), new TEvPrivate::TEvRefreshToken(name));
    }
    Become(&TMvpTokenator::StateWork, PERIODIC_CHECK, new NActors::TEvents::TEvWakeup());
}

void TMvpTokenator::HandlePeriodic() {
    TInstant now = TInstant::Now();
    while (!RefreshQueue.empty() && RefreshQueue.top().RefreshTime <= now) {
        TString name = RefreshQueue.top().Name;
        RefreshQueue.pop();
        Send(SelfId(), new TEvPrivate::TEvRefreshToken(name));
    }
    Schedule(PERIODIC_CHECK, new NActors::TEvents::TEvWakeup());
}

void TMvpTokenator::Handle(TEvPrivate::TEvRefreshToken::TPtr event) {
    TString name = event->Get()->Name;
    BLOG_D("Refreshing token " << name);
    const NMvp::TJwtInfo* jwtInfo = TokenConfigs.GetJwtTokenConfig(name);
    if (jwtInfo != nullptr) {
        UpdateJwtToken(jwtInfo);
        return;
    }
    const NMvp::TOAuthInfo* oauthInfo = TokenConfigs.GetOAuthTokenConfig(name);
    if (oauthInfo != nullptr) {
        UpdateOAuthToken(oauthInfo);
        return;
    }
    const NMvp::TMetadataTokenInfo* metadataTokenInfo = TokenConfigs.GetMetadataTokenConfig(name);
    if (metadataTokenInfo != nullptr) {
        UpdateMetadataToken(metadataTokenInfo);
        return;
    }
    const NMvp::TStaticCredentialsInfo* staticCredentialsInfo = TokenConfigs.GetStaticCredentialsTokenConfig(name);
    if (staticCredentialsInfo != nullptr) {
        UpdateStaticCredentialsToken(staticCredentialsInfo);
        return;
    }
    BLOG_ERROR("Token " << name << " not found");
}

void TMvpTokenator::Handle(TEvPrivate::TEvUpdateIamTokenYandex::TPtr event) {
    TDuration refreshPeriod = SUCCESS_REFRESH_PERIOD;
    if (event->Get()->Status.Ok()) {
        BLOG_D("Updating token " << event->Get()->Name << " to " << event->Get()->Response.subject());
        {
            auto guard = Guard(TokensLock);
            Tokens[event->Get()->Name] = "Bearer " + std::move(event->Get()->Response.iam_token());
        }
    } else {
        BLOG_ERROR("Error refreshing token " << event->Get()->Name << ", status: " << event->Get()->Status.GRpcStatusCode << ", error: " << event->Get()->Status.Msg);
        refreshPeriod = ERROR_REFRESH_PERIOD;
    }
    RefreshQueue.push({TInstant::Now() + refreshPeriod, event->Get()->Name});
}

void TMvpTokenator::Handle(TEvPrivate::TEvUpdateIamTokenNebius::TPtr event) {
    TDuration refreshPeriod = SUCCESS_REFRESH_PERIOD;
    if (event->Get()->Status.Ok()) {
        BLOG_D("Updating token " << event->Get()->Name << " to " << event->Get()->Subject);
        {
            auto guard = Guard(TokensLock);
            Tokens[event->Get()->Name] = "Bearer " + std::move(event->Get()->Response.access_token());
        }
    } else {
        BLOG_ERROR("Error refreshing token " << event->Get()->Name << ", status: " << event->Get()->Status.GRpcStatusCode << ", error: " << event->Get()->Status.Msg);
        refreshPeriod = ERROR_REFRESH_PERIOD;
    }
    RefreshQueue.push({TInstant::Now() + refreshPeriod, event->Get()->Name});
}

void TMvpTokenator::Handle(TEvPrivate::TEvUpdateStaticCredentialsToken::TPtr event) {
    TDuration refreshPeriod = SUCCESS_REFRESH_PERIOD;
    if (event->Get()->Status.Ok() && event->Get()->Response.operation().has_result()) {
        BLOG_D("Updating token " << event->Get()->Name);
        {
            auto guard = Guard(TokensLock);
            Ydb::Auth::LoginResult result;
            event->Get()->Response.operation().result().UnpackTo(&result);
            Tokens[event->Get()->Name] = "Login " + std::move(result.token());
        }
    } else {
        TString error;
        if (event->Get()->Response.operation().has_result()) {
            error = ", status: " + std::to_string(event->Get()->Status.GRpcStatusCode) + ", error: " + event->Get()->Status.Msg;
        } else {
            for (const auto& issue : event->Get()->Response.operation().issues()) {
                error += " " + issue.DebugString();
            }
        }
        BLOG_ERROR("Error refreshing token " << event->Get()->Name << error);
        refreshPeriod = ERROR_REFRESH_PERIOD;
    }
    RefreshQueue.push({TInstant::Now() + refreshPeriod, event->Get()->Name});
}

const NMvp::TJwtInfo* TMvpTokenator::TTokenConfigs::GetJwtTokenConfig(const TString& name) {
    return GetTokenConfig(JwtTokenConfigs, name);
}

const NMvp::TOAuthInfo* TMvpTokenator::TTokenConfigs::GetOAuthTokenConfig(const TString& name) {
    return GetTokenConfig(OauthTokenConfigs, name);
}

const NMvp::TMetadataTokenInfo* TMvpTokenator::TTokenConfigs::GetMetadataTokenConfig(const TString& name) {
    return GetTokenConfig(MetadataTokenConfigs, name);
}

const NMvp::TStaticCredentialsInfo* TMvpTokenator::TTokenConfigs::GetStaticCredentialsTokenConfig(const TString& name) {
    return GetTokenConfig(StaticCredentialsConfigs, name);
}

void TMvpTokenator::UpdateStaffApiUserToken(const NMvp::TStaffApiUserTokenInfo* staffApiUserTokenInfo) {
    Tokens[staffApiUserTokenInfo->name()] = "OAuth " + staffApiUserTokenInfo->token();
}

void TMvpTokenator::UpdateStaticCredentialsToken(const NMvp::TStaticCredentialsInfo* staticCredentialsInfo) {
    Ydb::Auth::LoginRequest request;
    request.set_user(staticCredentialsInfo->login());
    request.set_password(staticCredentialsInfo->password());

    RequestCreateToken<Ydb::Auth::V1::AuthService,
                       Ydb::Auth::LoginRequest,
                       Ydb::Auth::LoginResponse,
                       TEvPrivate::TEvUpdateStaticCredentialsToken>(staticCredentialsInfo->name(), staticCredentialsInfo->endpoint(), request, &Ydb::Auth::V1::AuthService::Stub::AsyncLogin);
}

void TMvpTokenator::UpdateMetadataToken(const NMvp::TMetadataTokenInfo* metadataTokenInfo) {
    const TStringBuf& url = metadataTokenInfo->endpoint();
    NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestGet(url);
    httpRequest->Set("Metadata-Flavor", "Google");
    HttpRequestNames.emplace(httpRequest.Get(), metadataTokenInfo->name());
    Send(HttpProxy, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest));
}

void TMvpTokenator::Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event) {
    NHttp::THttpOutgoingRequestPtr request(event->Get()->Request);
    const auto httpRequstsIt = HttpRequestNames.find(request.Get());
    TStringBuilder requestInfo;
    requestInfo << "host " << request->Host << ", url: " << request->URL;
    if (httpRequstsIt == HttpRequestNames.end()) {
        BLOG_ERROR(TStringBuilder() << "Unknown http request: " << requestInfo);
        return;
    }
    TDuration refreshPeriod = SUCCESS_REFRESH_PERIOD;
    if (event->Get()->Response != nullptr) {
        NHttp::THttpIncomingResponsePtr response = event->Get()->Response;
        if (response->Status == "200") {
            NJson::TJsonReaderConfig JsonConfig;
            NJson::TJsonValue jsonValue;
            if (NJson::ReadJsonTree(response->Body, &JsonConfig, &jsonValue)) {
                auto jsonValueMap = jsonValue.GetMap();
                if (auto it = jsonValueMap.find("access_token"); it == jsonValueMap.end()) {
                    BLOG_ERROR("Result doesn't contain access_token. Request: " << requestInfo);
                } else if (TString ticket = it->second.GetStringSafe(); ticket.empty()) {
                    BLOG_ERROR("Got empty ticket. Request: " << requestInfo);
                } else {
                    BLOG_D("Updating metadata token");
                    {
                        auto guard = Guard(TokensLock);
                        Tokens[httpRequstsIt->second] = "Bearer " + std::move(ticket);
                    }
                }
                if (auto it = jsonValueMap.find("expires_in"); it == jsonValueMap.end()) {
                    BLOG_ERROR("Result doesn't contain expires_in. Request: " << requestInfo);
                    refreshPeriod = ERROR_REFRESH_PERIOD;
                } else {
                    refreshPeriod = Min(TDuration::Seconds(it->second.GetUInteger()), SUCCESS_REFRESH_PERIOD);
                }
            }
        } else {
            BLOG_ERROR("Error refreshing metadata token, status: " << response->Status << ", error: " << response->Message);
            refreshPeriod = ERROR_REFRESH_PERIOD;
        }
    } else {
        BLOG_ERROR("Error refreshing metadata token, error: " << event->Get()->Error);
        refreshPeriod = ERROR_REFRESH_PERIOD;
    }
    RefreshQueue.push({TInstant::Now() + refreshPeriod, httpRequstsIt->second});
    HttpRequestNames.erase(httpRequstsIt);
}

void TMvpTokenator::UpdateJwtToken(const NMvp::TJwtInfo* jwtInfo) {
    auto now = std::chrono::system_clock::now();
    auto expiresAt = now + std::chrono::hours(1);
    const auto& serviceAccountId = jwtInfo->accountid();
    const auto& keyId = jwtInfo->keyid();
    std::set<std::string> audience;
    audience.insert(jwtInfo->audience());

    switch (AuthProfile) {
        case NMVP::EAuthProfile::Yandex: {
            auto algorithm = jwt::algorithm::ps256(jwtInfo->publickey(), jwtInfo->privatekey());
            auto encodedToken = jwt::create()
                    .set_key_id(keyId)
                    .set_issuer(serviceAccountId)
                    .set_audience(audience)
                    .set_issued_at(now)
                    .set_expires_at(expiresAt)
                    .sign(algorithm);
            yandex::cloud::priv::iam::v1::CreateIamTokenRequest request;
            request.set_jwt(TString(encodedToken));
            RequestCreateToken<yandex::cloud::priv::iam::v1::IamTokenService,
                                yandex::cloud::priv::iam::v1::CreateIamTokenRequest,
                                yandex::cloud::priv::iam::v1::CreateIamTokenResponse,
                                TEvPrivate::TEvUpdateIamTokenYandex>(jwtInfo->name(), jwtInfo->endpoint(), request, &yandex::cloud::priv::iam::v1::IamTokenService::Stub::AsyncCreate);

            break;
        }
        case NMVP::EAuthProfile::Nebius: {
            auto algorithm = jwt::algorithm::rs256(jwtInfo->publickey(), jwtInfo->privatekey());
            auto encodedToken = jwt::create()
                    .set_key_id(keyId)
                    .set_issuer(serviceAccountId)
                    .set_subject(serviceAccountId)
                    .set_issued_at(now)
                    .set_expires_at(expiresAt)
                    .sign(algorithm);
            nebius::iam::v1::ExchangeTokenRequest request;
            request.set_grant_type("urn:ietf:params:oauth:grant-type:token-exchange");
            request.set_requested_token_type("urn:ietf:params:oauth:token-type:access_token");
            request.set_subject_token_type("urn:ietf:params:oauth:token-type:jwt");
            request.set_subject_token(TString(encodedToken));

            RequestCreateToken<nebius::iam::v1::TokenExchangeService,
                                nebius::iam::v1::ExchangeTokenRequest,
                                nebius::iam::v1::CreateTokenResponse,
                                TEvPrivate::TEvUpdateIamTokenNebius>(jwtInfo->name(), jwtInfo->endpoint(), request, &nebius::iam::v1::TokenExchangeService::Stub::AsyncExchange, serviceAccountId);

            break;
        }
    }
}

void TMvpTokenator::UpdateOAuthToken(const NMvp::TOAuthInfo* oauthInfo) {
    yandex::cloud::priv::iam::v1::CreateIamTokenRequest request;
    request.set_yandex_passport_oauth_token(oauthInfo->token());
    RequestCreateToken<yandex::cloud::priv::iam::v1::IamTokenService,
                       yandex::cloud::priv::iam::v1::CreateIamTokenRequest,
                       yandex::cloud::priv::iam::v1::CreateIamTokenResponse,
                       TEvPrivate::TEvUpdateIamTokenYandex>(oauthInfo->name(), oauthInfo->endpoint(), request, &yandex::cloud::priv::iam::v1::IamTokenService::Stub::AsyncCreate);
}

}
