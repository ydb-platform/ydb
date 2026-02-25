#include "mvp_tokens.h"
#include "mvp_token_exchange.h"

#include <ydb/public/api/grpc/ydb_auth_v1.grpc.pb.h>

#include <util/string/builder.h>

namespace NMVP {

TMvpTokenator* TMvpTokenator::CreateTokenator(const NMvp::TTokensConfig& tokensConfig,  const NActors::TActorId& httpProxy) {
    return new TMvpTokenator(tokensConfig, httpProxy);
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

TMvpTokenator::TMvpTokenator(NMvp::TTokensConfig tokensConfig, const NActors::TActorId& httpProxy)
    : HttpProxy(httpProxy)
{
    if (tokensConfig.HasStaffApiUserTokenInfo()) {
        UpdateStaffApiUserToken(&tokensConfig.GetStaffApiUserTokenInfo());
    }
    TokenConfigs.AccessServiceType = tokensConfig.GetAccessServiceType();
    for (const NMvp::TOAuth2Exchange& tokenExchangeInfo : tokensConfig.GetOAuth2Exchange()) {
        TokenConfigs.OAuth2ExchangeConfigs[tokenExchangeInfo.GetName()] = tokenExchangeInfo;
    }
    for (const NMvp::TOAuthInfo& oauthInfo : tokensConfig.GetOAuthInfo()) {
        TokenConfigs.OauthTokenConfigs[oauthInfo.GetName()] = oauthInfo;
    }
    for (const NMvp::TMetadataTokenInfo& metadataTokenInfo : tokensConfig.GetMetadataTokenInfo()) {
        TokenConfigs.MetadataTokenConfigs[metadataTokenInfo.GetName()] = metadataTokenInfo;
    }
    for (const NMvp::TStaticCredentialsInfo& staticCredentialsInfo : tokensConfig.GetStaticCredentialsInfo()) {
        TokenConfigs.StaticCredentialsConfigs[staticCredentialsInfo.GetName()] = staticCredentialsInfo;
    }
}

void TMvpTokenator::Bootstrap() {
    for (const auto& [name, config] : TokenConfigs.OAuth2ExchangeConfigs) {
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
    const NMvp::TOAuth2Exchange* tokenExchangeInfo = TokenConfigs.GetOAuth2ExchangeConfig(name);
    if (tokenExchangeInfo != nullptr) {
        UpdateOAuth2ExchangeToken(tokenExchangeInfo);
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
        const i64 responseRefresh = event->Get()->Response.expires_in() / 2;
        if (responseRefresh > 0) {
            refreshPeriod = TDuration::Seconds(responseRefresh);
        }
        BLOG_D("Updating token " << event->Get()->Name);
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

const NMvp::TOAuth2Exchange* TMvpTokenator::TTokenConfigs::GetOAuth2ExchangeConfig(const TString& name) {
    return GetTokenConfig(OAuth2ExchangeConfigs, name);
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
    Tokens[staffApiUserTokenInfo->GetName()] = "OAuth " + staffApiUserTokenInfo->GetToken();
}

void TMvpTokenator::UpdateStaticCredentialsToken(const NMvp::TStaticCredentialsInfo* staticCredentialsInfo) {
    Ydb::Auth::LoginRequest request;
    request.set_user(staticCredentialsInfo->GetLogin());
    request.set_password(staticCredentialsInfo->GetPassword());

    RequestCreateToken<Ydb::Auth::V1::AuthService,
                       Ydb::Auth::LoginRequest,
                       Ydb::Auth::LoginResponse,
                       TEvPrivate::TEvUpdateStaticCredentialsToken>(staticCredentialsInfo->GetName(), staticCredentialsInfo->GetEndpoint(), request, &Ydb::Auth::V1::AuthService::Stub::AsyncLogin);
}

void TMvpTokenator::UpdateMetadataToken(const NMvp::TMetadataTokenInfo* metadataTokenInfo) {
    const TStringBuf& url = metadataTokenInfo->GetEndpoint();
    NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestGet(url);
    httpRequest->Set("Metadata-Flavor", "Google");
    HttpRequestNames.emplace(httpRequest.Get(), metadataTokenInfo->GetName());
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

void TMvpTokenator::UpdateOAuth2ExchangeToken(const NMvp::TOAuth2Exchange* tokenExchangeInfo) {
    TString endpoint = tokenExchangeInfo->GetTokenEndpoint();
    if (endpoint.empty()) {
        BLOG_ERROR("Token endpoint is empty for token " << tokenExchangeInfo->GetName());
        RefreshQueue.push({TInstant::Now() + ERROR_REFRESH_PERIOD, tokenExchangeInfo->GetName()});
        return;
    }

    TOAuth2ExchangeData prepared;
    TString error;
    if (!TryBuildOAuth2ExchangeData(tokenExchangeInfo, prepared, error)) {
        BLOG_ERROR(error);
        RefreshQueue.push({TInstant::Now() + ERROR_REFRESH_PERIOD, tokenExchangeInfo->GetName()});
        return;
    }

    if (TokenConfigs.AccessServiceType == NMvp::yandex_v2) {
        if (prepared.SubjectToken.empty()) {
            BLOG_ERROR("Failed to build JWT for yandex_v2 oauth2_token_exchange token " << tokenExchangeInfo->GetName());
            RefreshQueue.push({TInstant::Now() + ERROR_REFRESH_PERIOD, tokenExchangeInfo->GetName()});
            return;
        }

        yandex::cloud::priv::iam::v1::CreateIamTokenRequest request;
        request.set_jwt(prepared.SubjectToken);
        RequestCreateToken<yandex::cloud::priv::iam::v1::IamTokenService,
                           yandex::cloud::priv::iam::v1::CreateIamTokenRequest,
                           yandex::cloud::priv::iam::v1::CreateIamTokenResponse,
                           TEvPrivate::TEvUpdateIamTokenYandex>(
                               tokenExchangeInfo->GetName(),
                               endpoint,
                               request,
                               &yandex::cloud::priv::iam::v1::IamTokenService::Stub::AsyncCreate);
        return;
    }

    if (TokenConfigs.AccessServiceType == NMvp::nebius_v1) {
        nebius::iam::v1::ExchangeTokenRequest exchangeRequest;
        exchangeRequest.set_grant_type(prepared.GrantType);
        exchangeRequest.set_requested_token_type(prepared.RequestedTokenType);
        if (!prepared.Audience.empty()) {
            exchangeRequest.set_audience(prepared.Audience);
        }
        for (const auto& scope : prepared.Scopes) {
            exchangeRequest.add_scopes(scope);
        }
        for (const auto& res : prepared.Resources) {
            exchangeRequest.add_resource(res);
        }
        exchangeRequest.set_subject_token(prepared.SubjectToken);
        if (!prepared.SubjectTokenType.empty()) {
            exchangeRequest.set_subject_token_type(prepared.SubjectTokenType);
        }
        if (!prepared.ActorToken.empty()) {
            exchangeRequest.set_actor_token(prepared.ActorToken);
        }
        if (!prepared.ActorTokenType.empty()) {
            exchangeRequest.set_actor_token_type(prepared.ActorTokenType);
        }
        RequestCreateToken<nebius::iam::v1::TokenExchangeService,
                            nebius::iam::v1::ExchangeTokenRequest,
                            nebius::iam::v1::CreateTokenResponse,
                            TEvPrivate::TEvUpdateIamTokenNebius>(
                                tokenExchangeInfo->GetName(),
                                endpoint,
                                exchangeRequest,
                                &nebius::iam::v1::TokenExchangeService::Stub::AsyncExchange);
        return;
    }

    BLOG_ERROR("Unsupported access service type for oauth2_token_exchange token " << tokenExchangeInfo->GetName());
    RefreshQueue.push({TInstant::Now() + ERROR_REFRESH_PERIOD, tokenExchangeInfo->GetName()});
}

void TMvpTokenator::UpdateOAuthToken(const NMvp::TOAuthInfo* oauthInfo) {
    yandex::cloud::priv::iam::v1::CreateIamTokenRequest request;
    request.set_yandex_passport_oauth_token(oauthInfo->GetToken());
    RequestCreateToken<yandex::cloud::priv::iam::v1::IamTokenService,
                       yandex::cloud::priv::iam::v1::CreateIamTokenRequest,
                       yandex::cloud::priv::iam::v1::CreateIamTokenResponse,
                       TEvPrivate::TEvUpdateIamTokenYandex>(oauthInfo->GetName(), oauthInfo->GetEndpoint(), request, &yandex::cloud::priv::iam::v1::IamTokenService::Stub::AsyncCreate);
}

}
