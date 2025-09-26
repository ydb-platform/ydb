#include <ydb/library/actors/http/http.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/library/actors/core/hfunc.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/writer/json_value.h>

#include <ydb/core/security/token_manager/vm_metadata_token_provider_handler.h>
#include <ydb/core/security/token_manager/token_provider_settings.h>
#include <ydb/core/security/token_manager/private_events.h>
#include <ydb/core/security/token_manager/token_manager_log.h>

namespace NKikimr::NTokenManager {

TVmMetadataTokenProviderHandler::TVmMetadataTokenProviderHandler(const NActors::TActorId& sender,
    const NActors::TActorId& httpProxyId,
    const NKikimrProto::TTokenManager::TVmMetadataProvider::TVmMetadataInfo& providerInfo,
    const NTokenManager::TTokenProviderSettings& settings)
    : Sender(sender)
    , HttpProxyId(httpProxyId)
    , ProviderInfo(providerInfo)
    , Settings(settings)
{}

void TVmMetadataTokenProviderHandler::Bootstrap() {
    BLOG_TRACE("Handle send request to vm metaservice");
    NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestGet(ProviderInfo.GetEndpoint());
    httpRequest->Set("Metadata-Flavor", "Google");
    std::unique_ptr<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest> outgoingRequest = std::make_unique<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(httpRequest);
    outgoingRequest->Timeout = Settings.RequestTimeout;
    Send(HttpProxyId, outgoingRequest.release());
    TBase::Become(&TVmMetadataTokenProviderHandler::StateWork);
}

void TVmMetadataTokenProviderHandler::StateWork(TAutoPtr<NActors::IEventHandle>& ev) {
    switch (ev->GetTypeRewrite()) {
        hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
    }
}

void TVmMetadataTokenProviderHandler::Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr& ev) {
    NHttp::THttpOutgoingRequestPtr request(ev->Get()->Request);
    TStringBuilder requestInfo;
    requestInfo << "host " << request->Host << ", url: " << request->URL;
    TString token;
    TDuration refreshPeriod = Settings.MaxErrorRefreshPeriod;
    TDuration tokenExpiresIn = Max(Settings.MaxErrorRefreshPeriod, Settings.SuccessRefreshPeriod);
    TEvTokenManager::TStatus status {.Code = TEvTokenManager::TStatus::ECode::SUCCESS, .Message = "Ok"};
    if (ev->Get()->Response != nullptr) {
        NHttp::THttpIncomingResponsePtr response = ev->Get()->Response;
        if (response->Status == "200") {
            NJson::TJsonReaderConfig JsonConfig;
            NJson::TJsonValue jsonValue;
            if (NJson::ReadJsonTree(response->Body, &JsonConfig, &jsonValue)) {
                auto jsonValueMap = jsonValue.GetMap();
                if (auto it = jsonValueMap.find("access_token"); it == jsonValueMap.end()) {
                    BLOG_ERROR("Result doesn't contain access_token. Request: " << requestInfo);
                    status = {.Code = TEvTokenManager::TStatus::ECode::ERROR, .Message = "Result doesn't contain access_token"};
                } else if (token = it->second.GetStringSafe(); token.empty()) {
                    BLOG_ERROR("Got empty token. Request: " << requestInfo);
                    status = {.Code = TEvTokenManager::TStatus::ECode::ERROR, .Message = "Got empty token"};
                } else {
                    BLOG_D("Updating vm metadata token");
                    refreshPeriod = Settings.SuccessRefreshPeriod;
                }
                if (auto it = jsonValueMap.find("expires_in"); it == jsonValueMap.end()) {
                    BLOG_ERROR("Result doesn't contain expires_in. Request: " << requestInfo);
                } else {
                    tokenExpiresIn = TDuration::Seconds(it->second.GetUInteger());
                }
            } else {
                BLOG_ERROR("Can not read json");
                status = {.Code = TEvTokenManager::TStatus::ECode::ERROR, .Message = "Can not read json"};
            }
        } else {
            BLOG_ERROR("Error refreshing metadata token, status: " << response->Status << ", error: " << response->Message);
            status = {.Code = TEvTokenManager::TStatus::ECode::ERROR, .Message = TString(response->Message)};
        }
    } else {
        BLOG_ERROR("Error refreshing metadata token, error: " << ev->Get()->Error);
        status = {.Code = TEvTokenManager::TStatus::ECode::ERROR, .Message = ev->Get()->Error};
    }
    refreshPeriod = Min(tokenExpiresIn, refreshPeriod);
    Send(Sender, new NTokenManager::TEvPrivate::TEvUpdateToken(ProviderInfo.GetId(), status, token, refreshPeriod));
    PassAway();
}

} // NKikimr::NTokenManager
