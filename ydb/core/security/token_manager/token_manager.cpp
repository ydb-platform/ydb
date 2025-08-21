#include <util/datetime/base.h>
#include <util/generic/hash.h>

#include <memory>

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/core/protos/auth.pb.h>
#include <ydb/core/security/token_manager/token_manager.h>
#include <ydb/core/security/token_manager/token_manager_log.h>
#include <ydb/core/security/token_manager/private_events.h>
#include <ydb/core/security/token_manager/token_provider.h>

namespace NKikimr {

TTokenManager::TTokenManager(const TTokenManagerSettings& settings)
    : HttpProxyId(settings.HttpProxyId)
    , Config(settings.Config)
{}

void TTokenManager::Bootstrap() {
    RefreshCheckPeriod = TDuration::Parse(Config.GetRefreshCheckPeriod());
    BootstrapTokenProviders();
    RefreshAllTokens();
    Schedule(RefreshCheckPeriod, new NActors::TEvents::TEvWakeup());
    TBase::Become(&TTokenManager::StateWork);
}

void TTokenManager::BootstrapTokenProviders() {
    if (Config.HasVmMetadataProvider()) {
        NActors::TActorId httpProxyId;
        if (HttpProxyId.has_value()) {
            httpProxyId = HttpProxyId.value();
        } else {
            httpProxyId = Register(NHttp::CreateHttpProxy());
        }

        const auto& vmMetadataProvider = Config.GetVmMetadataProvider();
        const auto& tokenProviderSettings = vmMetadataProvider.GetSettings();
        VmMetadataProviderSettings =  {
            .SuccessRefreshPeriod = TDuration::Parse(tokenProviderSettings.GetSuccessRefreshPeriod()),
            .MinErrorRefreshPeriod = TDuration::Parse(tokenProviderSettings.GetMinErrorRefreshPeriod()),
            .MaxErrorRefreshPeriod = TDuration::Parse(tokenProviderSettings.GetMaxErrorRefreshPeriod()),
            .RequestTimeout = TDuration::Parse(tokenProviderSettings.GetRequestTimeout())
        };
        for (const auto& vmMetadataInfo : vmMetadataProvider.GetProvidersInfo()) {
            BLOG_TRACE("Initialize token provider# " << vmMetadataInfo.GetId());
            TokenProviders[vmMetadataInfo.GetId()] = std::make_shared<NTokenManager::TVmMetadataTokenProvider>(this->SelfId(),
                                                        VmMetadataProviderSettings,
                                                        httpProxyId,
                                                        vmMetadataInfo);
        }
    }
}

void TTokenManager::StateWork(TAutoPtr<NActors::IEventHandle>& ev) {
    switch (ev->GetTypeRewrite()) {
        hFunc(NTokenManager::TEvPrivate::TEvUpdateToken, Handle);
        hFunc(TEvTokenManager::TEvSubscribeUpdateToken, Handle);
        cFunc(NActors::TEvents::TSystem::Wakeup, HandleRefreshCheck);
        cFunc(NActors::TEvents::TSystem::PoisonPill, PassAway);
    }
}

void TTokenManager::HandleRefreshCheck() {
    BLOG_TRACE("Handle refresh tokens");
    while (!RefreshQueue.empty() && RefreshQueue.top().RefreshTime <= NActors::TlsActivationContext->Now()) {
        std::shared_ptr<NTokenManager::TTokenProvider> provider = RefreshQueue.top().Provider;
        RefreshQueue.pop();
        BLOG_D("Refresh token for provider# " << provider->GetId());
        Register(provider->CreateTokenProviderHandler());
    }
    Schedule(RefreshCheckPeriod, new NActors::TEvents::TEvWakeup());
}

void TTokenManager::PassAway() {
    TBase::PassAway();
}

void TTokenManager::RefreshAllTokens() {
    for (const auto& tokenProvider : TokenProviders) {
        Register(tokenProvider.second->CreateTokenProviderHandler());
    }
}

void TTokenManager::NotifySubscribers(const std::shared_ptr<NTokenManager::TTokenProvider>& provider) const {
    BLOG_TRACE("Handle NotifySubscribers");
    auto it = Subscribers.find(provider->GetId());
    if (it != Subscribers.end()) {
        BLOG_D("Notify subscribers# " << provider->GetId());
        for (const auto& subscriber : it->second) {
            Send(subscriber, new TEvTokenManager::TEvUpdateToken(provider->GetId(), provider->GetToken(), provider->GetStatus()));
        }
    } else {
        BLOG_ERROR("Can not find subscribers# " << provider->GetId());
    }
}

void TTokenManager::Handle(NTokenManager::TEvPrivate::TEvUpdateToken::TPtr& ev) {
    BLOG_TRACE("Handle TEvPrivate::TEvUpdateToken");
    const TString& tokenProviderId = ev->Get()->Id;
    auto it = TokenProviders.find(tokenProviderId);
    if (it != TokenProviders.end()) {
        if (ev->Get()->Status.Code == TEvTokenManager::TStatus::ECode::SUCCESS) {
            BLOG_D("Update token for provider# " << tokenProviderId);
            it->second->UpdateToken(ev->Get()->Token, ev->Get()->RefreshPeriod);
        } else {
            BLOG_D("Can not update token for provider# " << tokenProviderId << " (" << ev->Get()->Status.Message << ")");
            it->second->SetError(ev->Get()->Status, ev->Get()->RefreshPeriod);
        }
        RefreshQueue.push({.RefreshTime = it->second->GetRefreshTime(), .Provider = it->second});
        NotifySubscribers(it->second);
    } else {
        BLOG_ERROR("TEvPrivate::TEvUpdateToken: Can not find token provider");
    }
}

void TTokenManager::Handle(TEvTokenManager::TEvSubscribeUpdateToken::TPtr& ev) {
    TString id = ev->Get()->Id;
    BLOG_D("Handle TEvTokenManager::TEvSubscribeUpdateToken to token provider# " << id);
    auto it = TokenProviders.find(id);
    if (it != TokenProviders.end()) {
        Subscribers[id].insert(ev->Sender);
        Send(ev->Sender, new TEvTokenManager::TEvUpdateToken(id, it->second->GetToken(), it->second->GetStatus()));
    } else {
        const TString errorMessage = "Token provider " + id + " was not found";
        BLOG_ERROR("Handle TEvSTokenManager::TEvSubscribeUpdateToken: " << errorMessage);
        Send(ev->Sender, new TEvTokenManager::TEvUpdateToken(id, "", {.Code = TEvTokenManager::TStatus::ECode::ERROR, .Message = errorMessage}));
    }
}

NActors::IActor* CreateTokenManager(const TTokenManagerSettings& settings) {
    return new TTokenManager(settings);
}

} // NKikimr
