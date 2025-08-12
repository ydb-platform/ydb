#include "token_manager.h"
#include "token_manager_log.h"
#include "private_events.h"
#include "token_provider_settings.h"
#include "vm_metadata_token_provider_handler.h"

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/queue.h>

#include <memory>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/core/protos/auth.pb.h>

namespace NKikimr {

class TTokenManager : public NActors::TActorBootstrapped<TTokenManager> {
    using TBase = NActors::TActorBootstrapped<TTokenManager>;

public:
    struct TInitializer {
        NKikimrProto::TTokenManager Config;
        NActors::TActorId HttpProxyId;
    };

protected:
    struct TTokenProvider;

private:
    struct TRefreshRecord {
        TInstant RefreshTime;
        std::shared_ptr<TTokenProvider> Provider;

        bool operator <(const TRefreshRecord& other) const {
            return RefreshTime > other.RefreshTime;
        }
    };

    struct TVmMetadataTokenProvider;

private:
    NKikimrProto::TTokenManager Config;
    TDuration RefreshCheckPeriod = TDuration::Seconds(30);
    THashMap<TString, std::shared_ptr<TTokenProvider>> TokenProviders;
    NTokenManager::TTokenProviderSettings VmMetadataProviderSettings;
    TPriorityQueue<TRefreshRecord> RefreshQueue;
    NActors::TActorId HttpProxyId;
    THashMap<TString, THashSet<NActors::TActorId>> Subscribers;

public:
    TTokenManager(const TInitializer& initializer);
    TTokenManager(const NKikimrProto::TTokenManager& config);

    void Bootstrap();
    void StateWork(TAutoPtr<NActors::IEventHandle>& ev);
    void BootstrapVmMetadataProvider();

protected:
    void PassAway() override;

private:
    void HandleRefreshCheck();
    void RefreshAllTokens();
    void NotifySubscribers(const std::shared_ptr<TTokenProvider>& provider) const;

    void Handle(NTokenManager::TEvPrivate::TEvUpdateToken::TPtr& ev);
    void Handle(TEvTokenManager::TEvSubscribeUpdateToken::TPtr& ev);
};

struct TTokenManager::TTokenProvider {
protected:
    TTokenManager* Manager = nullptr;
    const NTokenManager::TTokenProviderSettings& Settings;
    TString Token;
    TInstant RefreshTime;
    TDuration Delay;
    TEvTokenManager::TStatus Status {
        .Code = TEvTokenManager::TStatus::ECode::NOT_READY,
        .Message = "Token is not ready"
    };

public:
    TTokenProvider(TTokenManager* const manager, const NTokenManager::TTokenProviderSettings& settings);
    virtual ~TTokenProvider() = default;
    virtual void CreateToken() = 0;
    virtual TString GetId() const = 0;

    TString GetToken() const;
    void UpdateToken(const TString& token, const TDuration& refreshPeriod);
    void SetError(const TEvTokenManager::TStatus& status, const TDuration& refreshPeriod);
    TInstant GetRefreshTime() const;
    TEvTokenManager::TStatus GetStatus() const;
};

struct TTokenManager::TVmMetadataTokenProvider : public TTokenManager::TTokenProvider {
    using Base = TTokenProvider;

    const NActors::TActorId HttpProxyId;
    const NKikimrProto::TTokenManager::TVmMetadataProvider::TVmMetadataInfo& ProviderInfo;

    TVmMetadataTokenProvider(TTokenManager* const manager,
                            const NTokenManager::TTokenProviderSettings& settings,
                            const NActors::TActorId httpProxyId,
                            const NKikimrProto::TTokenManager::TVmMetadataProvider::TVmMetadataInfo& providerInfo);

    void CreateToken() override;
    TString GetId() const override;
};

////////////////////////////////  IMPLEMENTATION  ////////////////////////////////

TTokenManager::TTokenManager(const TInitializer& initializer)
    : Config(initializer.Config)
    , HttpProxyId(initializer.HttpProxyId)
{}

TTokenManager::TTokenManager(const NKikimrProto::TTokenManager& config)
    : Config(config)
{
    HttpProxyId = Register(NHttp::CreateHttpProxy());
}

void TTokenManager::Bootstrap() {
    RefreshCheckPeriod = TDuration::Parse(Config.GetRefreshCheckPeriod());
    BootstrapVmMetadataProvider();
    RefreshAllTokens();
    Schedule(RefreshCheckPeriod, new NActors::TEvents::TEvWakeup());
    TBase::Become(&TTokenManager::StateWork);
}

void TTokenManager::BootstrapVmMetadataProvider() {
    if (Config.HasVmMetadataProvider()) {
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
            TokenProviders[vmMetadataInfo.GetId()] = std::make_shared<TVmMetadataTokenProvider>(this, VmMetadataProviderSettings, HttpProxyId, vmMetadataInfo);
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
        std::shared_ptr<TTokenProvider> provider = RefreshQueue.top().Provider;
        RefreshQueue.pop();
        BLOG_D("Refresh token for provider# " << provider->GetId());
        provider->CreateToken();
    }
    Schedule(RefreshCheckPeriod, new NActors::TEvents::TEvWakeup());
}

void TTokenManager::PassAway() {
    TBase::PassAway();
}

void TTokenManager::RefreshAllTokens() {
    for (const auto& tokenProvider : TokenProviders) {
        tokenProvider.second->CreateToken();
    }
}

void TTokenManager::NotifySubscribers(const std::shared_ptr<TTokenProvider>& provider) const {
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

TTokenManager::TTokenProvider::TTokenProvider(TTokenManager* const manager, const NTokenManager::TTokenProviderSettings& settings)
    : Manager(manager)
    , Settings(settings)
    , Delay(Settings.MinErrorRefreshPeriod)
{}

TString TTokenManager::TTokenProvider::GetToken() const {
    return Token;
}

void TTokenManager::TTokenProvider::UpdateToken(const TString& token, const TDuration& refreshPeriod) {
    Token = token;
    Delay = Settings.MinErrorRefreshPeriod;
    RefreshTime = NActors::TlsActivationContext->Now() + Min(refreshPeriod, Settings.SuccessRefreshPeriod);
    Status = {.Code = TEvTokenManager::TStatus::ECode::SUCCESS, .Message = "OK"};
}

void TTokenManager::TTokenProvider::SetError(const TEvTokenManager::TStatus& status, const TDuration& refreshPeriod) {
    const static double scaleFactor = 2.0;
    Status = status;
    RefreshTime = NActors::TlsActivationContext->Now() + TDuration::FromValue((Delay.GetValue() * 9) / 10 + RandomNumber(Delay.GetValue() / 10 + 1));
    Delay = Min(Delay * scaleFactor, Min(refreshPeriod, Settings.MaxErrorRefreshPeriod));
}

TInstant TTokenManager::TTokenProvider::GetRefreshTime() const {
    return RefreshTime;
}

TEvTokenManager::TStatus TTokenManager::TTokenProvider::GetStatus() const {
    return Status;
}

TTokenManager::TVmMetadataTokenProvider::TVmMetadataTokenProvider(TTokenManager* const manager,
                                 const NTokenManager::TTokenProviderSettings& settings,
                                 const NActors::TActorId httpProxyId,
                                 const NKikimrProto::TTokenManager::TVmMetadataProvider::TVmMetadataInfo& providerInfo)
    : Base(manager, settings)
    , HttpProxyId(httpProxyId)
    , ProviderInfo(providerInfo)
{}

void TTokenManager::TVmMetadataTokenProvider::CreateToken() {
    Manager->Register(new NTokenManager::TVmMetadataTokenProviderHandler(Manager->SelfId(), HttpProxyId, ProviderInfo, Settings));
}

TString TTokenManager::TVmMetadataTokenProvider::GetId() const {
    return ProviderInfo.GetId();
}

NActors::IActor* CreateTokenManager(const NKikimrProto::TTokenManager& config) {
    return new TTokenManager(config);
}

NActors::IActor* CreateTokenManager(const NKikimrProto::TTokenManager& config, const NActors::TActorId& HttpProxyId) {
    return new TTokenManager({.Config = config, .HttpProxyId = HttpProxyId});
}

} // NKikimr
