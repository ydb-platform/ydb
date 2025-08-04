#include "service-ticket-manager.h"

#include <util/datetime/base.h>
#include <util/generic/hash.h>

#include <memory>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/core/protos/auth.pb.h>

namespace NKikimr {

class TServiceTicketManager : public NActors::TActorBootstrapped<TServiceTicketManager> {
    using TBase = NActors::TActorBootstrapped<TServiceTicketManager>;

protected:
    struct TTokenProvider {
        TServiceTicketManager* Manager = nullptr;
        TString Token;

        TTokenProvider(TServiceTicketManager* const manager);
        virtual ~TTokenProvider() = 0;
        virtual void CreateToken() = 0;
        TString GetToken();
    };

private:
    struct TVmMetadataTokenProvider : public TTokenProvider {
        using Base = TTokenProvider;

        class TVmMetadataTokenProviderHandler : public NActors::TActorBootstrapped<TVmMetadataTokenProviderHandler> {
        private:
            const NActors::TActorId HttpProxyId;
            const NKikimrProto::TServiceTicketManager::TVmMetadataProvider::TVmMetadataInfo& ProviderInfo;
            const NKikimrProto::TServiceTicketManager::TTokenProviderSettings& Settings;
            TString* Token;

        public:
            TVmMetadataTokenProviderHandler(const NActors::TActorId& httpProxyId,
                                            const NKikimrProto::TServiceTicketManager::TVmMetadataProvider::TVmMetadataInfo& providerInfo,
                                            const NKikimrProto::TServiceTicketManager::TTokenProviderSettings& settings,
                                            TString* token);
            void Bootstrap();
            void StateWork(TAutoPtr<NActors::IEventHandle>& ev);
            void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr& ev);
        };

        const NActors::TActorId HttpProxyId;
        const NKikimrProto::TServiceTicketManager::TVmMetadataProvider::TVmMetadataInfo& ProviderInfo;
        const NKikimrProto::TServiceTicketManager::TTokenProviderSettings& Settings;

        TVmMetadataTokenProvider(TServiceTicketManager* const manager,
                                 const NKikimrProto::TServiceTicketManager::TVmMetadataProvider::TVmMetadataInfo& providerInfo,
                                 const NKikimrProto::TServiceTicketManager::TTokenProviderSettings& settings);

        void CreateToken() override;
    };

private:
    NKikimrProto::TServiceTicketManager Config;
    TDuration RefreshCheckPeriod = TDuration::Seconds(30);
    THashMap<TString, std::shared_ptr<TTokenProvider>> TokenProviders;

public:
    TServiceTicketManager(const NKikimrProto::TServiceTicketManager& config);

    void Bootstrap();
    void StateWork(TAutoPtr<NActors::IEventHandle>& ev);

protected:
    void PassAway() override;

private:
    void HandleRefreshCheck() const;
    void RefreshAllTokens();
};

////////////////////////////////  IMPLEMENTATION  ////////////////////////////////

TServiceTicketManager::TServiceTicketManager(const NKikimrProto::TServiceTicketManager& config)
    : Config(config)
{
    if (Config.HasVmMetadataProvider()) {
        const auto& vmMetadataProvider = Config.GetVmMetadataProvider();
        for (const auto& vmMetadataInfo : vmMetadataProvider.GetVmMetadataInfo()) {
            TokenProviders[vmMetadataInfo.GetId()] = std::make_shared<TVmMetadataTokenProvider>(vmMetadataInfo, vmMetadataProvider.GetTokenProviderSettings());
        }
    }
}

void TServiceTicketManager::Bootstrap() {
    RefreshCheckPeriod = TDuration::Parse(Config.GetRefreshCheckPeriod());
    RefreshAllTokens();
    Schedule(RefreshCheckPeriod, new NActors::TEvents::TEvWakeup());
    TBase::Become(&TServiceTicketManager::StateWork);
}

void TServiceTicketManager::StateWork(TAutoPtr<NActors::IEventHandle>& ev) {
    switch (ev->GetTypeRewrite()) {
        cFunc(NActors::TEvents::TSystem::Wakeup, HandleRefreshCheck);
        cFunc(NActors::TEvents::TSystem::PoisonPill, PassAway);
    }
}

void TServiceTicketManager::HandleRefreshCheck() const {
    Schedule(RefreshCheckPeriod, new NActors::TEvents::TEvWakeup());
}

void TServiceTicketManager::PassAway() {
    TBase::PassAway();
}

void TServiceTicketManager::RefreshAllTokens() {
    for (const auto& tokenProvider : TokenProviders) {
        tokenProvider.second->CreateToken();
    }
}

TServiceTicketManager::TTokenProvider::TTokenProvider(TServiceTicketManager* const manager)
    : Manager(manager)
{}

TString TServiceTicketManager::TTokenProvider::GetToken() {
    return Token;
}

TServiceTicketManager::TVmMetadataTokenProvider::TVmMetadataTokenProvider(TServiceTicketManager* const manager,
                                 const NKikimrProto::TServiceTicketManager::TVmMetadataProvider::TVmMetadataInfo& providerInfo,
                                 const NKikimrProto::TServiceTicketManager::TTokenProviderSettings& settings)
    : Base(manager)
    , ProviderInfo(providerInfo)
    , Settings(settings)
{}

void TServiceTicketManager::TVmMetadataTokenProvider::CreateToken() {
    Manager->Register(new TVmMetadataTokenProviderHandler(HttpProxyId, ProviderInfo, Settings, &Token));
}

TServiceTicketManager::TVmMetadataTokenProvider::TVmMetadataTokenProviderHandler::TVmMetadataTokenProviderHandler(const NActors::TActorId& httpProxyId,
                                            const NKikimrProto::TServiceTicketManager::TVmMetadataProvider::TVmMetadataInfo& providerInfo,
                                            const NKikimrProto::TServiceTicketManager::TTokenProviderSettings& settings,
                                            TString* token)
    : HttpProxyId(httpProxyId)
    , ProviderInfo(providerInfo)
    , Settings(settings)
    , Token(token)
{}

void TServiceTicketManager::TVmMetadataTokenProvider::TVmMetadataTokenProviderHandler::Bootstrap() {
    NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestGet(ProviderInfo.GetEndpoint());
    httpRequest->Set("Metadata-Flavor", "Google");
    Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest));
}

void TServiceTicketManager::TVmMetadataTokenProvider::TVmMetadataTokenProviderHandler::Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr&) {
    *Token = "qqqqq";
}

NActors::IActor* CreateServiceTicketManager(const NKikimrProto::TServiceTicketManager& config) {
    return new TServiceTicketManager(config);
}

} // NKikimr
