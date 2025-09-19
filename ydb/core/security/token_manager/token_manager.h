#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/core/base/events.h>
#include <ydb/core/protos/auth.pb.h>

#include <util/generic/queue.h>

#include <ydb/core/security/token_manager/token_manager_settings.h>
#include <ydb/core/security/token_manager/token_provider_settings.h>

namespace NKikimrProto {

class TTokenManager;

} // NKikimrProto


namespace NKikimr {

namespace TEvTokenManager {
    enum EEv {
        EvSubscribeUpdateToken = EventSpaceBegin(TKikimrEvents::ES_TOKEN_MANAGER),
        EvUpdateToken,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_TOKEN_MANAGER), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_TOKEN_MANAGER)");

    struct TEvSubscribeUpdateToken : NActors::TEventLocal<TEvSubscribeUpdateToken, EvSubscribeUpdateToken> {
        TString Id;

        TEvSubscribeUpdateToken(const TString& id)
            : Id(id)
        {}
    };

    struct TStatus {
        enum class ECode {
            SUCCESS,
            NOT_READY,
            ERROR,
        };

        ECode Code;
        TString Message;
    };

    struct TEvUpdateToken : NActors::TEventLocal<TEvUpdateToken, EvUpdateToken> {
        TString Id;
        TString Token;
        TStatus Status;

        TEvUpdateToken(const TString& id, const TString& token, const TStatus& status)
            : Id(id)
            , Token(token)
            , Status(status)
        {}
    };
};

namespace NTokenManager {

struct TTokenProvider;

namespace TEvPrivate {

struct TEvUpdateToken;
using TEvUpdateToken_HandlePtr = TAutoPtr<NActors::TEventHandle<TEvUpdateToken>>;

} // TEvPrivate
} // NTokenManager

class TTokenManager : public NActors::TActorBootstrapped<TTokenManager> {
    using TBase = NActors::TActorBootstrapped<TTokenManager>;

private:
    struct TRefreshRecord {
        TInstant RefreshTime;
        std::shared_ptr<NTokenManager::TTokenProvider> Provider;

        bool operator <(const TRefreshRecord& other) const {
            return RefreshTime > other.RefreshTime;
        }
    };

private:
    TDuration RefreshCheckPeriod = TDuration::Seconds(30);
    NTokenManager::TTokenProviderSettings VmMetadataProviderSettings;
    TPriorityQueue<TRefreshRecord> RefreshQueue;
    std::optional<NActors::TActorId> HttpProxyId;
    THashMap<TString, THashSet<NActors::TActorId>> Subscribers;

protected:
    NKikimrProto::TTokenManager Config;
    THashMap<TString, std::shared_ptr<NTokenManager::TTokenProvider>> TokenProviders;

public:
    TTokenManager(const TTokenManagerSettings& settings);

    void Bootstrap();
    void StateWork(TAutoPtr<NActors::IEventHandle>& ev);

protected:
    virtual void BootstrapTokenProviders();
    void PassAway() override;

private:
    void HandleRefreshCheck();
    void RefreshAllTokens();
    void NotifySubscribers(const std::shared_ptr<NTokenManager::TTokenProvider>& provider) const;

    void Handle(NTokenManager::TEvPrivate::TEvUpdateToken_HandlePtr& ev);
    void Handle(TEvTokenManager::TEvSubscribeUpdateToken::TPtr& ev);
};


inline NActors::TActorId MakeTokenManagerID() {
    constexpr const char name[12] = "srvtokmngr";
    return NActors::TActorId(0, TStringBuf(name, 12));
}

NActors::IActor* CreateTokenManager(const TTokenManagerSettings& settings);

} // NKikimr
