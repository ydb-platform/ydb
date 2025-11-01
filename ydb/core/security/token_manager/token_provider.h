#pragma once

#include <util/datetime/base.h>
#include <ydb/library/actors/core/actorid.h>

#include <ydb/core/protos/auth.pb.h>
#include <ydb/core/security/token_manager/token_manager.h>

namespace NKikimr::NTokenManager {

struct TTokenProviderSettings;

struct TTokenProvider {
protected:
    const NActors::TActorId TokenManagerId;
    const TTokenProviderSettings& Settings;
    TString Token;
    TInstant RefreshTime;
    TDuration Delay;
    TEvTokenManager::TStatus Status {
        .Code = TEvTokenManager::TStatus::ECode::NOT_READY,
        .Message = "Token is not ready"
    };

public:
    TTokenProvider(const NActors::TActorId& tokenManagerId, const NTokenManager::TTokenProviderSettings& settings);
    virtual ~TTokenProvider() = default;
    virtual NActors::IActor* CreateTokenProviderHandler() = 0;
    virtual TString GetId() const = 0;

    TString GetToken() const;
    void UpdateToken(const TString& token, const TDuration& refreshPeriod);
    void SetError(const TEvTokenManager::TStatus& status, const TDuration& refreshPeriod);
    TInstant GetRefreshTime() const;
    TEvTokenManager::TStatus GetStatus() const;
};

struct TVmMetadataTokenProvider : public TTokenProvider {
    using Base = TTokenProvider;

    const NActors::TActorId HttpProxyId;
    const NKikimrProto::TTokenManager::TVmMetadataProvider::TVmMetadataInfo& ProviderInfo;

    TVmMetadataTokenProvider(const NActors::TActorId& tokenManagerId,
        const NTokenManager::TTokenProviderSettings& settings,
        const NActors::TActorId& httpProxyId,
        const NKikimrProto::TTokenManager::TVmMetadataProvider::TVmMetadataInfo& providerInfo);

    NActors::IActor* CreateTokenProviderHandler() override;
    TString GetId() const override;
};

} // NKikimr::NTokenManager
