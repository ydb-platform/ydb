#include <ydb/core/security/token_manager/token_provider.h>
#include <ydb/core/security/token_manager/token_provider_settings.h>
#include <ydb/core/security/token_manager/vm_metadata_token_provider_handler.h>

namespace NKikimr::NTokenManager {

TTokenProvider::TTokenProvider(const NActors::TActorId& tokenManagerId, const NTokenManager::TTokenProviderSettings& settings)
    : TokenManagerId(tokenManagerId)
    , Settings(settings)
    , Delay(Settings.MinErrorRefreshPeriod)
{}

TString TTokenProvider::GetToken() const {
    return Token;
}

void TTokenProvider::UpdateToken(const TString& token, const TDuration& refreshPeriod) {
    Token = token;
    Delay = Settings.MinErrorRefreshPeriod;
    RefreshTime = NActors::TlsActivationContext->Now() + Min(refreshPeriod, Settings.SuccessRefreshPeriod);
    Status = {.Code = TEvTokenManager::TStatus::ECode::SUCCESS, .Message = "OK"};
}

void TTokenProvider::SetError(const TEvTokenManager::TStatus& status, const TDuration& refreshPeriod) {
    const constexpr double scaleFactor = 2.0;
    Status = status;
    RefreshTime = NActors::TlsActivationContext->Now() + TDuration::FromValue((Delay.GetValue() * 9) / 10 + RandomNumber(Delay.GetValue() / 10 + 1));
    Delay = Min(Delay * scaleFactor, Min(refreshPeriod, Settings.MaxErrorRefreshPeriod));
}

TInstant TTokenProvider::GetRefreshTime() const {
    return RefreshTime;
}

TEvTokenManager::TStatus TTokenProvider::GetStatus() const {
    return Status;
}

TVmMetadataTokenProvider::TVmMetadataTokenProvider(const NActors::TActorId& tokenManagerId,
    const NTokenManager::TTokenProviderSettings& settings,
    const NActors::TActorId& httpProxyId,
    const NKikimrProto::TTokenManager::TVmMetadataProvider::TVmMetadataInfo& providerInfo)
    : Base(tokenManagerId, settings)
    , HttpProxyId(httpProxyId)
    , ProviderInfo(providerInfo)
{}

NActors::IActor* TVmMetadataTokenProvider::CreateTokenProviderHandler() {
    return new NTokenManager::TVmMetadataTokenProviderHandler(TokenManagerId, HttpProxyId, ProviderInfo, Settings);
}

TString TVmMetadataTokenProvider::GetId() const {
    return ProviderInfo.GetId();
}

} // NKikimr::NTokenManager
