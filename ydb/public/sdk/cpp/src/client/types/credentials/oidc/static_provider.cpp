#include "static_provider.h"

namespace NYdb::inline Dev::NOidc::NPrivate {

class TStaticProvider::TState final: public TProviderState {
public:
    TState(const TOidcConfig& config, std::weak_ptr<ICoreFacility> facility, bool standalone)
        : TProviderState(config, std::move(facility), standalone)
    {
    }

private:
    TTokenCache Bootstrap() override {
        const auto& flow = std::get<TStaticOidcConfig>(Config.FlowConfig);
        TTokenCache current;
        current.AccessToken = {flow.AccessToken, flow.ExpiresAt};
        if (!current.AccessToken.ExpiresAt) {
            current.AccessToken.ExpiresAt = JwtExpiry(current.AccessToken.Token);
        }
        if (!flow.RefreshToken.empty()) {
            current.RefreshToken = TOAuthToken{flow.RefreshToken, flow.RefreshExpiresAt};
        }
        auto lock = LockCache();
        Write(current);
        return current;
    }

    TTokenCache AcquireToken(const TTokenCache&, std::unique_ptr<ITokenCacheLock>&) override {
        throw TError("static credentials cannot be refreshed", false, {});
    }

    bool WaitForRefresh(const TTokenCache& current) override {
        if (!current.RefreshToken || !current.RefreshToken->IsValid(TInstant::Now())) {
            if (current.AccessToken.ExpiresAt) {
                Fail(std::make_exception_ptr(TError("static credentials have expired and cannot be refreshed", false, {})));
            }
            return false;
        }
        return TProviderState::WaitForRefresh(current);
    }
};

TStaticProvider::TStaticProvider(const TOidcConfig& config, std::weak_ptr<ICoreFacility> facility, bool standalone)
    : TProviderBase(std::make_shared<TState>(config, std::move(facility), standalone))
{
}

} // namespace NYdb::inline Dev::NOidc::NPrivate
