#include "device_provider.h"

namespace NYdb::inline Dev::NOidc::NPrivate {

class TDeviceProvider::TState final: public TProviderState {
public:
    TState(const TOidcConfig& config, std::weak_ptr<ICoreFacility> facility, bool standalone)
        : TProviderState(config, std::move(facility), standalone)
    {
    }

private:
    TTokenCache Bootstrap() override {
        return ReadCache().value_or(TTokenCache{});
    }

    TTokenCache AcquireToken(const TTokenCache& current, std::unique_ptr<ITokenCacheLock>& lock) override {
        // User interaction must not hold the process lock. Another provider may
        // obtain a usable token while this one waits for authorization.
        lock.reset();
        auto tokens = GetProtocol().DeviceGrant([this](TDuration delay) { return Wait(delay); });
        lock = LockCache();
        if (auto cached = ReadCache(); cached && cached->AccessToken.IsValid(TInstant::Now()) &&
                                       cached->AccessToken.Token != current.AccessToken.Token) {
            return *cached;
        }
        Write(tokens);
        return tokens;
    }
};

TDeviceProvider::TDeviceProvider(const TOidcConfig& config, std::weak_ptr<ICoreFacility> facility, bool standalone)
    : TProviderBase(std::make_shared<TState>(config, std::move(facility), standalone))
{
}

} // namespace NYdb::inline Dev::NOidc::NPrivate
