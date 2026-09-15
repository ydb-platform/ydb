#include "client_provider.h"

namespace NYdb::inline Dev::NOidc::NPrivate {

class TClientProvider::TState final: public TProviderState {
public:
    TState(const TOidcConfig& config, std::weak_ptr<ICoreFacility> facility, bool standalone)
        : TProviderState(config, std::move(facility), standalone)
    {
    }

private:
    TTokenCache Bootstrap() override {
        return ReadCache().value_or(TTokenCache{});
    }

    TTokenCache AcquireToken(const TTokenCache&, std::unique_ptr<ITokenCacheLock>&) override {
        auto tokens = GetProtocol().ClientGrant();
        Write(tokens);
        return tokens;
    }
};

TClientProvider::TClientProvider(const TOidcConfig& config, std::weak_ptr<ICoreFacility> facility, bool standalone)
    : TProviderBase(std::make_shared<TState>(config, std::move(facility), standalone))
{
}

} // namespace NYdb::inline Dev::NOidc::NPrivate
