#include "multi_protocol_channel_factory.h"

#include "backend.h"
#include "config.h"
#include "endpoint_address.h"

#include <library/cpp/yt/threading/atomic_object.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

class TMultiProtocolChannelFactory
    : public IChannelFactory
{
public:
    explicit TMultiProtocolChannelFactory(TMultiProtocolClientConfigPtr config)
        : Config_(std::move(config))
    { }

    IChannelPtr CreateChannel(const std::string& address) final
    {
        auto parsedAddress = ParseEndpointAddress(address);

        auto* backend = TBackendRegistry::FindBackend(parsedAddress.Protocol);
        if (!backend) {
            THROW_ERROR_EXCEPTION("Unsupported RPC protocol %Qv in address %Qv",
                parsedAddress.Protocol,
                address);
        }

        auto config = Config_->GetUntypedConfig(parsedAddress.Protocol);

        auto factory = GetOrCreateChannelFactory(backend, config);

        return factory->CreateChannel(std::string(parsedAddress.Address));
    }

private:
    const TMultiProtocolClientConfigPtr Config_;

    NThreading::TAtomicObject<THashMap<IBackend*, IChannelFactoryPtr>> BackendToFactory_;

    IChannelFactoryPtr GetOrCreateChannelFactory(IBackend* backend, const std::any& config)
    {
        if (auto factory = BackendToFactory_.Read([&] (const auto& backendToFactory) {
            return GetOrDefault(backendToFactory, backend);
        }))
        {
            return factory;
        }

        return BackendToFactory_.Transform([&] (auto& backendToFactory) {
            auto it = backendToFactory.find(backend);
            if (it == backendToFactory.end()) {
                auto factory = backend->CreateChannelFactory(config);
                it = backendToFactory.emplace(backend, std::move(factory)).first;
            }
            return it->second;
        });
    }
};

////////////////////////////////////////////////////////////////////////////////

IChannelFactoryPtr CreateMultiProtocolChannelFactory(TMultiProtocolClientConfigPtr config)
{
    return New<TMultiProtocolChannelFactory>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
