#include "multi_protocol_server.h"

#include "backend.h"
#include "config.h"
#include "server.h"
#include "service.h"

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

//! Aggregates one underlying server per registered backend, fanning out all
//! operations so that the services are exposed over every configured transport.
class TMultiProtocolServer
    : public IServer
{
public:
    explicit TMultiProtocolServer(const TMultiProtocolServerConfigPtr& config)
    {
        for (auto* backend : TBackendRegistry::GetBackends()) {
            if (auto backendConfig = config->FindUntypedConfig(backend->GetProtocol()); backendConfig.has_value()) {
                Servers_.push_back(backend->CreateServer(backendConfig));
            }
        }
    }

    void RegisterService(IServicePtr service) final
    {
        for (const auto& server : Servers_) {
            server->RegisterService(service);
        }
    }

    bool UnregisterService(IServicePtr service) final
    {
        bool unregistered = false;
        for (const auto& server : Servers_) {
            unregistered |= server->UnregisterService(service);
        }
        return unregistered;
    }

    IServicePtr FindService(const TServiceId& serviceId) const final
    {
        for (const auto& server : Servers_) {
            if (auto service = server->FindService(serviceId)) {
                return service;
            }
        }
        return nullptr;
    }

    IServicePtr GetServiceOrThrow(const TServiceId& serviceId) const final
    {
        if (auto service = FindService(serviceId)) {
            return service;
        }
        THROW_ERROR_EXCEPTION(NRpc::EErrorCode::NoSuchService,
            "Service is not registered")
            << TErrorAttribute("service", serviceId.ServiceName)
            << TErrorAttribute("realm_id", serviceId.RealmId);
    }

    void Configure(const TServerConfigPtr& config) final
    {
        for (const auto& server : Servers_) {
            server->Configure(config);
        }
    }

    void OnDynamicConfigChanged(const TServerDynamicConfigPtr& config) final
    {
        for (const auto& server : Servers_) {
            server->OnDynamicConfigChanged(config);
        }
    }

    void Start() final
    {
        for (const auto& server : Servers_) {
            server->Start();
        }
    }

    TFuture<void> Stop(bool graceful) final
    {
        std::vector<TFuture<void>> futures;
        futures.reserve(Servers_.size());
        for (const auto& server : Servers_) {
            futures.push_back(server->Stop(graceful));
        }
        return AllSucceeded(std::move(futures));
    }

private:
    std::vector<IServerPtr> Servers_;
};

////////////////////////////////////////////////////////////////////////////////

IServerPtr CreateMultiProtocolServer(TMultiProtocolServerConfigPtr config)
{
    return New<TMultiProtocolServer>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
