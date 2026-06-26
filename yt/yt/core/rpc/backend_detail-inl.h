#ifndef BACKEND_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include backend_detail.h"
// For the sake of sane code completion.
#include "backend_detail.h"
#endif

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

template <class TClientConfig, class TServerConfig>
void TBackendBase<TClientConfig, TServerConfig>::RegisterClientConfigField(NYTree::TYsonStructRegistrar<TMultiProtocolClientConfig> registrar)
{
    registrar
        .template ParameterWithUniversalAccessor<TIntrusivePtr<TClientConfig>>(
            std::string(GetProtocol()),
            [protocol = GetProtocol()] (TMultiProtocolClientConfig* config) -> auto& {
                return *config->template MutableTypedConfig<TClientConfig>(protocol);
            })
        .DefaultNew();
}

template <class TClientConfig, class TServerConfig>
IChannelFactoryPtr TBackendBase<TClientConfig, TServerConfig>::CreateChannelFactory(const std::any& config)
{
    return DoCreateChannelFactory(std::any_cast<TIntrusivePtr<TClientConfig>>(config));
}

template <class TClientConfig, class TServerConfig>
void TBackendBase<TClientConfig, TServerConfig>::RegisterServerConfigField(NYTree::TYsonStructRegistrar<TMultiProtocolServerConfig> registrar)
{
    // Unlike the client side, server backends must be configured explicitly:
    // only protocols present in the config get an actual server. Hence no DefaultNew.
    registrar
        .template ParameterWithUniversalAccessor<TIntrusivePtr<TServerConfig>>(
            std::string(GetProtocol()),
            [protocol = GetProtocol()] (TMultiProtocolServerConfig* config) -> auto& {
                return *config->template MutableTypedConfig<TServerConfig>(protocol);
            })
        .Default();
}

template <class TClientConfig, class TServerConfig>
IServerPtr TBackendBase<TClientConfig, TServerConfig>::CreateServer(const std::any& config)
{
    return DoCreateServer(std::any_cast<TIntrusivePtr<TServerConfig>>(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
