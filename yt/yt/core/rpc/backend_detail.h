#pragma once

#include "backend.h"
#include "config.h"
#include "dispatcher.h"

#include <library/cpp/yt/misc/static_initializer.h>

#include <library/cpp/yt/memory/leaky_singleton.h>

#include <library/cpp/yt/assert/assert.h>

#include <any>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

template <class TClientConfig, class TServerConfig>
class TBackendBase
    : public IBackend
{
public:
    std::string BuildLocalEndpointAddress(const std::any& config) final;

    void RegisterClientConfigField(NYTree::TYsonStructRegistrar<TMultiProtocolClientConfig> registrar) final;
    IChannelFactoryPtr CreateChannelFactory(const std::any& config) final;

    void RegisterServerConfigField(NYTree::TYsonStructRegistrar<TMultiProtocolServerConfig> registrar) final;
    IServerPtr CreateServer(const std::any& config) final;

protected:
    virtual std::string DoBuildLocalEndpointAddress(const TIntrusivePtr<TServerConfig>& config) = 0;
    virtual IChannelFactoryPtr DoCreateChannelFactory(const TIntrusivePtr<TClientConfig>& config) = 0;
    virtual IServerPtr DoCreateServer(const TIntrusivePtr<TServerConfig>& config) = 0;
};

////////////////////////////////////////////////////////////////////////////////

#define YT_DEFINE_RPC_BACKEND(...)                        \
    YT_STATIC_INITIALIZER({                               \
        ::NYT::NRpc::TBackendRegistry::RegisterBackend(   \
            ::NYT::LeakySingleton<__VA_ARGS__>());        \
    })

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

#define BACKEND_DETAIL_INL_H_
#include "backend_detail-inl.h"
#undef BACKEND_DETAIL_INL_H_
