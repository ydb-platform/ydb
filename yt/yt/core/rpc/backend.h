#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

#include <any>
#include <vector>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

struct IBackend
{
    virtual ~IBackend() = default;

    virtual TStringBuf GetProtocol() = 0;

    virtual void RegisterClientConfigField(NYTree::TYsonStructRegistrar<TMultiProtocolClientConfig> registrar) = 0;
    virtual IChannelFactoryPtr CreateChannelFactory(const std::any& config) = 0;

    virtual void RegisterServerConfigField(NYTree::TYsonStructRegistrar<TMultiProtocolServerConfig> registrar) = 0;
    virtual IServerPtr CreateServer(const std::any& config) = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TBackendRegistry
{
public:
    static std::vector<IBackend*> GetBackends();
    static IBackend* FindBackend(TStringBuf protocol);
    static void RegisterBackend(IBackend* backend);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
