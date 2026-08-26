#include "channel.h"
#include "config.h"
#include "server.h"

#include <yt/yt/core/rpc/backend_detail.h>
#include <yt/yt/core/rpc/endpoint_address.h>

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/local_address.h>

namespace NYT::NRpc::NHttp {
namespace {

////////////////////////////////////////////////////////////////////////////////

using TClientConfigPtr = TIntrusivePtr<TClientConfig>;
using TServerConfigPtr = TIntrusivePtr<TServerConfig>;

class THttpBackend
    : public TBackendBase<TClientConfig, TServerConfig>
{
public:
    TStringBuf GetProtocol() final
    {
        return "http"_sb;
    }

protected:
    std::string DoBuildLocalEndpointAddress(const TServerConfigPtr& config) final
    {
        return FormatEndpointAddress({
            .Protocol = GetProtocol(),
            .Address = NNet::BuildServiceAddress(NNet::GetLocalHostName(), config->Port),
        });
    }

    IChannelFactoryPtr DoCreateChannelFactory(const TClientConfigPtr& config) final
    {
        return CreateHttpChannelFactory(config);
    }

    IServerPtr DoCreateServer(const TServerConfigPtr& config) final
    {
        return NHttp::CreateServer(config);
    }
};

YT_DEFINE_RPC_BACKEND(THttpBackend);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NRpc::NHttp
