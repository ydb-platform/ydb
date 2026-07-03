#include "channel.h"
#include "config.h"
#include "server.h"

#include <yt/yt/core/rpc/backend_detail.h>

namespace NYT::NRpc::NGrpc {
namespace {

////////////////////////////////////////////////////////////////////////////////

using TClientConfig = TChannelFactoryConfig;
using TClientConfigPtr = TIntrusivePtr<TClientConfig>;

using TServerConfigPtr = TIntrusivePtr<TServerConfig>;

class TGrpcBackend
    : public TBackendBase<TClientConfig, TServerConfig>
{
public:
    TStringBuf GetProtocol() final
    {
        return "grpc"_sb;
    }

protected:
    std::string DoBuildLocalEndpointAddress(const TServerConfigPtr& /*config*/) final
    {
        // gRPC binds a list of (often wildcard) addresses with no single routable
        // local host, so there is no meaningful local endpoint to advertise.
        THROW_ERROR_EXCEPTION("RPC backend %Qv does not support local endpoint address resolution",
            GetProtocol());
    }

    IChannelFactoryPtr DoCreateChannelFactory(const TClientConfigPtr& config) final
    {
        return CreateGrpcChannelFactory(config);
    }

    IServerPtr DoCreateServer(const TServerConfigPtr& config) final
    {
        return NGrpc::CreateServer(config);
    }
};

YT_DEFINE_RPC_BACKEND(TGrpcBackend);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NRpc::NGrpc
