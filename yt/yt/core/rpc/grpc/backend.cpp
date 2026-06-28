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
