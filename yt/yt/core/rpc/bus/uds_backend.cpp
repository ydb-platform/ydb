#include "channel.h"
#include "server.h"

#include <yt/yt/core/rpc/backend_detail.h>

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/server.h>

namespace NYT::NRpc::NBus {
namespace {

////////////////////////////////////////////////////////////////////////////////

using TClientConfig = NYT::NBus::NTcp::TBusConfig;
using TClientConfigPtr = TIntrusivePtr<TClientConfig>;

using TServerConfig = NYT::NBus::NTcp::TBusServerConfig;
using TServerConfigPtr = TIntrusivePtr<TServerConfig>;

class TUdsBackend
    : public TBackendBase<TClientConfig, TServerConfig>
{
public:
    TStringBuf GetProtocol() final
    {
        return "yt-uds"_sb;
    }

protected:
    IChannelFactoryPtr DoCreateChannelFactory(const TClientConfigPtr& config) final
    {
        return CreateUdsBusChannelFactory(config);
    }

    IServerPtr DoCreateServer(const TServerConfigPtr& config) final
    {
        auto busServer = NYT::NBus::NTcp::CreateBusServer(config);
        return CreateBusServer(std::move(busServer));
    }
};

YT_DEFINE_RPC_BACKEND(TUdsBackend);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NRpc::NBus
