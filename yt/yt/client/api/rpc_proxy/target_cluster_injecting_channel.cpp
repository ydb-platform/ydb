#include "target_cluster_injecting_channel.h"

#include <yt/yt/core/rpc/channel_detail.h>
#include <yt/yt/core/rpc/client.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TTargetClusterInjectingChannel
    : public TChannelWrapper
{
public:
    TTargetClusterInjectingChannel(IChannelPtr underlying, std::string cluster)
        : TChannelWrapper(std::move(underlying))
        , Cluster_(std::move(cluster))
    { }

    IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        const TSendOptions& options) override
    {
        auto* ext = request->Header().MutableExtension(NRpc::NProto::TMultiproxyTargetExt::multiproxy_target_ext);
        ext->set_cluster(Cluster_);
        return TChannelWrapper::Send(
            std::move(request),
            std::move(responseHandler),
            options);
    }

private:
    const std::string Cluster_;
};

IChannelPtr CreateTargetClusterInjectingChannel(
    IChannelPtr underlying,
    std::optional<std::string> cluster)
{
    if (cluster) {
        return New<TTargetClusterInjectingChannel>(std::move(underlying), std::move(*cluster));
    } else {
        return underlying;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
