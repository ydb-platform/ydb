#include "peer_discovery.h"

#include "client.h"

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NRpc {

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

class TDefaultPeerDiscovery
    : public IPeerDiscovery
{
public:
    explicit TDefaultPeerDiscovery(TDiscoverRequestHook hook)
        : Hook_(std::move(hook))
    { }

    TFuture<TPeerDiscoveryResponse> Discover(
        IChannelPtr channel,
        TDuration timeout,
        TDuration replyDelay,
        const std::string& serviceName) override
    {
        auto serviceDescriptor = TServiceDescriptor(serviceName)
            .SetProtocolVersion(GenericProtocolVersion);
        TGenericProxy proxy(std::move(channel), serviceDescriptor);
        auto req = proxy.Discover();
        if (Hook_) {
            Hook_(req.Get());
        }
        req->SetTimeout(timeout);
        req->set_reply_delay(replyDelay.GetValue());
        return req->Invoke().Apply(BIND(&TDefaultPeerDiscovery::ConvertResponse));
    }

private:
    const TDiscoverRequestHook Hook_;

    static TPeerDiscoveryResponse ConvertResponse(const TIntrusivePtr<TTypedClientResponse<NProto::TRspDiscover>>& rsp)
    {
        return TPeerDiscoveryResponse{
            .IsUp = rsp->up(),
            .Addresses = FromProto<std::vector<TString>>(rsp->suggested_addresses()),
        };
    }
};

////////////////////////////////////////////////////////////////////////////////

IPeerDiscoveryPtr CreateDefaultPeerDiscovery(TDiscoverRequestHook hook)
{
    return New<TDefaultPeerDiscovery>(std::move(hook));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
