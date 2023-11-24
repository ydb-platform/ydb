#include "peer_discovery.h"

#include "client.h"

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

class TDefaultPeerDiscovery
    : public IPeerDiscovery
{
public:
    TDefaultPeerDiscovery(TDiscoverRequestHook hook)
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
    TDiscoverRequestHook Hook_;

    static TPeerDiscoveryResponse ConvertResponse(const TIntrusivePtr<TTypedClientResponse<NProto::TRspDiscover>>& rsp)
    {
        TPeerDiscoveryResponse response;
        response.Addresses = NYT::FromProto<std::vector<TString>>(rsp->suggested_addresses());
        response.IsUp = rsp->up();
        return response;
    }
};

////////////////////////////////////////////////////////////////////////////////

IPeerDiscoveryPtr CreateDefaultPeerDiscovery(TDiscoverRequestHook hook)
{
    return New<TDefaultPeerDiscovery>(std::move(hook));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
