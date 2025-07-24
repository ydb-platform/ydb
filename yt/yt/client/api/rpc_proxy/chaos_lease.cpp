#include "chaos_lease.h"

#include "client_impl.h"
#include "private.h"

#include <yt/yt/client/api/chaos_lease_base.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TChaosLease
    : public TChaosLeaseBase
{
public:
    TChaosLease(
        IClientPtr client,
        NRpc::IChannelPtr channel,
        NChaosClient::TChaosLeaseId chaosLeaseId,
        TDuration timeout,
        bool pingAncestors,
        std::optional<TDuration> pingPeriod)
        : TChaosLeaseBase(
            std::move(client),
            std::move(channel),
            chaosLeaseId,
            timeout,
            pingAncestors,
            pingPeriod,
            RpcProxyClientLogger())
        , Proxy_(Channel_)
    { }

private:
    TApiServiceProxy Proxy_;

    TFuture<void> DoPing(const TPrerequisitePingOptions& /*options*/) override
    {
        auto req = Proxy_.PingChaosLease();
        // TODO(gryzlov-ad): Put correct timeout here.
        req->SetTimeout(NRpc::DefaultRpcRequestTimeout);

        ToProto(req->mutable_chaos_lease_id(), GetId());
        req->set_ping_ancestors(PingAncestors_);

        return req->Invoke().AsVoid();
    }
};

////////////////////////////////////////////////////////////////////////////////

IPrerequisitePtr CreateChaosLease(
    IClientPtr client,
    NRpc::IChannelPtr channel,
    NChaosClient::TChaosLeaseId chaosLeaseId,
    TDuration timeout,
    bool pingAncestors,
    std::optional<TDuration> pingPeriod)
{
    return New<TChaosLease>(
        std::move(client),
        std::move(channel),
        chaosLeaseId,
        timeout,
        pingAncestors,
        pingPeriod);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
