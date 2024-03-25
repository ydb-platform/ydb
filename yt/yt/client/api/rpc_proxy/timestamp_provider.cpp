#include "timestamp_provider.h"
#include "api_service_proxy.h"
#include "connection_impl.h"

#include <yt/yt/client/transaction_client/timestamp_provider_base.h>

namespace NYT::NApi::NRpcProxy {

using namespace NTransactionClient;
using namespace NObjectClient;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TTimestampProvider
    : public TTimestampProviderBase
{
public:
    TTimestampProvider(
        IChannelPtr channel,
        TDuration rpcTimeout,
        TDuration latestTimestampUpdatePeriod,
        TCellTag clockClusterTag)
        : TTimestampProviderBase(latestTimestampUpdatePeriod)
        , Channel_(std::move(channel))
        , RpcTimeout_(rpcTimeout)
        , ClockClusterTag_(clockClusterTag)
    { }

private:
    const IChannelPtr Channel_;
    const TDuration RpcTimeout_;
    const TCellTag ClockClusterTag_;

    TFuture<NTransactionClient::TTimestamp> DoGenerateTimestamps(int count, TCellTag clockClusterTag) override
    {
        TApiServiceProxy proxy(Channel_);

        auto req = proxy.GenerateTimestamps();
        req->SetTimeout(RpcTimeout_);
        req->set_count(count);
        if (clockClusterTag == InvalidCellTag) {
            clockClusterTag = ClockClusterTag_;
        }
        if (clockClusterTag != InvalidCellTag) {
            req->set_clock_cluster_tag(ToProto<int>(clockClusterTag));
        }

        return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspGenerateTimestampsPtr& rsp) {
            return rsp->timestamp();
        }));
    }
};

////////////////////////////////////////////////////////////////////////////////

NTransactionClient::ITimestampProviderPtr CreateTimestampProvider(
    IChannelPtr channel,
    TDuration rpcTimeout,
    TDuration latestTimestampUpdatePeriod,
    TCellTag clockClusterTag)
{
    return New<TTimestampProvider>(
        std::move(channel),
        rpcTimeout,
        latestTimestampUpdatePeriod,
        clockClusterTag);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
