#include "throttling_channel.h"
#include "channel_detail.h"
#include "client.h"
#include "config.h"

#include <yt/yt/core/concurrency/throughput_throttler.h>
#include <yt/yt/core/concurrency/config.h>

namespace NYT::NRpc {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TThrottlingChannel
    : public TChannelWrapper
    , public IThrottlingChannel
{
public:
    TThrottlingChannel(
        TThrottlingChannelConfigPtr config,
        IChannelPtr underlyingChannel,
        NProfiling::TProfiler profiler)
        : TChannelWrapper(std::move(underlyingChannel))
        , Config_(std::move(config))
        , Throttler_(CreateReconfigurableThroughputThrottler(
            TThroughputThrottlerConfig::Create(Config_->RateLimit),
            /*logger*/ {},
            std::move(profiler)))
    { }

    IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        const TSendOptions& options) override
    {
        auto sendTime = TInstant::Now();
        auto timeout = options.Timeout;
        auto requestControlThunk = New<TClientRequestControlThunk>();
        Throttler_->Throttle(1)
            .WithTimeout(timeout)
            .Subscribe(BIND([=, this, this_ = MakeStrong(this)] (const TError& error) {
                if (!error.IsOK()) {
                    responseHandler->HandleError(TError("Error throttling RPC request")
                        << error);
                    return;
                }

                auto adjustedOptions = options;
                auto now = TInstant::Now();
                adjustedOptions.Timeout = timeout
                    ? std::make_optional(*timeout - (now - sendTime))
                    : std::nullopt;

                auto requestControl = UnderlyingChannel_->Send(
                    std::move(request),
                    std::move(responseHandler),
                    adjustedOptions);
                requestControlThunk->SetUnderlying(std::move(requestControl));
            }));
        return requestControlThunk;
    }

    void Reconfigure(const TThrottlingChannelDynamicConfigPtr& config) override
    {
        Throttler_->Reconfigure(TThroughputThrottlerConfig::Create(
            config->RateLimit.value_or(Config_->RateLimit)));
    }

private:
    const TThrottlingChannelConfigPtr Config_;
    const IReconfigurableThroughputThrottlerPtr Throttler_;
};

////////////////////////////////////////////////////////////////////////////////

IThrottlingChannelPtr CreateThrottlingChannel(
    TThrottlingChannelConfigPtr config,
    IChannelPtr underlyingChannel,
    NProfiling::TProfiler profiler)
{
    YT_VERIFY(config);
    YT_VERIFY(underlyingChannel);

    return New<TThrottlingChannel>(
        std::move(config),
        std::move(underlyingChannel),
        std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
