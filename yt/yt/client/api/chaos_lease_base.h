#pragma once

#include "private.h"

#include "prerequisite.h"

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/client/api/public.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

class TChaosLeaseBase
    : public virtual IPrerequisite
{
public:
    TChaosLeaseBase(
        IClientPtr client,
        NRpc::IChannelPtr channel,
        NChaosClient::TChaosLeaseId id,
        TDuration timeout,
        bool pingAncestors,
        std::optional<TDuration> pingPeriod,
        const NLogging::TLogger& logger);

    NApi::IClientPtr GetClient() const override;
    NPrerequisiteClient::TPrerequisiteId GetId() const override;
    TDuration GetTimeout() const override;

    TFuture<void> Ping(const TPrerequisitePingOptions& options = {}) override;
    TFuture<void> Abort(const TPrerequisiteAbortOptions& options = {}) override;

    void SubscribeAborted(const TAbortedHandler& handler) override;
    void UnsubscribeAborted(const TAbortedHandler& handler) override;

protected:
    const IClientPtr Client_;
    const NRpc::IChannelPtr Channel_;
    const NChaosClient::TChaosLeaseId Id_;
    const TDuration Timeout_;
    const bool PingAncestors_;
    const std::optional<TDuration> PingPeriod_;

    const NLogging::TLogger Logger;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    TPromise<void> AbortPromise_;

    TSingleShotCallbackList<TAbortedHandlerSignature> Aborted_;

    virtual TFuture<void> DoPing(const TPrerequisitePingOptions& options) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
