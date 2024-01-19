#pragma once

#include "public.h"

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

using TRetryChecker = TCallback<bool(const TError&)>;

//! Constructs a channel that implements a simple retry policy.
/*!
 *  The channel determines if the request must be retried by calling
 *  #retryChecker (which is #NRpc::IsRetriableError by default).
 *
 *  The channel makes at most #TRetryingChannelConfig::RetryAttempts
 *  attempts totally spending at most #TRetryingChannelConfig::RetryTimeout time
 *  (if given).
 *
 *  A delay of #TRetryingChannelConfig::RetryBackoffTime is inserted
 *  between any pair of consequent attempts.
 */
IChannelPtr CreateRetryingChannel(
    TRetryingChannelConfigPtr config,
    IChannelPtr underlyingChannel,
    TRetryChecker retryChecker = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
