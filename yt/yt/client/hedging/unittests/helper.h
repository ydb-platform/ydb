#pragma once

#include <yt/yt/client/hedging/public.h>

#include <util/datetime/base.h>

#include <vector>

namespace NYT::NClient::NHedging::NRpc::NTest {

NApi::IClientPtr CreateTestHedgingClient(
    std::vector<NApi::IClientPtr> clients,
    std::vector<TCounterPtr> counters,
    std::vector<TDuration> initialPenalties,
    const IPenaltyProviderPtr& penaltyProvider,
    TDuration banPenalty,
    TDuration banDuration);

} // NYT::NClient::NHedging::NTest
