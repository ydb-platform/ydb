#include "helper.h"

#include <yt/yt/client/hedging/counter.h>
#include <yt/yt/client/hedging/hedging.h>
#include <yt/yt/client/hedging/hedging_executor.h>

#include <vector>

namespace NYT::NClient::NHedging::NRpc::NTest {

NApi::IClientPtr CreateTestHedgingClient(
    std::vector<NApi::IClientPtr> clients,
    std::vector<TCounterPtr> counters,
    std::vector<TDuration> initialPenalties,
    const IPenaltyProviderPtr& penaltyProvider,
    TDuration banPenalty,
    TDuration banDuration)
{
    YT_VERIFY(clients.size() == counters.size());
    YT_VERIFY(clients.size() == initialPenalties.size());

    std::vector<THedgingExecutor::TNode> executorNodes;
    executorNodes.reserve(clients.size());
    for (int i = 0; i != std::ssize(clients); ++i) {
        executorNodes.push_back({
            .Client = clients[i],
            .Counter = counters[i],
            .ClusterName = Format("cluster-%v", i),
            .InitialPenalty = initialPenalties[i],
        });
    }
    return CreateHedgingClient(
        New<THedgingExecutor>(executorNodes, banPenalty, banDuration, penaltyProvider));
}


} // NYT::NClient::NHedging::NTest
