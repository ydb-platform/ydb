#include "hedging_executor.h"

#include "private.h"

#include <yt/yt/core/logging/log.h>


namespace NYT::NClient::NHedging::NRpc {

////////////////////////////////////////////////////////////////////////////////

THedgingExecutor::THedgingExecutor(
    const std::vector<TNode>& nodes,
    TDuration banPenalty,
    TDuration banDuration,
    const IPenaltyProviderPtr& penaltyProvider)
    : BanPenalty_(banPenalty)
    , BanDuration_(banDuration)
    , PenaltyProvider_(penaltyProvider)
{
    Nodes_.reserve(nodes.size());
    for (const auto& node : nodes) {
        Nodes_.push_back({node});
    }
}

NApi::IClientPtr THedgingExecutor::GetClient(int index)
{
    return Nodes_.at(index).Client;
}

void THedgingExecutor::OnFinishRequest(
    int index,
    TDuration effectivePenalty,
    TDuration adaptivePenalty,
    TDuration externalPenalty,
    TInstant start,
    const TError& error)
{
    auto& node = Nodes_[index];
    if (error.IsOK()) {
        if (adaptivePenalty) {
            TGuard guard(SpinLock_);
            node.BanUntil = TInstant::Max();
            node.AdaptivePenalty = TDuration::Zero();
        }
        node.Counter->SuccessRequestCount.Increment();
        node.Counter->RequestDuration.Record(TInstant::Now() - start);
    } else if (effectivePenalty && (error.GetCode() == EErrorCode::Canceled || error.GetCode() == EErrorCode::FutureCombinerShortcut)) {
        node.Counter->CancelRequestCount.Increment();
    } else {
        auto banUntil = TInstant::Now() + BanDuration_;
        {
            TGuard guard(SpinLock_);
            node.BanUntil = banUntil;
            node.AdaptivePenalty += BanPenalty_;
        }
        node.Counter->ErrorRequestCount.Increment();
        YT_LOG_WARNING(error, "Cluster banned (ClusterName: %v, Deadline: %v)",
            node.ClusterName,
            banUntil);
    }
    node.Counter->EffectivePenalty.Update(effectivePenalty);
    node.Counter->ExternalPenalty.Update(externalPenalty);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NHedging::NRpc
