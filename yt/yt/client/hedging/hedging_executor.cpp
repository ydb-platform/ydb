#include "hedging_executor.h"

#include "logger.h"

#include <yt/yt/core/logging/log.h>


namespace NYT::NClient::NHedging::NRpc {

////////////////////////////////////////////////////////////////////////////////

THedgingExecutor::THedgingExecutor(const THedgingClientOptions& options, const IPenaltyProviderPtr& penaltyProvider)
    : BanPenalty_(NProfiling::DurationToCpuDuration(options.BanPenalty))
    , BanDuration_(NProfiling::DurationToCpuDuration(options.BanDuration))
    , PenaltyProvider_(penaltyProvider)
{
    Y_ENSURE(!options.Clients.empty(), "Clients should not be empty!");
    for (const auto& clientOptions : options.Clients) {
        Y_ENSURE(clientOptions.Client, "Client pointer should be valid!");
        Clients_.emplace_back(
            clientOptions.Client,
            NProfiling::DurationToCpuDuration(clientOptions.InitialPenalty),
            clientOptions.Counter ? clientOptions.Counter : New<TCounter>(clientOptions.Client->GetConnection()->GetClusterId()),
            clientOptions.ClusterName);
    }
}

NApi::IConnectionPtr THedgingExecutor::GetConnection()
{
    return Clients_[0].Client->GetConnection();
}

void THedgingExecutor::OnFinishRequest(
    size_t clientIndex,
    TDuration effectivePenalty,
    NProfiling::TCpuDuration adaptivePenalty,
    NProfiling::TCpuDuration externalPenalty,
    NProfiling::TCpuInstant start,
    const TError& error)
{
    auto& clientInfo = Clients_[clientIndex];
    if (error.IsOK()) {
        if (adaptivePenalty) {
            TGuard guard(SpinLock_);
            clientInfo.BanUntil = Max<NProfiling::TCpuInstant>();
            clientInfo.AdaptivePenalty = 0;
        }
        clientInfo.Counter->SuccessRequestCount.Increment();
        clientInfo.Counter->RequestDuration.Record(NProfiling::CpuDurationToDuration(NProfiling::GetCpuInstant() - start));
    } else if (effectivePenalty && (error.GetCode() == EErrorCode::Canceled || error.GetCode() == EErrorCode::FutureCombinerShortcut)) {
        clientInfo.Counter->CancelRequestCount.Increment();
    } else {
        with_lock (SpinLock_) {
            clientInfo.BanUntil = NProfiling::GetCpuInstant() + BanDuration_;
            clientInfo.AdaptivePenalty += BanPenalty_;
        }
        clientInfo.Counter->ErrorRequestCount.Increment();
        YT_LOG_WARNING("client#%v failed with error %v", clientIndex, error);
    }
    clientInfo.Counter->EffectivePenalty.Update(effectivePenalty);
    clientInfo.Counter->ExternalPenalty.Update(NProfiling::CpuDurationToDuration(externalPenalty));
}

THedgingExecutor::TEntry::TEntry(
    NApi::IClientPtr client,
    NProfiling::TCpuDuration initialPenalty,
    TCounterPtr counter,
    const std::string& clusterName)
    : Client(std::move(client))
    , ClusterName(clusterName)
    , AdaptivePenalty(0)
    , InitialPenalty(initialPenalty)
    , ExternalPenalty(0)
    , BanUntil(Max<NProfiling::TCpuInstant>())
    , Counter(std::move(counter))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NHedging::NRpc
