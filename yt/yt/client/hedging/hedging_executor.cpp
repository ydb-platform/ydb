#include "hedging_executor.h"

#include "private.h"

#include <yt/yt/client/api/helpers.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/rpc/dispatcher.h>

namespace NYT::NClient::NHedging::NRpc {

using namespace NApi;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

THedgingRatioCounter::THedgingRatioCounter(size_t bucketCount, TDuration shiftPeriod)
    : PrimaryRequests_(bucketCount, 0)
    , HedgingRequests_(bucketCount, 0)
    , ShiftPeriod_(shiftPeriod)
    , LastShift_(TInstant::Now())
{ }

void THedgingRatioCounter::AddRequest(ERequestType requestType)
{
    TGuard guard(SpinLock_);
    DoShift();
    if (requestType == ERequestType::Hedging) {
        ++HedgingRequests_[Head_];
    } else if (requestType == ERequestType::Primary) {
        ++PrimaryRequests_[Head_];
    }
}

double THedgingRatioCounter::GetRatio()
{
    TGuard guard(SpinLock_);
    DoShift();
    double allRequests = 0.0;
    double hedgingRequests = 0.0;
    for (size_t i = 0; i != PrimaryRequests_.size(); ++i) {
        allRequests += PrimaryRequests_[i] + HedgingRequests_[i];
        hedgingRequests += HedgingRequests_[i];
    }
    return allRequests > 0.0 ? hedgingRequests / allRequests : 0.0;
}

void THedgingRatioCounter::DoShift()
{
    auto now = TInstant::Now();
    if (LastShift_ + ShiftPeriod_ > now) {
        return;
    }
    int i = PrimaryRequests_.size();
    while (LastShift_ + ShiftPeriod_ < now && i-- > 0) {
        Head_ = (Head_ + 1) % PrimaryRequests_.size();
        PrimaryRequests_[Head_] = 0;
        HedgingRequests_[Head_] = 0;
        LastShift_ += ShiftPeriod_;
    }
    if (i == -1) {
        LastShift_ = now;
    }
}

THedgingExecutor::THedgingExecutor(
    const std::vector<TNode>& nodes,
    TDuration banPenalty,
    TDuration banDuration,
    const IPenaltyProviderPtr& penaltyProvider,
    size_t ratioCounterBucketCount,
    TDuration ratioCounterShiftPeriod,
    double hedgingRatioLimit,
    const std::vector<TDuration>& hedgingRequestDelays,
    TDuration remoteDataCenterPenalty,
    std::optional<std::string> localDataCenter,
    THedgingExecutorCountersPtr hedgingExecutorCounters)
    : BanPenalty_(banPenalty)
    , BanDuration_(banDuration)
    , PenaltyProvider_(penaltyProvider)
    , HedgingRequestDelays_(hedgingRequestDelays)
    , HedgingExecutorCounters_(std::move(hedgingExecutorCounters))
    , RemoteDataCenterPenalty_(remoteDataCenterPenalty)
    , HedgingRatioLimit_(hedgingRatioLimit)
    , LocalDataCenter_(localDataCenter.value_or(""))
{
    if (ratioCounterBucketCount && ratioCounterShiftPeriod) {
        HedgingRatioCounter_ = New<THedgingRatioCounter>(ratioCounterBucketCount, ratioCounterShiftPeriod);
    }
    Nodes_.reserve(nodes.size());
    for (const auto& node : nodes) {
        Nodes_.push_back({node});
    }

    if (!LocalDataCenter_.empty()) {
        ClientPriorityUpdateExecutor_ = New<TPeriodicExecutor>(
            NYT::NRpc::TDispatcher::Get()->GetLightInvoker(),
            BIND(&THedgingExecutor::UpdateClientPriorities, MakeWeak(this)),
            TDuration::Seconds(5));
        ClientPriorityUpdateExecutor_->Start();
    }
}

IClientPtr THedgingExecutor::GetClient(int index)
{
    return Nodes_.at(index).Client;
}

void THedgingExecutor::UpdateClientPriorities()
{
    bool allResolved = true;
    for (int nodeIndex = 0; nodeIndex < std::ssize(Nodes_); ++nodeIndex) {
        IClientPtr client;
        {
            TGuard guard(SpinLock_);
            if (Nodes_[nodeIndex].ClientPriority != EClientPriority::Undefined) {
                continue;
            }
            client = Nodes_[nodeIndex].Client;
        }

        auto clientDataCenter = WaitFor(GetDataCenterByClient(client));
        if (!clientDataCenter.IsOK() || !clientDataCenter.Value()) {
            allResolved = false;
            continue;
        }

        auto priority = *clientDataCenter.Value() == LocalDataCenter_
            ? EClientPriority::Local
            : EClientPriority::Remote;
        {
            TGuard guard(SpinLock_);
            Nodes_[nodeIndex].ClientPriority = priority;
        }
    }

    if (allResolved) {
        YT_UNUSED_FUTURE(ClientPriorityUpdateExecutor_->Stop());
    }
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
    } else if (effectivePenalty && (error.GetCode() == NYT::EErrorCode::Canceled || error.GetCode() == NYT::EErrorCode::FutureCombinerShortcut)) {
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
