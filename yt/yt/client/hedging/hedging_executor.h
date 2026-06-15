#pragma once

#include "counter.h"
#include "penalty_provider.h"
#include "public.h"

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/helpers.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <util/datetime/base.h>

#include <util/generic/string.h>

#include <util/random/random.h>

#include <vector>

namespace NYT::NClient::NHedging::NRpc {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ERequestType,
    ((Primary)       (0))
    ((Hedging)       (1))
);

DECLARE_REFCOUNTED_CLASS(THedgingRatioCounter)

class THedgingRatioCounter final
{
public:
    THedgingRatioCounter(size_t bucketCount, TDuration shiftPeriod);

    void AddRequest(ERequestType requestType);

    double GetRatio();

private:
    void DoShift();

    size_t Head_ = 0;
    std::vector<ui32> PrimaryRequests_;
    std::vector<ui32> HedgingRequests_;
    TDuration ShiftPeriod_;
    TInstant LastShift_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
};

DEFINE_REFCOUNTED_TYPE(THedgingRatioCounter)

class THedgingExecutor final
{
public:
    struct TNode
    {
        NApi::IClientPtr Client;
        TCounterPtr Counter;
        std::string ClusterName;
        TDuration InitialPenalty;
        NApi::EClientPriority ClientPriority = NApi::EClientPriority::Undefined;
    };

    THedgingExecutor(
        const std::vector<TNode>& nodes,
        TDuration banPenalty,
        TDuration banDuration,
        const IPenaltyProviderPtr& penaltyProvider,
        size_t ratioCounterBucketCount = 0,
        TDuration ratioCounterShiftPeriod = TDuration::Zero(),
        double hedgingRatioLimit = 1.0,
        const std::vector<TDuration>& hedgingRequestDelays = {},
        TDuration remoteDataCenterPenalty = TDuration::Zero(),
        std::optional<std::string> localDataCenter = std::nullopt,
        THedgingExecutorCountersPtr hedgingExecutorCounters = nullptr);

    NApi::IClientPtr GetClient(int index);

    template <typename T>
    TFuture<T> DoWithHedging(TCallback<TFuture<T>(NApi::IClientPtr)> callback)
    {
        auto now = TInstant::Now();
        auto nodes = [&] {
            TGuard guard(SpinLock_);
            for (auto& node : Nodes_) {
                if (node.BanUntil < now) {
                    node.AdaptivePenalty = TDuration::Zero();
                }
            }
            return Nodes_;
        }();

        ui32 remoteNodesCount = 0;
        if (!LocalDataCenter_.empty()) {
            for (const auto& node : nodes) {
                if (node.ClientPriority != NApi::EClientPriority::Local) {
                    ++remoteNodesCount;
                }
            }
        }

        auto minInitialPenalty = TDuration::Max();
        std::vector<std::pair<TDuration, int>> sortedNodePenalties;
        sortedNodePenalties.reserve(nodes.size());
        ui32 randomRemotePenalty = RandomNumber<ui32>();
        for (int nodeIndex = 0; nodeIndex < std::ssize(nodes); ++nodeIndex) {
            auto& node = nodes[nodeIndex];
            node.ExternalPenalty = PenaltyProvider_->Get(node.ClusterName);
            auto currentInitialPenalty = node.InitialPenalty + node.AdaptivePenalty + node.ExternalPenalty;
            minInitialPenalty = std::min(minInitialPenalty, currentInitialPenalty);

            auto additionalPenalty = node.AdaptivePenalty + node.ExternalPenalty;
            if (node.ClientPriority != NApi::EClientPriority::Local) {
                additionalPenalty += RemoteDataCenterPenalty_;
                if (remoteNodesCount) {
                    additionalPenalty += TDuration::MilliSeconds(randomRemotePenalty % remoteNodesCount);
                    ++randomRemotePenalty;
                }
            }
            sortedNodePenalties.emplace_back(additionalPenalty, nodeIndex);
        }
        std::sort(sortedNodePenalties.begin(), sortedNodePenalties.end());

        std::vector<TFuture<T>> futures;
        futures.reserve(nodes.size());

        double hedgingRequestRatio = HedgingRatioCounter_ ? HedgingRatioCounter_->GetRatio() : 0.0;
        if (HedgingExecutorCounters_) {
            HedgingExecutorCounters_->HedgingRequestRatio.Update(hedgingRequestRatio);
        }

        auto addFuture = [&] (const TNodeExtended& node, int nodeIndex, TDuration penalty) {
            auto runRequest = BIND([callback] (NApi::IClientPtr client, TCounterPtr counter, THedgingRatioCounterPtr hedgingRatioCounter, ERequestType requestType) {
                counter->TotalRequestCount.Increment();
                if (hedgingRatioCounter) {
                    hedgingRatioCounter->AddRequest(requestType);
                }
                return callback(client);
            });

            bool futureAdded = false;
            if (penalty) {
                if (hedgingRequestRatio <= HedgingRatioLimit_) {
                    auto delayedFuture = NConcurrency::TDelayedExecutor::MakeDelayed(
                        penalty,
                        NYT::NRpc::TDispatcher::Get()->GetHeavyInvoker());
                    futures.push_back(delayedFuture.Apply(BIND(runRequest, node.Client, node.Counter, HedgingRatioCounter_, ERequestType::Hedging)));
                    futureAdded = true;
                }
            } else {
                futures.push_back(runRequest(node.Client, node.Counter, HedgingRatioCounter_, ERequestType::Primary));
                futureAdded = true;
            }
            if (futureAdded) {
                futures.back().Subscribe(BIND(
                    &THedgingExecutor::OnFinishRequest,
                    MakeWeak(this),
                    nodeIndex,
                    penalty,
                    node.AdaptivePenalty,
                    node.ExternalPenalty,
                    now));
            }
        };

        if (!HedgingRequestDelays_.empty()) {
            size_t delayIndex = 0;
            for (const auto& [_, nodeIndex] : sortedNodePenalties) {
                if (delayIndex > HedgingRequestDelays_.size()) {
                    break;
                }
                addFuture(nodes[nodeIndex], nodeIndex, delayIndex > 0 ? HedgingRequestDelays_[delayIndex - 1] : TDuration::Zero());
                ++delayIndex;
            }
        } else {
            for (int nodeIndex = 0; nodeIndex < std::ssize(nodes); ++nodeIndex) {
                const auto& node = nodes[nodeIndex];
                addFuture(node, nodeIndex, node.InitialPenalty + node.AdaptivePenalty + node.ExternalPenalty - minInitialPenalty);
            }
        }

        return AnySucceeded(std::move(futures));
    }

private:
    struct TNodeExtended
        : TNode
    {
        TDuration AdaptivePenalty = TDuration::Zero();
        TDuration ExternalPenalty = TDuration::Zero();
        TInstant BanUntil = TInstant::Max();
    };

    std::vector<TNodeExtended> Nodes_;
    TDuration BanPenalty_;
    TDuration BanDuration_;
    IPenaltyProviderPtr PenaltyProvider_;
    std::vector<TDuration> HedgingRequestDelays_;
    THedgingRatioCounterPtr HedgingRatioCounter_;
    THedgingExecutorCountersPtr HedgingExecutorCounters_;
    TDuration RemoteDataCenterPenalty_;
    double HedgingRatioLimit_ = 1.0;
    std::string LocalDataCenter_;

    NConcurrency::TPeriodicExecutorPtr ClientPriorityUpdateExecutor_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);

    void UpdateClientPriorities();

    void OnFinishRequest(
        int index,
        TDuration effectivePenalty,
        TDuration adaptivePenalty,
        TDuration externalPenalty,
        TInstant start,
        const TError& error);
};

DEFINE_REFCOUNTED_TYPE(THedgingExecutor)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NHedging::NRpc
