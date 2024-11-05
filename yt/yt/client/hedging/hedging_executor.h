#pragma once

#include "counter.h"
#include "penalty_provider.h"
#include "public.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <util/datetime/base.h>

#include <util/generic/string.h>

#include <vector>


namespace NYT::NClient::NHedging::NRpc {

////////////////////////////////////////////////////////////////////////////////

class THedgingExecutor final
{
public:
    struct TNode
    {
        NApi::IClientPtr Client;
        TCounterPtr Counter;
        std::string ClusterName;
        TDuration InitialPenalty;
    };

    THedgingExecutor(
        const std::vector<TNode>& nodes,
        TDuration banPenalty,
        TDuration banDuration,
        const IPenaltyProviderPtr& penaltyProvider);

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

        auto minInitialPenalty = TDuration::Max();
        for (auto& node : nodes) {
            node.ExternalPenalty = PenaltyProvider_->Get(node.ClusterName);
            auto currentInitialPenalty = node.InitialPenalty + node.AdaptivePenalty + node.ExternalPenalty;
            minInitialPenalty = std::min(minInitialPenalty, currentInitialPenalty);
        }

        std::vector<TFuture<T>> futures;
        futures.reserve(nodes.size());
        for (int i = 0; i != std::ssize(nodes); ++i) {
            const auto& node = nodes[i];
            auto penalty = node.InitialPenalty + node.AdaptivePenalty + node.ExternalPenalty - minInitialPenalty;
            if (penalty) {
                auto delayedFuture = NConcurrency::TDelayedExecutor::MakeDelayed(
                    penalty,
                    NYT::NRpc::TDispatcher::Get()->GetHeavyInvoker());
                futures.push_back(delayedFuture.Apply(BIND(callback, node.Client)));
            } else {
                futures.push_back(callback(node.Client));
            }
            futures.back().Subscribe(BIND(
                &THedgingExecutor::OnFinishRequest,
                MakeWeak(this),
                i,
                penalty,
                node.AdaptivePenalty,
                node.ExternalPenalty,
                now));
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

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);

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
