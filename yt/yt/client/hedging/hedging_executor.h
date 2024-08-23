#pragma once

#include "config.h"
#include "counter.h"
#include "penalty_provider.h"
#include "public.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <library/cpp/iterator/enumerate.h>

#include <util/datetime/base.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <util/system/spinlock.h>


namespace NYT::NClient::NHedging::NRpc {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(THedgingExecutor)

class THedgingExecutor final
{
public:
    THedgingExecutor(const THedgingClientOptions& options, const IPenaltyProviderPtr& penaltyProvider);

    NApi::IConnectionPtr GetConnection();

    template <typename T>
    TFuture<T> DoWithHedging(TCallback<TFuture<T>(NApi::IClientPtr)> callback)
    {
        auto now = NProfiling::GetCpuInstant();
        auto clients = [&] {
            TGuard guard(SpinLock_);
            for (auto& client : Clients_) {
                if (client.BanUntil < now) {
                    client.AdaptivePenalty = 0;
                }
            }

            return Clients_;
        }();

        NProfiling::TCpuDuration minInitialPenalty = Max<i64>();
        for (auto& client : clients) {
            client.ExternalPenalty = PenaltyProvider_->Get(client.ClusterName);
            NProfiling::TCpuDuration currentInitialPenalty = client.InitialPenalty + client.AdaptivePenalty + client.ExternalPenalty;
            minInitialPenalty = Min(minInitialPenalty, currentInitialPenalty);
        }

        TVector<TFuture<T>> futures(Reserve(clients.size()));
        for (auto [i, client] : Enumerate(clients)) {
            TDuration effectivePenalty = NProfiling::CpuDurationToDuration(client.InitialPenalty + client.AdaptivePenalty + client.ExternalPenalty - minInitialPenalty);
            if (effectivePenalty) {
                auto delayedFuture = NConcurrency::TDelayedExecutor::MakeDelayed(effectivePenalty, NYT::NRpc::TDispatcher::Get()->GetHeavyInvoker());
                futures.push_back(delayedFuture.Apply(BIND(callback, client.Client)));
            } else {
                futures.push_back(callback(client.Client));
            }
            futures.back().Subscribe(BIND(&THedgingExecutor::OnFinishRequest, MakeWeak(this), i, effectivePenalty, client.AdaptivePenalty, client.ExternalPenalty, now));
        }

        return AnySucceeded(std::move(futures));
    }

private:
    void OnFinishRequest(
        size_t clientIndex,
        TDuration effectivePenalty,
        NProfiling::TCpuDuration adaptivePenalty,
        NProfiling::TCpuDuration externalPenalty,
        NProfiling::TCpuInstant start,
        const TError& r);

    struct TEntry
    {
        TEntry(
            NApi::IClientPtr client,
            NProfiling::TCpuDuration initialPenalty,
            TCounterPtr counter,
            const std::string& clusterName);

        NApi::IClientPtr Client;
        std::string ClusterName;
        NProfiling::TCpuDuration AdaptivePenalty;
        NProfiling::TCpuDuration InitialPenalty;
        NProfiling::TCpuDuration ExternalPenalty;
        NProfiling::TCpuInstant BanUntil;
        TCounterPtr Counter;
    };

    TVector<TEntry> Clients_;
    NProfiling::TCpuDuration BanPenalty_;
    NProfiling::TCpuDuration BanDuration_;
    IPenaltyProviderPtr PenaltyProvider_;
    TSpinLock SpinLock_;
};

DEFINE_REFCOUNTED_TYPE(THedgingExecutor)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NHedging::NRpc
