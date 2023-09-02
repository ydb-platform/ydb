#include "batching_secret_vault_service.h"
#include "secret_vault_service.h"
#include "config.h"
#include "private.h"

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <queue>

namespace NYT::NAuth {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TBatchingSecretVaultService
    : public ISecretVaultService
{
public:
    TBatchingSecretVaultService(
        TBatchingSecretVaultServiceConfigPtr config,
        ISecretVaultServicePtr underlying,
        NProfiling::TProfiler profiler)
        : Config_(std::move(config))
        , Underlying_(std::move(underlying))
        , TickExecutor_(New<TPeriodicExecutor>(
            NRpc::TDispatcher::Get()->GetHeavyInvoker(),
            BIND(&TBatchingSecretVaultService::OnTick, MakeWeak(this)),
            Config_->BatchDelay))
        , RequestThrottler_(CreateReconfigurableThroughputThrottler(
            Config_->RequestsThrottler,
            NLogging::TLogger(),
            profiler.WithPrefix("/request_throttler")))
        , BatchingLatencyTimer_(profiler.Timer("/batching_latency"))
    {
        TickExecutor_->Start();
    }

    TFuture<std::vector<TErrorOrSecretSubresponse>> GetSecrets(const std::vector<TSecretSubrequest>& subrequests) override
    {
        std::vector<TFuture<TSecretSubresponse>> asyncResults;
        asyncResults.reserve(subrequests.size());
        auto guard = Guard(SpinLock_);
        for (const auto& subrequest : subrequests) {
            asyncResults.push_back(DoGetSecret(subrequest, guard));
        }
        return AllSet(asyncResults);
    }

    TFuture<TString> GetDelegationToken(TDelegationTokenRequest request) override
    {
        return Underlying_->GetDelegationToken(std::move(request));
    }

private:
    const TBatchingSecretVaultServiceConfigPtr Config_;
    const ISecretVaultServicePtr Underlying_;

    const TPeriodicExecutorPtr TickExecutor_;
    const IThroughputThrottlerPtr RequestThrottler_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);

    struct TQueueItem
    {
        TSecretSubrequest Subrequest;
        TPromise<TSecretSubresponse> Promise;
        TInstant EnqueueTime;
    };
    std::queue<TQueueItem> SubrequestQueue_;

    NProfiling::TEventTimer BatchingLatencyTimer_;

    TFuture<TSecretSubresponse> DoGetSecret(const TSecretSubrequest& subrequest, TGuard<NThreading::TSpinLock>& /*guard*/)
    {
        auto promise = NewPromise<TSecretSubresponse>();
        SubrequestQueue_.push(TQueueItem{
            subrequest,
            promise,
            TInstant::Now()
        });
        return promise.ToFuture();
    }

    void OnTick()
    {
        while (true) {
            {
                auto guard = Guard(SpinLock_);
                if (SubrequestQueue_.empty()) {
                    break;
                }
            }

            if (!RequestThrottler_->TryAcquire(1)) {
                break;
            }

            std::vector<TQueueItem> items;
            {
                auto guard = Guard(SpinLock_);
                while (!SubrequestQueue_.empty() && static_cast<int>(items.size()) < Config_->MaxSubrequestsPerRequest) {
                    items.push_back(SubrequestQueue_.front());
                    SubrequestQueue_.pop();
                }
            }

            if (items.empty()) {
                break;
            }

            std::vector<TSecretSubrequest> subrequests;
            subrequests.reserve(items.size());
            auto now = TInstant::Now();
            for (const auto& item : items) {
                subrequests.push_back(item.Subrequest);
                BatchingLatencyTimer_.Record(now - item.EnqueueTime);
            }

            Underlying_->GetSecrets(subrequests).Subscribe(
                BIND([=, items = std::move(items)] (const TErrorOr<std::vector<TErrorOrSecretSubresponse>>& result) mutable {
                    if (result.IsOK()) {
                        const auto& subresponses = result.Value();
                        for (size_t index = 0; index < items.size(); ++index) {
                            auto& item = items[index];
                            item.Promise.Set(subresponses[index]);
                        }
                    } else {
                        for (auto& item : items) {
                            item.Promise.Set(TError(result));
                        }
                    }
                }));
        }
    }
};

ISecretVaultServicePtr CreateBatchingSecretVaultService(
    TBatchingSecretVaultServiceConfigPtr config,
    ISecretVaultServicePtr underlying,
    NProfiling::TProfiler profiler)
{
    return New<TBatchingSecretVaultService>(
        std::move(config),
        std::move(underlying),
        std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
