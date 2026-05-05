#include "transaction_pinger.h"

#include "transaction.h"

#include <yt/cpp/mapreduce/interface/error_codes.h>
#include <yt/cpp/mapreduce/interface/logging/yt_log.h>
#include <yt/cpp/mapreduce/interface/raw_client.h>

#include <yt/cpp/mapreduce/http/requests.h>
#include <yt/cpp/mapreduce/http/retry_request.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <library/cpp/yt/threading/spin_lock.h>
#include <library/cpp/yt/assert/assert.h>

#include <util/datetime/base.h>
#include <util/random/random.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TSharedTransactionPinger
    : public ITransactionPinger
{
public:
    TSharedTransactionPinger(int poolThreadCount, IRawClientPtr rawClient)
        : ThreadPool_(NConcurrency::CreateThreadPool(poolThreadCount, "tx_pinger_pool"))
        , Invoker_(ThreadPool_->GetInvoker())
        , RawClient_(std::move(rawClient))
    { }

    ~TSharedTransactionPinger() override
    {
        ThreadPool_->Shutdown();
    }

    ITransactionPingerPtr GetChildTxPinger() override
    {
        return this;
    }

    void RegisterTransaction(const TPingableTransaction& pingableTx) override
    {
        auto [minPingInterval, maxPingInterval] = pingableTx.GetPingInterval();
        auto pingInterval = (minPingInterval + maxPingInterval) / 2;
        double jitter = (maxPingInterval - pingInterval) / pingInterval;

        auto opts = NConcurrency::TPeriodicExecutorOptions{pingInterval, pingInterval, jitter};
        auto periodic = std::make_shared<NConcurrency::TPeriodicExecutorPtr>(nullptr);
        // Have to use weak_ptr in order to break reference cycle
        // This weak_ptr holds pointer to periodic, which will contain this lambda
        // Also we consider that lifetime of this lambda is no longer than lifetime of pingableTx
        // because every pingableTx have to call RemoveTransaction before it is destroyed
        auto pingRoutine = BIND([this, &pingableTx, periodic = std::weak_ptr{periodic}] {
            auto strong_ptr = periodic.lock();
            YT_VERIFY(strong_ptr);
            DoPingTransaction(pingableTx, *strong_ptr);
        });
        *periodic = New<NConcurrency::TPeriodicExecutor>(ThreadPool_->GetInvoker(), pingRoutine, opts);
        (*periodic)->Start();

        auto guard = Guard(SpinLock_);
        YT_VERIFY(!Transactions_.contains(pingableTx.GetId()));
        Transactions_[pingableTx.GetId()] = std::move(periodic);
    }

    bool HasTransaction(const TPingableTransaction& pingableTx) override
    {
        auto guard = Guard(SpinLock_);
        return Transactions_.contains(pingableTx.GetId());
    }


    void RemoveTransaction(const TPingableTransaction& pingableTx) override
    {
        std::shared_ptr<NConcurrency::TPeriodicExecutorPtr> periodic;
        {
            auto guard = Guard(SpinLock_);

            auto it = Transactions_.find(pingableTx.GetId());

            YT_VERIFY(it != Transactions_.end());

            periodic = std::move(it->second);
            Transactions_.erase(it);
        }
        YT_UNUSED_FUTURE((*periodic)->Stop());
    }

    TFuture<void> AsyncAbortTransaction(const TTransactionId& transactionId) override
    {
        auto result = BIND([rawClient = RawClient_, transactionId = transactionId] {
            TMutationId mutationId;
            rawClient->AbortTransaction(mutationId, transactionId);
        })
            .AsyncVia(ThreadPool_->GetInvoker())
            .Run();

        return result;
    }

private:
    void DoPingTransaction(const TPingableTransaction& pingableTx, NConcurrency::TPeriodicExecutorPtr periodic)
    {
        try {
            pingableTx.Ping();
        } catch (const TErrorResponse& e) {
            /// NB: No logging here, CheckError() already logged TErrorResponse.
            if (e.GetError().ContainsErrorCode(NYT::NClusterErrorCodes::NTransactionClient::NoSuchTransaction)) {
                YT_UNUSED_FUTURE(periodic->Stop());
            } else if (e.GetError().ContainsErrorCode(NYT::NClusterErrorCodes::Timeout)) {
                periodic->ScheduleOutOfBand();
            }
        } catch (const std::exception& e) {
            YT_LOG_ERROR("DoPingTransaction has failed (TransactionId: %v, Error: %v)",
                GetGuidAsString(pingableTx.GetId()),
                e.what());
        }
    }


private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    THashMap<TTransactionId, std::shared_ptr<NConcurrency::TPeriodicExecutorPtr>> Transactions_;

    NConcurrency::IThreadPoolPtr ThreadPool_;
    IInvokerPtr Invoker_;
    IRawClientPtr RawClient_;
};

////////////////////////////////////////////////////////////////////////////////

ITransactionPingerPtr CreateTransactionPinger(const TConfigPtr& config, IRawClientPtr rawClient)
{
    YT_LOG_DEBUG("Using async transaction pinger");

    return MakeIntrusive<TSharedTransactionPinger>(config->AsyncTxPingerPoolThreads, std::move(rawClient));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
