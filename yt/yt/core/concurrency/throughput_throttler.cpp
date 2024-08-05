#include "throughput_throttler.h"
#include "config.h"

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/singleton.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <queue>

namespace NYT::NConcurrency {

using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

namespace {

bool WillOverflowMul(i64 lhs, i64 rhs)
{
    i64 result;
    return __builtin_mul_overflow(lhs, rhs, &result);
}

i64 ClampingAdd(i64 lhs, i64 rhs, i64 max)
{
    i64 result;
    if (__builtin_add_overflow(lhs, rhs, &result) || result > max) {
        return max;
    }
    return result;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TThrottlerRequest)

struct TThrottlerRequest
    : public TRefCounted
{
    explicit TThrottlerRequest(i64 amount)
        : Amount(amount)
    { }

    i64 Amount;
    TPromise<void> Promise;
    std::atomic_flag Set = ATOMIC_FLAG_INIT;
    NProfiling::TCpuInstant StartTime = NProfiling::GetCpuInstant();
    NTracing::TTraceContextPtr TraceContext;
};

DEFINE_REFCOUNTED_TYPE(TThrottlerRequest)

////////////////////////////////////////////////////////////////////////////////

class TReconfigurableThroughputThrottler
    : public ITestableReconfigurableThroughputThrottler
{
public:
    TReconfigurableThroughputThrottler(
        TThroughputThrottlerConfigPtr config,
        const TLogger& logger,
        const NProfiling::TProfiler& profiler)
        : Logger(logger)
        , ValueCounter_(profiler.Counter("/value"))
        , ReleaseCounter_(profiler.Counter("/released"))
        , QueueSizeGauge_(profiler.Gauge("/queue_size"))
        , WaitTimer_(profiler.Timer("/wait_time"))
        , LimitGauge_(profiler.Gauge("/limit"))
    {
        Reconfigure(config);
    }

    TFuture<void> GetAvailableFuture() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return DoThrottle(0);
    }

    TFuture<void> Throttle(i64 amount) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(amount >= 0);

        // Fast lane.
        if (amount == 0) {
            return VoidFuture;
        }

        return DoThrottle(amount);
    }

    bool TryAcquire(i64 amount) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(amount >= 0);

        // Fast lane (only).
        if (amount == 0) {
            return true;
        }

        if (Limit_.load() >= 0) {
            while (true) {
                TryUpdateAvailable();
                auto available = Available_.load();
                if (available < 0) {
                    return false;
                }
                if (Available_.compare_exchange_weak(available, available - amount)) {
                    break;
                }
            }
        }

        ValueCounter_.Increment(amount);
        return true;
    }

    i64 TryAcquireAvailable(i64 amount) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(amount >= 0);

        // Fast lane (only).
        if (amount == 0) {
            return 0;
        }

        if (Limit_.load() >= 0) {
            while (true) {
                TryUpdateAvailable();
                auto available = Available_.load();
                if (available < 0) {
                    return 0;
                }
                i64 acquire = std::min(amount, available);
                if (Available_.compare_exchange_weak(available, available - acquire)) {
                    amount = acquire;
                    break;
                }
            }
        }

        ValueCounter_.Increment(amount);
        return amount;
    }

    void Acquire(i64 amount) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(amount >= 0);

        // Fast lane (only).
        if (amount == 0) {
            return;
        }

        TryUpdateAvailable();
        if (Limit_.load() >= 0) {
            Available_ -= amount;
        }

        ValueCounter_.Increment(amount);
    }

    void Release(i64 amount) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(amount >= 0);

        if (amount == 0) {
            return;
        }

        Available_ += amount;
        ReleaseCounter_.Increment(amount);
    }

    bool IsOverdraft() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        // Fast lane (only).
        TryUpdateAvailable();

        if (Limit_.load() < 0) {
            return false;
        }

        return Available_ <= 0;
    }

    void Reconfigure(TThroughputThrottlerConfigPtr config) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        DoReconfigure(config->Limit, config->Period);
    }

    void SetLimit(std::optional<double> limit) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        DoReconfigure(limit, Period_);
    }

    i64 GetQueueTotalAmount() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        // Fast lane (only).
        return QueueTotalAmount_;
    }

    TDuration GetEstimatedOverdraftDuration() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto queueTotalCount = QueueTotalAmount_.load();
        auto limit = Limit_.load();
        if (queueTotalCount == 0 || limit <= 0) {
            return TDuration::Zero();
        }

        return queueTotalCount / limit * TDuration::Seconds(1);
    }

    TThroughputThrottlerConfigPtr GetConfig() const override
    {
        auto result = New<TThroughputThrottlerConfig>();
        result->Limit = Limit_.load();
        result->Period = Period_.load();
        return result;
    }

    i64 GetAvailable() const override
    {
        return Available_.load();
    }

    void SetLastUpdated(TInstant lastUpdated) override
    {
        LastUpdated_.store(lastUpdated);
    }

private:
    const TLogger Logger;

    NProfiling::TCounter ValueCounter_;
    NProfiling::TCounter ReleaseCounter_;
    NProfiling::TGauge QueueSizeGauge_;
    NProfiling::TEventTimer WaitTimer_;
    NProfiling::TGauge LimitGauge_;

    std::atomic<TInstant> LastUpdated_ = TInstant::Zero();
    std::atomic<i64> Available_ = 0;
    std::atomic<i64> QueueTotalAmount_ = 0;

    //! Protects the section immediately following it.
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    // -1 indicates no limit
    std::atomic<double> Limit_;
    std::atomic<TDuration> Period_;
    TDelayedExecutorCookie UpdateCookie_;

    std::queue<TThrottlerRequestPtr> Requests_;

    TFuture<void> DoThrottle(i64 amount)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        ValueCounter_.Increment(amount);
        if (Limit_.load() < 0) {
            return VoidFuture;
        }

        while (true) {
            TryUpdateAvailable();

            if (QueueTotalAmount_ > 0) {
                break;
            }

            auto available = Available_.load();
            if (available <= 0) {
                break;
            }

            if (Available_.compare_exchange_strong(available, available - amount)) {
                return VoidFuture;
            }
        }

        // Slow lane.
        auto guard = Guard(SpinLock_);

        if (Limit_.load() < 0) {
            return VoidFuture;
        }

        // Enqueue request to be executed later.
        auto promise = NewPromise<void>();
        auto request = New<TThrottlerRequest>(amount);
        request->TraceContext = NTracing::CreateTraceContextFromCurrent("Throttler");

        YT_LOG_DEBUG(
            "Started waiting for throttler (Amount: %v, RequestTraceId: %v)",
            amount,
            request->TraceContext->GetTraceId());

        promise.OnCanceled(BIND([weakRequest = MakeWeak(request), amount, this, this_ = MakeStrong(this)] (const TError& error) {
            auto request = weakRequest.Lock();
            if (request && !request->Set.test_and_set()) {
                NTracing::TTraceContextFinishGuard guard(std::move(request->TraceContext));
                YT_LOG_DEBUG(
                    "Canceled waiting for throttler (Amount: %v)",
                    amount);
                request->Promise.Set(TError(NYT::EErrorCode::Canceled, "Throttled request canceled")
                    << error);
                QueueTotalAmount_ -= amount;
                QueueSizeGauge_.Update(QueueTotalAmount_);
            }
        }));
        request->Promise = std::move(promise);
        Requests_.push(request);
        QueueTotalAmount_ += amount;
        QueueSizeGauge_.Update(QueueTotalAmount_);

        ScheduleUpdate();

        return request->Promise;
    }

    static i64 GetDeltaAvailable(TInstant current, TInstant lastUpdated, double limit)
    {
        auto timePassed = current - lastUpdated;

        if (limit > 1) {
            constexpr auto maxRepresentableMilliSeconds = static_cast<double>(TDuration::Max().MilliSeconds());
            auto maxValidMilliSecondsPassed = maxRepresentableMilliSeconds / limit;

            if (timePassed.MilliSeconds() > maxValidMilliSecondsPassed) {
                // NB(coteeq): Actual timePassed will overflow multiplication below,
                // so we have nothing better than to just shrink this duration.
                timePassed = TDuration::MilliSeconds(maxValidMilliSecondsPassed);
            }
        }

        auto deltaAvailable = static_cast<i64>(timePassed.MilliSeconds() * limit / 1000);
        YT_VERIFY(deltaAvailable >= 0);

        return deltaAvailable;
    }

    void DoReconfigure(std::optional<double> limit, TDuration period)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        // Slow lane (only).
        auto guard = Guard(SpinLock_);

        auto newLimit = limit.value_or(-1);
        Limit_ = newLimit;
        LimitGauge_.Update(newLimit);
        Period_ = period;
        TDelayedExecutor::CancelAndClear(UpdateCookie_);
        auto now = GetInstant();
        if (limit && *limit > 0) {
            YT_VERIFY(!WillOverflowMul(period.MilliSeconds(), *limit));
            auto lastUpdated = LastUpdated_.load();
            auto maxAvailable = period.MilliSeconds() * *limit / 1000;

            if (lastUpdated == TInstant::Zero()) {
                Available_ = maxAvailable;
                LastUpdated_ = now;
            } else {
                auto deltaAvailable = GetDeltaAvailable(now, lastUpdated, *limit);

                auto newAvailable = ClampingAdd(Available_.load(), deltaAvailable, maxAvailable);
                YT_VERIFY(newAvailable <= maxAvailable);
                if (newAvailable == maxAvailable) {
                    LastUpdated_ = now;
                } else {
                    LastUpdated_ = lastUpdated + TDuration::MilliSeconds(deltaAvailable * 1000 / *limit);
                    // Just in case.
                    LastUpdated_ = std::min(LastUpdated_.load(), now);
                }
                Available_ = newAvailable;
            }
        } else {
            Available_ = 0;
            LastUpdated_ = now;
        }
        ProcessRequests(std::move(guard));
    }

    void ScheduleUpdate()
    {
        VERIFY_SPINLOCK_AFFINITY(SpinLock_);

        if (UpdateCookie_) {
            return;
        }

        auto limit = Limit_.load();
        YT_VERIFY(limit >= 0);

        // Reconfigure clears the update cookie, so infinity is fine.
        auto delay = limit ? TDuration::MilliSeconds(Max<i64>(0, -Available_ * 1000 / limit)) : TDuration::Max();

        UpdateCookie_ = TDelayedExecutor::Submit(
            BIND_NO_PROPAGATE(&TReconfigurableThroughputThrottler::Update, MakeWeak(this)),
            delay);
    }

    void TryUpdateAvailable()
    {
        auto limit = Limit_.load();
        if (limit < 0) {
            return;
        }

        auto period = Period_.load();
        auto current = GetInstant();
        auto lastUpdated = LastUpdated_.load();

        auto deltaAvailable = GetDeltaAvailable(current, lastUpdated, limit);

        if (deltaAvailable == 0) {
            return;
        }
        // The delta computed above is zero if the limit is zero.
        YT_VERIFY(limit > 0);

        current = lastUpdated + TDuration::MilliSeconds(deltaAvailable * 1000 / limit);

        if (LastUpdated_.compare_exchange_strong(lastUpdated, current)) {
            auto available = Available_.load();
            auto throughputPerPeriod = static_cast<i64>(period.SecondsFloat() * limit);

            while (true) {
                auto newAvailable = ClampingAdd(available, deltaAvailable, /*max*/ throughputPerPeriod);
                if (Available_.compare_exchange_weak(available, newAvailable)) {
                    break;
                }
            }
        }
    }

    void Update()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = Guard(SpinLock_);
        UpdateCookie_.Reset();
        TryUpdateAvailable();

        ProcessRequests(std::move(guard));
    }

    void ProcessRequests(TGuard<NThreading::TSpinLock> guard)
    {
        VERIFY_SPINLOCK_AFFINITY(SpinLock_);

        std::vector<TThrottlerRequestPtr> readyList;

        auto limit = Limit_.load();
        while (!Requests_.empty() && (limit < 0 || Available_ >= 0)) {
            const auto& request = Requests_.front();
            if (!request->Set.test_and_set()) {
                NTracing::TTraceContextGuard traceGuard(std::move(request->TraceContext));

                auto waitTime = NProfiling::CpuDurationToDuration(NProfiling::GetCpuInstant() - request->StartTime);
                YT_LOG_DEBUG(
                    "Finished waiting for throttler (Amount: %v, WaitTime: %v)",
                    request->Amount,
                    waitTime);

                if (limit >= 0) {
                    Available_ -= request->Amount;
                }
                readyList.push_back(request);
                QueueTotalAmount_ -= request->Amount;
                QueueSizeGauge_.Update(QueueTotalAmount_);
                WaitTimer_.Record(waitTime);
            }
            Requests_.pop();
        }

        if (!Requests_.empty()) {
            ScheduleUpdate();
        }

        guard.Release();

        for (const auto& request : readyList) {
            request->Promise.Set();
        }
    }
};

IReconfigurableThroughputThrottlerPtr CreateReconfigurableThroughputThrottler(
    TThroughputThrottlerConfigPtr config,
    const TLogger& logger,
    const NProfiling::TProfiler& profiler)
{
    return New<TReconfigurableThroughputThrottler>(
        config,
        logger,
        profiler);
}

IReconfigurableThroughputThrottlerPtr CreateNamedReconfigurableThroughputThrottler(
    TThroughputThrottlerConfigPtr config,
    const TString& name,
    TLogger logger,
    NProfiling::TProfiler profiler)
{
    return CreateReconfigurableThroughputThrottler(
        std::move(config),
        logger.WithTag("Throttler: %v", name),
        profiler.WithPrefix("/" + CamelCaseToUnderscoreCase(name)));
}

////////////////////////////////////////////////////////////////////////////////

class TUnlimitedThroughputThrottler
    : public IReconfigurableThroughputThrottler
{
public:
    explicit TUnlimitedThroughputThrottler(
        const NProfiling::TProfiler& profiler = {})
        : ValueCounter_(profiler.Counter("/value"))
        , ReleaseCounter_(profiler.Counter("/released"))
    { }

    TFuture<void> Throttle(i64 amount) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(amount >= 0);

        ValueCounter_.Increment(amount);
        return VoidFuture;
    }

    bool TryAcquire(i64 amount) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(amount >= 0);

        ValueCounter_.Increment(amount);
        return true;
    }

    i64 TryAcquireAvailable(i64 amount) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(amount >= 0);

        ValueCounter_.Increment(amount);
        return amount;
    }

    void Acquire(i64 amount) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(amount >= 0);

        ValueCounter_.Increment(amount);
    }

    void Release(i64 amount) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(amount >= 0);

        ReleaseCounter_.Increment(amount);
    }

    bool IsOverdraft() override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return false;
    }

    i64 GetQueueTotalAmount() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return 0;
    }

    TDuration GetEstimatedOverdraftDuration() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return TDuration::Zero();
    }

    i64 GetAvailable() const override
    {
        YT_UNIMPLEMENTED();
    }

    void Reconfigure(TThroughputThrottlerConfigPtr /*config*/) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
    }

    void SetLimit(std::optional<double> /*limit*/) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
    }

    TFuture<void> GetAvailableFuture() override
    {
        YT_UNIMPLEMENTED();
    }

    TThroughputThrottlerConfigPtr GetConfig() const override
    {
        YT_UNIMPLEMENTED();
    }

private:
    NProfiling::TCounter ValueCounter_;
    NProfiling::TCounter ReleaseCounter_;
};

IReconfigurableThroughputThrottlerPtr GetUnlimitedThrottler()
{
    return LeakyRefCountedSingleton<TUnlimitedThroughputThrottler>();
}

IReconfigurableThroughputThrottlerPtr CreateNamedUnlimitedThroughputThrottler(
    const TString& name,
    NProfiling::TProfiler profiler)
{
    profiler = profiler.WithPrefix("/" + CamelCaseToUnderscoreCase(name));
    return New<TUnlimitedThroughputThrottler>(profiler);
}

////////////////////////////////////////////////////////////////////////////////

class TCombinedThroughputThrottler
    : public IThroughputThrottler
{
public:
    explicit TCombinedThroughputThrottler(const std::vector<IThroughputThrottlerPtr>& throttlers)
        : Throttlers_(throttlers)
    { }

    TFuture<void> Throttle(i64 amount) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(amount >= 0);

        SelfQueueSize_ += amount;

        std::vector<TFuture<void>> asyncResults;
        for (const auto& throttler : Throttlers_) {
            asyncResults.push_back(throttler->Throttle(amount));
        }

        return AllSucceeded(asyncResults).Apply(BIND([this, weakThis = MakeWeak(this), amount] (const TError& /*error*/ ) {
            if (auto this_ = weakThis.Lock()) {
                SelfQueueSize_ -= amount;
            }
        }));
    }

    bool TryAcquire(i64 /*amount*/) override
    {
        YT_ABORT();
    }

    i64 TryAcquireAvailable(i64 /*amount*/) override
    {
        YT_ABORT();
    }

    void Acquire(i64 amount) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(amount >= 0);

        for (const auto& throttler : Throttlers_) {
            throttler->Acquire(amount);
        }
    }

    void Release(i64 amount) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(amount >= 0);

        for (const auto& throttler : Throttlers_) {
            throttler->Release(amount);
        }
    }

    bool IsOverdraft() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        for (const auto& throttler : Throttlers_) {
            if (throttler->IsOverdraft()) {
                return true;
            }
        }
        return false;
    }

    i64 GetQueueTotalAmount() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto totalQueueSize = SelfQueueSize_.load();
        for (const auto& throttler : Throttlers_) {
            totalQueueSize += std::max<i64>(throttler->GetQueueTotalAmount() - SelfQueueSize_.load(), 0);
        }

        return totalQueueSize;
    }

    TDuration GetEstimatedOverdraftDuration() const override
    {
        TDuration result = TDuration::Zero();
        for (const auto& throttler : Throttlers_) {
            result = std::max(result, throttler->GetEstimatedOverdraftDuration());
        }
        return result;
    }

    i64 GetAvailable() const override
    {
        YT_UNIMPLEMENTED();
    }

private:
    const std::vector<IThroughputThrottlerPtr> Throttlers_;

    std::atomic<i64> SelfQueueSize_ = 0;
};

IThroughputThrottlerPtr CreateCombinedThrottler(
    const std::vector<IThroughputThrottlerPtr>& throttlers)
{
    return New<TCombinedThroughputThrottler>(throttlers);
}

////////////////////////////////////////////////////////////////////////////////

class TStealingThrottler
    : public IThroughputThrottler
{
public:
    TStealingThrottler(
        IThroughputThrottlerPtr stealer,
        IThroughputThrottlerPtr underlying)
        : Stealer_(std::move(stealer))
        , Underlying_(std::move(underlying))
    { }

    TFuture<void> Throttle(i64 amount) override
    {
        auto future = Stealer_->Throttle(amount);
        future.Subscribe(BIND([=, this, this_ = MakeStrong(this)] (const TError& error) {
            if (error.IsOK()) {
                Underlying_->Acquire(amount);
            }
        }));
        return future;
    }

    bool TryAcquire(i64 amount) override
    {
        if (Stealer_->TryAcquire(amount)) {
            Underlying_->Acquire(amount);
            return true;
        }

        return false;
    }

    i64 TryAcquireAvailable(i64 amount) override
    {
        auto result = Stealer_->TryAcquireAvailable(amount);
        Underlying_->Acquire(result);

        return result;
    }

    void Acquire(i64 amount) override
    {
        Stealer_->Acquire(amount);
        Underlying_->Acquire(amount);
    }

    void Release(i64 amount) override
    {
        Stealer_->Release(amount);
        Underlying_->Release(amount);
    }

    bool IsOverdraft() override
    {
        return Stealer_->IsOverdraft();
    }

    i64 GetQueueTotalAmount() const override
    {
        return Stealer_->GetQueueTotalAmount();
    }

    TDuration GetEstimatedOverdraftDuration() const override
    {
        return Stealer_->GetEstimatedOverdraftDuration();
    }

    i64 GetAvailable() const override
    {
        YT_UNIMPLEMENTED();
    }

private:
    const IThroughputThrottlerPtr Stealer_;
    const IThroughputThrottlerPtr Underlying_;
};

IThroughputThrottlerPtr CreateStealingThrottler(
    IThroughputThrottlerPtr stealer,
    IThroughputThrottlerPtr underlying)
{
    return New<TStealingThrottler>(
        std::move(stealer),
        std::move(underlying));
}

////////////////////////////////////////////////////////////////////////////////

class TPrefetchingThrottler
    : public IThroughputThrottler
{
public:
    TPrefetchingThrottler(
        const TPrefetchingThrottlerConfigPtr& config,
        const IThroughputThrottlerPtr& underlying,
        TLogger logger)
        : Config_(config)
        , Underlying_(underlying)
        , Logger(std::move(logger))
    { }

    TFuture<void> Throttle(i64 amount) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(amount >= 0);

        TPromise<void> promise;
        i64 incomingRequestId;

        {
            auto guard = Guard(Lock_);

            if (DoTryAcquire(amount)) {
                return VoidFuture;
            }

            promise = NewPromise<void>();

            Balance_ -= amount;
            incomingRequestId = ++IncomingRequestId_;
            IncomingRequests_.emplace_back(TIncomingRequest{amount, promise, incomingRequestId});
        }

        YT_LOG_DEBUG(
            "Enqueued a request to the prefetching throttler (Id: %v, Amount: %v)",
            incomingRequestId,
            amount);

        RequestUnderlyingIfNeeded();

        return promise.ToFuture();
    }

    bool TryAcquire(i64 amount) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(amount >= 0);

        {
            auto guard = Guard(Lock_);

            if (DoTryAcquire(amount)) {
                return true;
            }
        }

        StockUp(amount);

        return false;
    }

    i64 TryAcquireAvailable(i64 amount) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(amount >= 0);

        {
            auto guard = Guard(Lock_);

            ++IncomingRequestCountPastWindow_;

            if (IncomingRequests_.empty() && Available_ > 0) {
                auto acquire = std::min(Available_, amount);
                Available_ -= acquire;

                return acquire;
            }
        }

        StockUp(amount);

        return 0;
    }

    void Acquire(i64 amount) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(amount >= 0);

        i64 forecastedAvailable;

        {
            auto guard = Guard(Lock_);
            ++IncomingRequestCountPastWindow_;
            // Note that #Available_ can go negative.
            Available_ -= amount;
            forecastedAvailable = Available_ + Balance_;
        }

        if (forecastedAvailable < 0) {
            StockUp(-forecastedAvailable);
        }
    }

    void Release(i64 amount) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(amount >= 0);

        if (amount == 0) {
            return;
        }

        {
            auto guard = Guard(Lock_);
            Available_ += amount;
        }

        YT_LOG_DEBUG(
            "Released from prefetching throttler (Amount: %v)",
            amount);
    }

    bool IsOverdraft() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = Guard(Lock_);

        return Available_ <= 0;
    }

    i64 GetQueueTotalAmount() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Underlying_->GetQueueTotalAmount();
    }

    TDuration GetEstimatedOverdraftDuration() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Underlying_->GetEstimatedOverdraftDuration();
    }

    i64 GetAvailable() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = Guard(Lock_);

        return Available_;
    }

private:
    struct TIncomingRequest
    {
        //! The amount in the incoming request.
        i64 Amount;

        //! The promise created for the incoming request.
        //! It will be fulfilled when the Underlying_ will yield enough
        //! to satisfy the Amount fields of all the requests upto this one in the IncomingRequests_ queue.
        TPromise<void> Promise;

        //! The id of the incoming request.
        i64 Id;
    };

    struct TUnderlyingRequest
    {
        //! The amount in the underlying request.
        i64 Amount;

        //! When the request to the Underlying_ was made.
        TInstant Timestamp;

        //! How many incoming requests was received since the previous underlying request.
        //! Used to estimate the incoming RPS for the logging purposes.
        i64 IncomingRequestCount;
    };

    const TPrefetchingThrottlerConfigPtr Config_;

    //! The underlying throttler for which the RPS is limited.
    const IThroughputThrottlerPtr Underlying_;

    const TLogger Logger;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);

    //! The amount already received from the Underlying_ and not yet consumed.
    //! That is the amount that can be handed to incoming requests immediately and without requests to the Underlying_.
    i64 Available_ = 0;

    //! Total inflight underlying amount in the UnderlyingRequests_ minus total incoming amount in the IncomingRequests_.
    //! Negative value for the sum (Available_ + Balance_) means that a new request to the Underlying_ is required.
    i64 Balance_ = 0;

    //! The minimum amount to be requested from the Underlying_ next time.
    i64 PrefetchAmount_ = 1;

    //! The sum of the IncomingRequestCount fields for requests in the UnderlyingRequests_ window.
    //! Used to estimate the incoming RPS.
    i64 IncomingRequestCountInWindow_ = 0;

    //! The count of incoming requests since the last outgoing request.
    //! It will be moved into IncomingRequestCount field when a new request to the Underlying_ will be made.
    i64 IncomingRequestCountPastWindow_ = 0;

    //! The sum of the IncomingRequestCount fields for requests in the UnderlyingRequests_ window.
    //! Used to estimate the incoming RPS.
    i64 UnderlyingAmountInWindow_ = 0;

    //! The time point at which the UnderlyingRequests_ was dropped to avoid unbounded grouth of the prefetch value.
    TInstant RpsStatisticsStart_ = TInstant::Now();

    //! The FIFO of the incoming requests containing requested amounts and given promises.
    std::deque<TIncomingRequest> IncomingRequests_;

    //! The window of the requests to the Underlying_ used to estimate underlying RPS along with incoming RPS.
    std::deque<TUnderlyingRequest> UnderlyingRequests_;

    //! The id of the last incoming request.
    i64 IncomingRequestId_ = 0;

    //! The id of the last underlying request.
    i64 UnderlyingRequestId_ = 0;

    //! The total count of responses from the underlying throttler.
    i64 UnderlyingResponseCount_ = 0;

    //! If there are no pending incoming requests, tries to acquire #amount from the local #Available_.
    bool DoTryAcquire(i64 amount)
    {
        ++IncomingRequestCountPastWindow_;

        if (IncomingRequests_.empty() && amount <= Available_) {
            Available_ -= amount;
            return true;
        }
        return false;
    }

    //! Requests #amount from the underlying throttler for future requests.
    //! Triggered when a #TryAcquire or #TryAcquireAvailable incoming request can not be fully satisfied from #Available_.
    //! And when the sum of #Available_ and #Balance_ becomes negative after #Acquire.
    void StockUp(i64 amount)
    {
        amount = std::clamp(amount, Config_->MinPrefetchAmount, Config_->MaxPrefetchAmount);

        i64 underlyingRequestId = 0;
        {
            auto guard = Guard(Lock_);
            underlyingRequestId = ++UnderlyingRequestId_;
        }

        Throttle(amount)
            .Subscribe(BIND(&TPrefetchingThrottler::OnThrottlingResponse, MakeWeak(this), amount, underlyingRequestId));

        auto guard = Guard(Lock_);
        // Do not count this recursive request as an incoming one.
        --IncomingRequestCountPastWindow_;
    }

    //! Checks that the sum of #Available_ and #Balance_ is enough to satisfy pending incoming requests.
    //! If it is not, makes a request to the #Underlying_ with an appropriate amount.
    void RequestUnderlyingIfNeeded()
    {
        i64 underlyingAmount = 0;
        i64 underlyingRequestId = 0;
        double incomingRps = 0.0;
        double underlyingRps = 0.0;
        auto now = TInstant::Now();
        i64 balance;
        i64 prefetchAmount;

        {
            auto guard = Guard(Lock_);

            YT_VERIFY(GetOutstandingRequestCount() > 0 || Balance_ <= 0);

            auto forecastedAvailable = Available_ + Balance_;
            if (forecastedAvailable >= 0) {
                return;
            }

            UpdatePrefetch(now);
            underlyingAmount = std::max(PrefetchAmount_, -forecastedAvailable);
            Balance_ += underlyingAmount;

            incomingRps = IncomingRps();
            underlyingRps = UnderlyingRps();

            RegisterUnderlyingRequest(now, underlyingAmount);

            underlyingRequestId = ++UnderlyingRequestId_;

            YT_VERIFY(Available_ + Balance_ >= 0);

            balance = Balance_;
            prefetchAmount = PrefetchAmount_;
        }

        YT_LOG_DEBUG(
            "Request to the underlying throttler (Id: %v, UnderlyingAmount: %v, Balance: %v, Prefetch: %v, IncomingRps: %v, UnderlyingRps: %v)",
            underlyingRequestId,
            underlyingAmount,
            balance,
            prefetchAmount,
            incomingRps,
            underlyingRps);

        Underlying_->Throttle(underlyingAmount)
            .Subscribe(BIND(&TPrefetchingThrottler::OnThrottlingResponse, MakeWeak(this), underlyingAmount, underlyingRequestId));
    }

    //! Drops outdated requests to the #Underlying_ from the window used for RPS estimation.
    void UpdateRpsWindow(TInstant now)
    {
        while (!UnderlyingRequests_.empty() && UnderlyingRequests_.front().Timestamp + Config_->Window < now) {
            IncomingRequestCountInWindow_ -= UnderlyingRequests_.front().IncomingRequestCount;
            UnderlyingAmountInWindow_ -= UnderlyingRequests_.front().Amount;
            UnderlyingRequests_.pop_front();
        }
    }

    //! Registers the new request to the underlying throttler in the RPS window.
    void RegisterUnderlyingRequest(TInstant now, i64 underlyingAmount)
    {
        IncomingRequestCountInWindow_ += IncomingRequestCountPastWindow_;
        UnderlyingAmountInWindow_ += underlyingAmount;
        UnderlyingRequests_.push_back({underlyingAmount, now, IncomingRequestCountPastWindow_});
        IncomingRequestCountPastWindow_ = 0;
    }

    //! Recalculates #PrefetchAmount_ to be requested from the #Underlying_ based on the current RPS estimation.
    void UpdatePrefetch(TInstant now)
    {
        UpdateRpsWindow(now);

        auto underlyingRps = UnderlyingRps();
        if (UnderlyingRequests_.size() > 0 && UnderlyingAmountInWindow_ > 0) {
            PrefetchAmount_ = UnderlyingAmountInWindow_ * underlyingRps / Config_->TargetRps / UnderlyingRequests_.size();
        }
        PrefetchAmount_ = std::clamp(PrefetchAmount_, Config_->MinPrefetchAmount, Config_->MaxPrefetchAmount);

        YT_LOG_DEBUG(
            "Recalculate the amount to prefetch from the underlying throttler (RequestsInWindow: %v, Window: %v, UnderlyingRps: %v, TargetRps: %v, PrefetchAmount: %v)",
            UnderlyingRequests_.size(),
            Config_->Window,
            underlyingRps,
            Config_->TargetRps,
            PrefetchAmount_);
    }

    //! Handles a response from the underlying throttler.
    void OnThrottlingResponse(i64 available, i64 id, const TError& error)
    {
        YT_LOG_DEBUG(
            "Response from the underlying throttler (Id: %v, Amount: %v, Result: %v)",
            id,
            available,
            error.IsOK());

        if (error.IsOK()) {
            SatisfyIncomingRequests(available);
        } else {
            YT_LOG_ERROR(error, "Error requesting the underlying throttler");
            DropAllIncomingRequests(available, error);
        }
    }

    //! Distributes the #available amount from a positive response from the #Underlying_ among
    //! the incoming requests waiting in the queue.
    void SatisfyIncomingRequests(i64 available)
    {
        std::vector<TIncomingRequest> fulfilled;

        {
            auto guard = Guard(Lock_);

            // Note that #Available_ can be negative.
            // Moreover even the sum #Available_ + #Balance_ can be negative temporarily
            // if a concurrent request decreased the #Balance already but has not requested the #Underlying_ yet.
            Balance_ -= available;
            available += Available_;
            Available_ = 0;
            ++UnderlyingResponseCount_;

            while (!IncomingRequests_.empty() && available >= IncomingRequests_.front().Amount) {
                auto& request = IncomingRequests_.front();
                available -= request.Amount;
                Balance_ += request.Amount;
                fulfilled.emplace_back(std::move(request));
                IncomingRequests_.pop_front();
            }

            Available_ = available;
        }

        for (auto& request : fulfilled) {
            // The recursive call to #Throttle from #StockUp creates
            // a recursive call to #SatisfyIncomingRequests when the corresponding #promise is set.
            // So that #promise should be set without holding the #Lock_.
            auto result = request.Promise.TrySet();
            YT_LOG_DEBUG(
                "Sent the response for the incoming request (Id: %v, Amount: %v, Result: %v)",
                request.Id,
                request.Amount,
                result);
        }
    }

    //! Drops all incoming requests propagating an #error received from the underlying throttler.
    void DropAllIncomingRequests(i64 available, const TError& error)
    {
        std::vector<TIncomingRequest> fulfilled;

        {
            auto guard = Guard(Lock_);

            Balance_ -= available;
            ++UnderlyingResponseCount_;

            while (!IncomingRequests_.empty()) {
                auto& request = IncomingRequests_.front();
                Balance_ += request.Amount;
                fulfilled.emplace_back(std::move(request));
                IncomingRequests_.pop_front();
            }
        }

        for (auto& request : fulfilled) {
            request.Promise.Set(error);
            YT_LOG_DEBUG(
                "Dropped the incoming request (Id: %v, Amount: %v)",
                request.Id,
                request.Amount);
        }
    }

    //! Estimate RPS of incoming requests.
    double IncomingRps() const
    {
        return (IncomingRequestCountInWindow_ + IncomingRequestCountPastWindow_) / Config_->Window.SecondsFloat();
    }

    //! Estimate RPS of requests to the underlying throttler.
    double UnderlyingRps() const
    {
        return UnderlyingRequests_.size() / Config_->Window.SecondsFloat();
    }

    //! Calculate how many requests to the underlying throttler are still active.
    i64 GetOutstandingRequestCount() const
    {
        return UnderlyingRequestId_ - UnderlyingResponseCount_;
    }
};

IThroughputThrottlerPtr CreatePrefetchingThrottler(
    const TPrefetchingThrottlerConfigPtr& config,
    const IThroughputThrottlerPtr& underlying,
    const TLogger& Logger)
{
    if (config->Enable) {
        return New<TPrefetchingThrottler>(
            config,
            underlying,
            Logger);
    } else {
        return underlying;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
