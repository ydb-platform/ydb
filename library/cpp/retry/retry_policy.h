#pragma once
#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <util/generic/typetraits.h>
#include <util/random/random.h>

#include <functional>
#include <limits>
#include <memory>

//! Retry policy.
//! Calculates delay before next retry (if any).
//! Has several default implementations:
//! - exponential backoff policy;
//! - retries with fixed interval;
//! - no retries.

enum class ERetryErrorClass {
    // This error shouldn't be retried.
    NoRetry,

    // This error could be retried in short period of time.
    ShortRetry,

    // This error requires waiting before it could be retried.
    LongRetry,
};

template <class... TArgs>
struct IRetryPolicy {
    using TPtr = std::shared_ptr<IRetryPolicy>;

    using TRetryClassFunction = std::function<ERetryErrorClass(typename TTypeTraits<TArgs>::TFuncParam...)>;

    //! Retry state of single request.
    struct IRetryState {
        using TPtr = std::unique_ptr<IRetryState>;

        virtual ~IRetryState() = default;

        //! Calculate delay before next retry if next retry is allowed.
        //! Returns empty maybe if retry is not allowed anymore.
        [[nodiscard]] virtual TMaybe<TDuration> GetNextRetryDelay(typename TTypeTraits<TArgs>::TFuncParam... args) = 0;
    };

    virtual ~IRetryPolicy() = default;

    //! Function that is called after first error
    //! to find out a futher retry behaviour.
    //! Retry state is expected to be created for the whole single retry session.
    [[nodiscard]] virtual typename IRetryState::TPtr CreateRetryState() const = 0;

    //!
    //! Default implementations.
    //!

    static TPtr GetNoRetryPolicy(); // Denies all kind of retries.

    //! Randomized exponential backoff policy.
    static TPtr GetExponentialBackoffPolicy(TRetryClassFunction retryClassFunction,
                                            TDuration minDelay = TDuration::MilliSeconds(10),
                                            // Delay for statuses that require waiting before retry (such as OVERLOADED).
                                            TDuration minLongRetryDelay = TDuration::MilliSeconds(200),
                                            TDuration maxDelay = TDuration::Seconds(30),
                                            size_t maxRetries = std::numeric_limits<size_t>::max(),
                                            TDuration maxTime = TDuration::Max(),
                                            double scaleFactor = 2.0);

    //! Randomized fixed interval policy.
    static TPtr GetFixedIntervalPolicy(TRetryClassFunction retryClassFunction,
                                       TDuration delay = TDuration::MilliSeconds(100),
                                       // Delay for statuses that require waiting before retry (such as OVERLOADED).
                                       TDuration longRetryDelay = TDuration::MilliSeconds(300),
                                       size_t maxRetries = std::numeric_limits<size_t>::max(),
                                       TDuration maxTime = TDuration::Max());
};

template <class... TArgs>
struct TNoRetryPolicy : IRetryPolicy<TArgs...> {
    using IRetryState = typename IRetryPolicy<TArgs...>::IRetryState;

    struct TNoRetryState : IRetryState {
        TMaybe<TDuration> GetNextRetryDelay(typename TTypeTraits<TArgs>::TFuncParam...) override {
            return Nothing();
        }
    };

    typename IRetryState::TPtr CreateRetryState() const override {
        return std::make_unique<TNoRetryState>();
    }
};

namespace NRetryDetails {
inline TDuration RandomizeDelay(TDuration baseDelay) {
    const TDuration::TValue half = baseDelay.GetValue() / 2;
    return TDuration::FromValue(half + RandomNumber<TDuration::TValue>(half));
}
} // namespace NRetryDetails

template <class... TArgs>
struct TExponentialBackoffPolicy : IRetryPolicy<TArgs...> {
    using IRetryPolicy = IRetryPolicy<TArgs...>;
    using IRetryState = typename IRetryPolicy::IRetryState;

    struct TExponentialBackoffState : IRetryState {
        TExponentialBackoffState(typename IRetryPolicy::TRetryClassFunction retryClassFunction,
                                 TDuration minDelay,
                                 TDuration minLongRetryDelay,
                                 TDuration maxDelay,
                                 size_t maxRetries,
                                 TDuration maxTime,
                                 double scaleFactor)
            : MinLongRetryDelay(minLongRetryDelay)
            , MaxDelay(maxDelay)
            , MaxRetries(maxRetries)
            , MaxTime(maxTime)
            , ScaleFactor(scaleFactor)
            , StartTime(maxTime != TDuration::Max() ? TInstant::Now() : TInstant::Zero())
            , CurrentDelay(minDelay)
            , AttemptsDone(0)
            , RetryClassFunction(std::move(retryClassFunction))
        {
        }

        TMaybe<TDuration> GetNextRetryDelay(typename TTypeTraits<TArgs>::TFuncParam... args) override {
            const ERetryErrorClass errorClass = RetryClassFunction(args...);
            if (errorClass == ERetryErrorClass::NoRetry || AttemptsDone >= MaxRetries || StartTime && TInstant::Now() - StartTime >= MaxTime) {
                return Nothing();
            }

            if (errorClass == ERetryErrorClass::LongRetry) {
                CurrentDelay = Max(CurrentDelay, MinLongRetryDelay);
            }

            const TDuration delay = NRetryDetails::RandomizeDelay(CurrentDelay);

            if (CurrentDelay < MaxDelay) {
                CurrentDelay = Min(CurrentDelay * ScaleFactor, MaxDelay);
            }

            ++AttemptsDone;
            return delay;
        }

        const TDuration MinLongRetryDelay;
        const TDuration MaxDelay;
        const size_t MaxRetries;
        const TDuration MaxTime;
        const double ScaleFactor;
        const TInstant StartTime;
        TDuration CurrentDelay;
        size_t AttemptsDone;
        typename IRetryPolicy::TRetryClassFunction RetryClassFunction;
    };

    TExponentialBackoffPolicy(typename IRetryPolicy::TRetryClassFunction retryClassFunction,
                              TDuration minDelay,
                              TDuration minLongRetryDelay,
                              TDuration maxDelay,
                              size_t maxRetries,
                              TDuration maxTime,
                              double scaleFactor)
        : MinDelay(minDelay)
        , MinLongRetryDelay(minLongRetryDelay)
        , MaxDelay(maxDelay)
        , MaxRetries(maxRetries)
        , MaxTime(maxTime)
        , ScaleFactor(scaleFactor)
        , RetryClassFunction(std::move(retryClassFunction))
    {
        Y_ASSERT(RetryClassFunction);
        Y_ASSERT(MinDelay < MaxDelay);
        Y_ASSERT(MinLongRetryDelay < MaxDelay);
        Y_ASSERT(MinLongRetryDelay >= MinDelay);
        Y_ASSERT(ScaleFactor > 1.0);
        Y_ASSERT(MaxRetries > 0);
        Y_ASSERT(MaxTime > MinDelay);
    }

    typename IRetryState::TPtr CreateRetryState() const override {
        return std::make_unique<TExponentialBackoffState>(RetryClassFunction, MinDelay, MinLongRetryDelay, MaxDelay, MaxRetries, MaxTime, ScaleFactor);
    }

    const TDuration MinDelay;
    const TDuration MinLongRetryDelay;
    const TDuration MaxDelay;
    const size_t MaxRetries;
    const TDuration MaxTime;
    const double ScaleFactor;
    typename IRetryPolicy::TRetryClassFunction RetryClassFunction;
};

template <class... TArgs>
struct TFixedIntervalPolicy : IRetryPolicy<TArgs...> {
    using IRetryPolicy = IRetryPolicy<TArgs...>;
    using IRetryState = typename IRetryPolicy::IRetryState;

    struct TFixedIntervalState : IRetryState {
        TFixedIntervalState(typename IRetryPolicy::TRetryClassFunction retryClassFunction,
                            TDuration delay,
                            TDuration longRetryDelay,
                            size_t maxRetries,
                            TDuration maxTime)
            : Delay(delay)
            , LongRetryDelay(longRetryDelay)
            , MaxRetries(maxRetries)
            , MaxTime(maxTime)
            , StartTime(maxTime != TDuration::Max() ? TInstant::Now() : TInstant::Zero())
            , AttemptsDone(0)
            , RetryClassFunction(std::move(retryClassFunction))
        {
        }

        TMaybe<TDuration> GetNextRetryDelay(typename TTypeTraits<TArgs>::TFuncParam... args) override {
            const ERetryErrorClass errorClass = RetryClassFunction(args...);
            if (errorClass == ERetryErrorClass::NoRetry || AttemptsDone >= MaxRetries || StartTime && TInstant::Now() - StartTime >= MaxTime) {
                return Nothing();
            }

            const TDuration delay = NRetryDetails::RandomizeDelay(errorClass == ERetryErrorClass::LongRetry ? LongRetryDelay : Delay);

            ++AttemptsDone;
            return delay;
        }

        const TDuration Delay;
        const TDuration LongRetryDelay;
        const size_t MaxRetries;
        const TDuration MaxTime;
        const TInstant StartTime;
        size_t AttemptsDone;
        typename IRetryPolicy::TRetryClassFunction RetryClassFunction;
    };

    TFixedIntervalPolicy(typename IRetryPolicy::TRetryClassFunction retryClassFunction,
                         TDuration delay,
                         TDuration longRetryDelay,
                         size_t maxRetries,
                         TDuration maxTime)
        : Delay(delay)
        , LongRetryDelay(longRetryDelay)
        , MaxRetries(maxRetries)
        , MaxTime(maxTime)
        , RetryClassFunction(std::move(retryClassFunction))
    {
        Y_ASSERT(RetryClassFunction);
        Y_ASSERT(MaxTime > Delay);
        Y_ASSERT(MaxTime > LongRetryDelay);
        Y_ASSERT(LongRetryDelay >= Delay);
    }

    typename IRetryState::TPtr CreateRetryState() const override {
        return std::make_unique<TFixedIntervalState>(RetryClassFunction, Delay, LongRetryDelay, MaxRetries, MaxTime);
    }

    const TDuration Delay;
    const TDuration LongRetryDelay;
    const size_t MaxRetries;
    const TDuration MaxTime;
    typename IRetryPolicy::TRetryClassFunction RetryClassFunction;
};

template <class... TArgs>
typename IRetryPolicy<TArgs...>::TPtr IRetryPolicy<TArgs...>::GetNoRetryPolicy() {
    return std::make_shared<TNoRetryPolicy<TArgs...>>();
}

template <class... TArgs>
typename IRetryPolicy<TArgs...>::TPtr IRetryPolicy<TArgs...>::GetExponentialBackoffPolicy(TRetryClassFunction retryClassFunction,
                                                                                          TDuration minDelay,
                                                                                          TDuration minLongRetryDelay,
                                                                                          TDuration maxDelay,
                                                                                          size_t maxRetries,
                                                                                          TDuration maxTime,
                                                                                          double scaleFactor)
{
    return std::make_shared<TExponentialBackoffPolicy<TArgs...>>(std::move(retryClassFunction), minDelay, minLongRetryDelay, maxDelay, maxRetries, maxTime, scaleFactor);
}

template <class... TArgs>
typename IRetryPolicy<TArgs...>::TPtr IRetryPolicy<TArgs...>::GetFixedIntervalPolicy(TRetryClassFunction retryClassFunction,
                                                                                     TDuration delay,
                                                                                     TDuration longRetryDelay,
                                                                                     size_t maxRetries,
                                                                                     TDuration maxTime)
{
    return std::make_shared<TFixedIntervalPolicy<TArgs...>>(std::move(retryClassFunction), delay, longRetryDelay, maxRetries, maxTime);
}
