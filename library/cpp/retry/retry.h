#pragma once
#include "retry_policy.h"
#include "utils.h"

#include <library/cpp/retry/protos/retry_options.pb.h>

#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <util/generic/typetraits.h>
#include <util/generic/yexception.h>
#include <functional>

struct TRetryOptions {
    ui32 RetryCount;

    // TotalDuration = SleepDuration +/- SleepRandomDelta  + (attempt * SleepIncrement) + (2**attempt * SleepExponentialMultiplier)
    TDuration SleepDuration;
    TDuration SleepRandomDelta;
    TDuration SleepIncrement;
    TDuration SleepExponentialMultiplier;

    std::function<void(TDuration)> SleepFunction;

    TRetryOptions(ui32 retryCount = 3, TDuration sleepDuration = TDuration::Seconds(1), TDuration sleepRandomDelta = TDuration::Zero(),
                  TDuration sleepIncrement = TDuration::Zero(), TDuration sleepExponentialMultiplier = TDuration::Zero(),
                  std::function<void(TDuration)> sleepFunction = [](TDuration d) { Sleep(d); }) // can't use Sleep itself due to Win compilation error
        : RetryCount(retryCount)
        , SleepDuration(sleepDuration)
        , SleepRandomDelta(sleepRandomDelta)
        , SleepIncrement(sleepIncrement)
        , SleepExponentialMultiplier(sleepExponentialMultiplier)
        , SleepFunction(sleepFunction)
    {
    }

    TRetryOptions& WithCount(ui32 retryCount) {
        RetryCount = retryCount;
        return *this;
    }

    TRetryOptions& WithSleep(TDuration sleepDuration) {
        SleepDuration = sleepDuration;
        return *this;
    }

    TRetryOptions& WithRandomDelta(TDuration sleepRandomDelta) {
        SleepRandomDelta = sleepRandomDelta;
        return *this;
    }

    TRetryOptions& WithIncrement(TDuration sleepIncrement) {
        SleepIncrement = sleepIncrement;
        return *this;
    }

    TRetryOptions& WithExponentialMultiplier(TDuration sleepExponentialMultiplier) {
        SleepExponentialMultiplier = sleepExponentialMultiplier;
        return *this;
    }

    TRetryOptions& WithSleepFunction(std::function<void(TDuration)> sleepFunction) {
        SleepFunction = sleepFunction;
        return *this;
    }

    // for compatibility attempt == 0 by default
    TDuration GetTimeToSleep(ui32 attempt = 0) const {
        return SleepDuration + NRetryPrivate::AddRandomDelta(SleepRandomDelta) + NRetryPrivate::AddIncrement(attempt, SleepIncrement) + NRetryPrivate::AddExponentialMultiplier(attempt, SleepExponentialMultiplier);
    }

    static TRetryOptions Count(ui32 retryCount) {
        return TRetryOptions(retryCount);
    }

    static TRetryOptions Default() {
        return TRetryOptions();
    }

    static TRetryOptions NoRetry() {
        return TRetryOptions(0);
    }
};

TRetryOptions MakeRetryOptions(const NRetry::TRetryOptionsPB& retryOptions);


namespace NRetryDetails {

template <class TException>
class TRetryOptionsPolicy : public IRetryPolicy<const TException&> {
public:
    explicit TRetryOptionsPolicy(const TRetryOptions& opts)
        : Opts(opts)
    {
    }

    using IRetryState = typename IRetryPolicy<const TException&>::IRetryState;

    class TRetryState : public IRetryState {
    public:
        explicit TRetryState(const TRetryOptions& opts)
            : Opts(opts)
        {
        }

        TMaybe<TDuration> GetNextRetryDelay(const TException&) override {
            if (Attempt == Opts.RetryCount) {
                return Nothing();
            }
            return Opts.GetTimeToSleep(Attempt++);
        }

    private:
        const TRetryOptions Opts;
        size_t Attempt = 0;
    };

    typename IRetryState::TPtr CreateRetryState() const override {
        return std::make_unique<TRetryState>(Opts);
    }

private:
    const TRetryOptions Opts;
};

} // namespace NRetryDetails

template <class TException>
typename IRetryPolicy<const TException&>::TPtr MakeRetryPolicy(const TRetryOptions& opts) {
    return std::make_shared<NRetryDetails::TRetryOptionsPolicy<TException>>(opts);
}

template <class TException>
typename IRetryPolicy<const TException&>::TPtr MakeRetryPolicy(const NRetry::TRetryOptionsPB& opts) {
    return MakeRetryPolicy<TException>(MakeRetryOptions(opts));
}

template <typename TResult, typename TException = yexception>
TMaybe<TResult> DoWithRetry(std::function<TResult()> func, const typename IRetryPolicy<const TException&>::TPtr& retryPolicy, bool throwLast = true, std::function<void(const TException&)> onFail = {}, std::function<void(TDuration)> sleepFunction = {}) {
    typename IRetryPolicy<const TException&>::IRetryState::TPtr retryState;
    while (true) {
        try {
            return func();
        } catch (const TException& ex) {
            if (onFail) {
                onFail(ex);
            }

            if (!retryState) {
                retryState = retryPolicy->CreateRetryState();
            }

            if (const TMaybe<TDuration> delay = retryState->GetNextRetryDelay(ex)) {
                if (*delay) {
                    if (sleepFunction) {
                        sleepFunction(*delay);
                    } else {
                        Sleep(*delay);
                    }
                }
            } else {
                if (throwLast) {
                    throw;
                }
                break;
            }
        }
    }
    return Nothing();
}

template <typename TResult, typename TException = yexception>
TMaybe<TResult> DoWithRetry(std::function<TResult()> func, std::function<void(const TException&)> onFail, TRetryOptions retryOptions, bool throwLast = true) {
    return DoWithRetry<TResult, TException>(std::move(func), MakeRetryPolicy<TException>(retryOptions), throwLast, std::move(onFail), retryOptions.SleepFunction);
}

template <typename TResult, typename TException = yexception>
TMaybe<TResult> DoWithRetry(std::function<TResult()> func, TRetryOptions retryOptions, bool throwLast = true) {
    return DoWithRetry<TResult, TException>(std::move(func), MakeRetryPolicy<TException>(retryOptions), throwLast, {}, retryOptions.SleepFunction);
}

template <typename TException = yexception>
bool DoWithRetry(std::function<void()> func, const typename IRetryPolicy<const TException&>::TPtr& retryPolicy, bool throwLast = true, std::function<void(const TException&)> onFail = {}, std::function<void(TDuration)> sleepFunction = {}) {
    auto f = [&]() {
        func();
        return nullptr;
    };
    return DoWithRetry<void*, TException>(f, retryPolicy, throwLast, std::move(onFail), std::move(sleepFunction)).Defined();
}

template <typename TException = yexception>
bool DoWithRetry(std::function<void()> func, std::function<void(const TException&)> onFail, TRetryOptions retryOptions, bool throwLast) {
    return DoWithRetry<TException>(std::move(func), MakeRetryPolicy<TException>(retryOptions), throwLast, onFail, retryOptions.SleepFunction);
}

template <typename TException = yexception>
bool DoWithRetry(std::function<void()> func, TRetryOptions retryOptions, bool throwLast = true) {
    return DoWithRetry<TException>(std::move(func), MakeRetryPolicy<TException>(retryOptions), throwLast, {}, retryOptions.SleepFunction);
}

template <class TRetCode>
TRetCode DoWithRetryOnRetCode(std::function<TRetCode()> func, const typename IRetryPolicy<TRetCode>::TPtr& retryPolicy, std::function<void(TDuration)> sleepFunction = {}) {
    auto retryState = retryPolicy->CreateRetryState();
    while (true) {
        TRetCode code = func();
        if (const TMaybe<TDuration> delay = retryState->GetNextRetryDelay(code)) {
            if (*delay) {
                if (sleepFunction) {
                    sleepFunction(*delay);
                } else {
                    Sleep(*delay);
                }
            }
        } else {
            return code;
        }
    }
}

bool DoWithRetryOnRetCode(std::function<bool()> func, TRetryOptions retryOptions);

Y_DECLARE_PODTYPE(TRetryOptions);
