#pragma once

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

template <typename TResult, typename TException = yexception>
TMaybe<TResult> DoWithRetry(std::function<TResult()> func, std::function<void(const TException&)> onFail, TRetryOptions retryOptions, bool throwLast) {
    for (ui32 attempt = 0; attempt <= retryOptions.RetryCount; ++attempt) {
        try {
            return func();
        } catch (TException& ex) {
            onFail(ex);
            if (attempt == retryOptions.RetryCount) {
                if (throwLast) {
                    throw;
                }
            } else {
                auto sleep = retryOptions.SleepFunction;
                sleep(retryOptions.GetTimeToSleep(attempt));
            }
        }
    }
    return Nothing();
}

template <typename TResult, typename TException = yexception>
TMaybe<TResult> DoWithRetry(std::function<TResult()> func, TRetryOptions retryOptions, bool throwLast) {
    return DoWithRetry<TResult, TException>(func, [](const TException&){}, retryOptions, throwLast);
}

template <typename TException = yexception>
bool DoWithRetry(std::function<void()> func, std::function<void(const TException&)> onFail, TRetryOptions retryOptions, bool throwLast) { 
    auto f = [&]() {
        func();
        return nullptr;
    };
    return DoWithRetry<void*, TException>(f, onFail, retryOptions, throwLast).Defined();
}

template <typename TException = yexception>
bool DoWithRetry(std::function<void()> func, TRetryOptions retryOptions, bool throwLast) {
    auto f = [&]() {
        func();
        return nullptr;
    };
    return DoWithRetry<void*, TException>(f, [](const TException&){}, retryOptions, throwLast).Defined();
}

void DoWithRetry(std::function<void()> func, TRetryOptions retryOptions);

bool DoWithRetryOnRetCode(std::function<bool()> func, TRetryOptions retryOptions);

Y_DECLARE_PODTYPE(TRetryOptions);

TRetryOptions MakeRetryOptions(const NRetry::TRetryOptionsPB& retryOptions);
