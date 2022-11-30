#pragma once

#include "unistat.h"

#include <util/datetime/base.h>
#include <util/generic/noncopyable.h>
#include <util/generic/va_args.h>
#include <util/generic/yexception.h>
#include <util/system/defaults.h>

class TUnistatTimer: public TNonCopyable {
public:
    template <typename T>
    TUnistatTimer(TUnistat& unistat, T&& holename)
        : Started_(Now())
        , HoleName_(ToString(holename))
        , Aggregator_(unistat)
    {
    }

    ~TUnistatTimer() {
        if (!Dismiss_) {
            Aggregator_.PushSignalUnsafe(HoleName_, (Now() - Started_).MillisecondsFloat());
        }
    }

    void Dismiss() noexcept {
        Dismiss_ = true;
    }

    void Accept() noexcept {
        Dismiss_ = false;
    }

private:
    bool Dismiss_{false};
    const TInstant Started_;
    const TString HoleName_;
    TUnistat& Aggregator_;
};

class TUnistatExceptionCounter: public TNonCopyable {
public:
    template <typename T, typename U>
    TUnistatExceptionCounter(TUnistat& unistat, T&& hasExceptionHolename, U&& noExceptionHolename)
        : HasExceptionHoleName_(ToString(hasExceptionHolename))
        , NoExceptionHoleName_(ToString(noExceptionHolename))
        , Aggregator_(unistat)
    {
    }

    template <typename T>
    TUnistatExceptionCounter(TUnistat& unistat, T&& hasExceptionHolename)
        : HasExceptionHoleName_(ToString(hasExceptionHolename))
        , Aggregator_(unistat)
    {
    }

    ~TUnistatExceptionCounter() {
        if (!Dismiss_) {
            if (UncaughtException()) {
                Aggregator_.PushSignalUnsafe(HasExceptionHoleName_, 1.);
            } else if (NoExceptionHoleName_) {
                Aggregator_.PushSignalUnsafe(NoExceptionHoleName_, 1.);
            }
        }
    }

    void Dismiss() noexcept {
        Dismiss_ = true;
    }

    void Accept() noexcept {
        Dismiss_ = false;
    }

private:
    bool Dismiss_{false};
    const TString HasExceptionHoleName_;
    const TString NoExceptionHoleName_;
    TUnistat& Aggregator_;
};

/**
 * @def Y_UNISTAT_TIMER
 *
 * Macro is needed to time scope and push time into aggregator.
 *
 * @code
 * void DoSomethingImportant() {
 *     Y_UNISTAT_TIMER(TUnistat::Instance(), "doing-important-stuff")
 *     // doing something here
 * }
 * @endcode
 */
#define Y_UNISTAT_TIMER(unistat, holeName) \
    ::TUnistatTimer Y_GENERATE_UNIQUE_ID(timer){unistat, holeName};

#define Y_UNISTAT_EXCEPTION_COUNTER_IMPL_2(unistat, hasExceptionHoleName) \
    ::TUnistatExceptionCounter Y_GENERATE_UNIQUE_ID(exceptionCounter){unistat, hasExceptionHoleName};

#define Y_UNISTAT_EXCEPTION_COUNTER_IMPL_3(unistat, hasExceptionHolename, noExceptionHolename) \
    ::TUnistatExceptionCounter Y_GENERATE_UNIQUE_ID(exceptionCounter){unistat, hasExceptionHolename, noExceptionHolename};

#define Y_UNISTAT_EXCEPTION_COUNTER_IMPL_DISPATCHER(_1, _2, _3, NAME, ...) NAME

/**
 * @def Y_UNISTAT_EXCEPTION_COUNTER
 *
 * Macro is needed to check if there was an exception on scope exit or not.
 *
 * @code
 * void DoSomethingThatMayThrowException() {
 *     Y_UNISTAT_EXCEPTION_COUNTER(TUnistat::Instance(), "exception_occured", "no_exception")
 *     Y_UNISTAT_EXCEPTION_COUNTER(TUnistat::Instance(), "wow_exception_occured")
 *     // doing something here
 * }
 * @endcode
 */
#define Y_UNISTAT_EXCEPTION_COUNTER(...) Y_PASS_VA_ARGS(Y_UNISTAT_EXCEPTION_COUNTER_IMPL_DISPATCHER(__VA_ARGS__, Y_UNISTAT_EXCEPTION_COUNTER_IMPL_3, Y_UNISTAT_EXCEPTION_COUNTER_IMPL_2)(__VA_ARGS__))
