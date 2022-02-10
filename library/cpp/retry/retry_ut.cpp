#include "retry.h"

#include <library/cpp/testing/unittest/registar.h>

namespace {
    class TDoOnSecondOrThrow {
    public:
        ui32 operator()() {
            if (attempt++ != 1) {
                throw yexception();
            }
            return 42;
        }

    private:
        ui32 attempt = 0;
    };

    class TDoOnSecondOrFail {
    public:
        bool operator()() {
            return (attempt++ == 1);
        }

    private:
        ui32 attempt = 0;
    };
}

Y_UNIT_TEST_SUITE(Retry) {
    Y_UNIT_TEST(RetryOnExceptionSuccess) {
        UNIT_ASSERT_NO_EXCEPTION(DoWithRetry(TDoOnSecondOrThrow{}, TRetryOptions(1, TDuration::Zero())));
    }
    Y_UNIT_TEST(RetryOnExceptionSuccessWithOnFail) {
        ui32 value = 0;
        std::function<void(const yexception&)> cb = [&value](const yexception&){ value += 1; };
        UNIT_ASSERT_NO_EXCEPTION(DoWithRetry<ui32>(TDoOnSecondOrThrow{}, cb, TRetryOptions(1, TDuration::Zero()), true));
        UNIT_ASSERT_EQUAL(value, 1);
    }
    Y_UNIT_TEST(RetryOnExceptionFail) {
        UNIT_ASSERT_EXCEPTION(DoWithRetry(TDoOnSecondOrThrow{}, TRetryOptions(0, TDuration::Zero())), yexception);
    }
    Y_UNIT_TEST(RetryOnExceptionFailWithOnFail) {
        ui32 value = 0;
        std::function<void(const yexception&)> cb = [&value](const yexception&) { value += 1; };
        UNIT_ASSERT_EXCEPTION(DoWithRetry<ui32>(TDoOnSecondOrThrow{}, cb, TRetryOptions(0, TDuration::Zero()), true), yexception);
        UNIT_ASSERT_EQUAL(value, 1);
    }

    Y_UNIT_TEST(RetryOnExceptionSuccessWithValue) {
        std::function<ui32()> f = TDoOnSecondOrThrow{};
        UNIT_ASSERT(42 == *DoWithRetry<ui32>(f, TRetryOptions(1, TDuration::Zero()), false));
    }
    Y_UNIT_TEST(RetryOnExceptionSuccessWithValueWithOnFail) {
        ui32 value = 0;
        std::function<ui32()> f = TDoOnSecondOrThrow{};
        std::function<void(const yexception&)> cb = [&value](const yexception&){ value += 1; };
        UNIT_ASSERT(42 == *DoWithRetry<ui32>(f, cb, TRetryOptions(1, TDuration::Zero()), false));
        UNIT_ASSERT_EQUAL(value, 1);
    }
    Y_UNIT_TEST(RetryOnExceptionFailWithValue) {
        std::function<ui32()> f = TDoOnSecondOrThrow{};
        UNIT_ASSERT(!DoWithRetry<ui32>(f, TRetryOptions(0, TDuration::Zero()), false).Defined());
    }
    Y_UNIT_TEST(RetryOnExceptionFailWithValueWithOnFail) {
        ui32 value = 0;
        std::function<ui32()> f = TDoOnSecondOrThrow{};
        std::function<void(const yexception&)> cb = [&value](const yexception&){ value += 1; };
        UNIT_ASSERT(!DoWithRetry<ui32>(f, cb, TRetryOptions(0, TDuration::Zero()), false).Defined());
        UNIT_ASSERT_EQUAL(value, 1);
    }

    Y_UNIT_TEST(RetryOnExceptionSuccessWithValueAndRethrow) {
        std::function<ui32()> f = TDoOnSecondOrThrow{};
        UNIT_ASSERT(42 == *DoWithRetry<ui32>(f, TRetryOptions(1, TDuration::Zero()), true));
    }
    Y_UNIT_TEST(RetryOnExceptionSuccessWithValueAndRethrowWithOnFail) {
        ui32 value = 0;
        std::function<ui32()> f = TDoOnSecondOrThrow{};
        std::function<void(const yexception&)> cb = [&value](const yexception&){ value += 1; };
        UNIT_ASSERT(42 == *DoWithRetry<ui32>(f, cb, TRetryOptions(1, TDuration::Zero()), true));
        UNIT_ASSERT_EQUAL(value, 1);
    }
    Y_UNIT_TEST(RetryOnExceptionFailWithValueAndRethrow) {
        std::function<ui32()> f = TDoOnSecondOrThrow{};
        UNIT_ASSERT_EXCEPTION(DoWithRetry<ui32>(f, TRetryOptions(0, TDuration::Zero()), true), yexception);
    }
    Y_UNIT_TEST(RetryOnExceptionFailWithValueAndRethrowWithOnFail) {
        ui32 value = 0;
        std::function<ui32()> f = TDoOnSecondOrThrow{};
        std::function<void(const yexception&)> cb = [&value](const yexception&){ value += 1; };
        UNIT_ASSERT_EXCEPTION(42 == *DoWithRetry<ui32>(f, cb, TRetryOptions(0, TDuration::Zero()), true), yexception);
        UNIT_ASSERT_EQUAL(value, 1);
    }

    Y_UNIT_TEST(RetryOnRetCodeSuccess) {
        UNIT_ASSERT(true == DoWithRetryOnRetCode(TDoOnSecondOrFail{}, TRetryOptions(1, TDuration::Zero())));
    }
    Y_UNIT_TEST(RetryOnRetCodeFail) {
        UNIT_ASSERT(false == DoWithRetryOnRetCode(TDoOnSecondOrFail{}, TRetryOptions(0, TDuration::Zero())));
    }
    Y_UNIT_TEST(MakeRetryOptionsFromProto) {
        NRetry::TRetryOptionsPB protoOptions;
        protoOptions.SetMaxTries(1);
        protoOptions.SetInitialSleepMs(2);
        protoOptions.SetSleepIncrementMs(3);
        protoOptions.SetRandomDeltaMs(4);
        protoOptions.SetExponentalMultiplierMs(5);

        const TRetryOptions options = MakeRetryOptions(protoOptions);
        UNIT_ASSERT_EQUAL(options.RetryCount, 1);
        UNIT_ASSERT_EQUAL(options.SleepDuration, TDuration::MilliSeconds(2));
        UNIT_ASSERT_EQUAL(options.SleepIncrement, TDuration::MilliSeconds(3));
        UNIT_ASSERT_EQUAL(options.SleepRandomDelta, TDuration::MilliSeconds(4));
        UNIT_ASSERT_EQUAL(options.SleepExponentialMultiplier, TDuration::MilliSeconds(5));
    }
}
