#include "durable_wrapper.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/storage_test.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/scheduler_test.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/timer_test.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/deque.h>

namespace NYdb::NBS::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr auto WaitTimeout = TDuration::Seconds(5);

////////////////////////////////////////////////////////////////////////////////

struct TTestEnvironment
{
    std::shared_ptr<TTestStorage> Storage;
    std::shared_ptr<TTestScheduler> Scheduler;
    std::shared_ptr<TTestTimer> Timer;
    IStoragePtr Wrapper;

    // Deferred promises returned by the underlying storage for every attempt.
    TDeque<TPromise<TReadBlocksLocalResponse>> ReadPromises;
    TDeque<TPromise<TWriteBlocksLocalResponse>> WritePromises;
    TDeque<TPromise<TZeroBlocksLocalResponse>> ZeroPromises;

    ui32 ReadCount = 0;
    ui32 WriteCount = 0;
    ui32 ZeroCount = 0;

    TTestEnvironment()
    {
        Storage = std::make_shared<TTestStorage>();
        // Start the scheduler clock at zero so that AdvanceTime controls when
        // scheduled retries become due (the default clock is TInstant::Max()).
        Scheduler = std::make_shared<TTestScheduler>(TInstant::Zero());
        Timer = std::make_shared<TTestTimer>();
        Wrapper = CreateDurableStorageWrapper(Storage, Timer, Scheduler);

        Storage->ReadBlocksLocalHandler =
            [&](TCallContextPtr callContext,
                std::shared_ptr<TReadBlocksLocalRequest> request)
            -> TFuture<TReadBlocksLocalResponse>
        {
            Y_UNUSED(callContext);
            Y_UNUSED(request);
            ++ReadCount;
            auto promise = NewPromise<TReadBlocksLocalResponse>();
            ReadPromises.push_back(promise);
            return promise;
        };

        Storage->WriteBlocksLocalHandler =
            [&](TCallContextPtr callContext,
                std::shared_ptr<TWriteBlocksLocalRequest> request)
            -> TFuture<TWriteBlocksLocalResponse>
        {
            Y_UNUSED(callContext);
            Y_UNUSED(request);
            ++WriteCount;
            auto promise = NewPromise<TWriteBlocksLocalResponse>();
            WritePromises.push_back(promise);
            return promise;
        };

        Storage->ZeroBlocksLocalHandler =
            [&](TCallContextPtr callContext,
                std::shared_ptr<TZeroBlocksLocalRequest> request)
            -> TFuture<TZeroBlocksLocalResponse>
        {
            Y_UNUSED(callContext);
            Y_UNUSED(request);
            ++ZeroCount;
            auto promise = NewPromise<TZeroBlocksLocalResponse>();
            ZeroPromises.push_back(promise);
            return promise;
        };
    }

    std::shared_ptr<TZeroBlocksLocalRequest> MakeZeroRequest()
    {
        return std::make_shared<TZeroBlocksLocalRequest>(TRequestHeaders{});
    }

    std::shared_ptr<TReadBlocksLocalRequest> MakeReadRequest()
    {
        return std::make_shared<TReadBlocksLocalRequest>(TRequestHeaders{});
    }

    std::shared_ptr<TWriteBlocksLocalRequest> MakeWriteRequest()
    {
        return std::make_shared<TWriteBlocksLocalRequest>(TRequestHeaders{});
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDurableStorageWrapperTest)
{
    Y_UNIT_TEST(ShouldReturnSuccessWithoutRetry)
    {
        TTestEnvironment env;

        auto future = env.Wrapper->ZeroBlocksLocal(
            MakeIntrusive<TCallContext>(),
            env.MakeZeroRequest());

        UNIT_ASSERT_VALUES_EQUAL(1, env.ZeroCount);
        UNIT_ASSERT_VALUES_EQUAL(1, env.ZeroPromises.size());
        UNIT_ASSERT(!future.HasValue());

        // Complete with success.
        env.ZeroPromises.front().SetValue(TZeroBlocksLocalResponse());

        UNIT_ASSERT(future.HasValue());
        const auto& result = future.GetValue(WaitTimeout);
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result.Error.GetCode(),
            FormatError(result.Error));

        // No retry must have been scheduled or executed.
        UNIT_ASSERT_VALUES_EQUAL(1, env.ZeroCount);
    }

    Y_UNIT_TEST(ShouldRetryRetriableErrorUntilSuccess)
    {
        TTestEnvironment env;

        auto future = env.Wrapper->ZeroBlocksLocal(
            MakeIntrusive<TCallContext>(),
            env.MakeZeroRequest());

        UNIT_ASSERT_VALUES_EQUAL(1, env.ZeroCount);

        // First attempt fails with a retriable error.
        env.ZeroPromises.front().SetValue(
            TZeroBlocksLocalResponse{.Error = MakeError(E_REJECTED)});

        // Retry must have been scheduled, not executed yet.
        UNIT_ASSERT(!future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(1, env.ZeroCount);

        // Fire the scheduled retry.
        env.Scheduler->RunAllScheduledTasks();

        // Second attempt has been issued.
        UNIT_ASSERT_VALUES_EQUAL(2, env.ZeroCount);
        UNIT_ASSERT_VALUES_EQUAL(2, env.ZeroPromises.size());
        UNIT_ASSERT(!future.HasValue());

        // Second attempt succeeds.
        env.ZeroPromises.back().SetValue(TZeroBlocksLocalResponse());

        UNIT_ASSERT(future.HasValue());
        const auto& result = future.GetValue(WaitTimeout);
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result.Error.GetCode(),
            FormatError(result.Error));
    }

    Y_UNIT_TEST(ShouldRetryMultipleTimes)
    {
        TTestEnvironment env;

        auto future = env.Wrapper->ZeroBlocksLocal(
            MakeIntrusive<TCallContext>(),
            env.MakeZeroRequest());

        constexpr ui32 FailedAttempts = 3;
        for (ui32 i = 0; i < FailedAttempts; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(i + 1, env.ZeroCount);
            env.ZeroPromises.back().SetValue(
                TZeroBlocksLocalResponse{.Error = MakeError(E_REJECTED)});
            UNIT_ASSERT(!future.HasValue());
            env.Scheduler->RunAllScheduledTasks();
        }

        // The last attempt finally succeeds.
        UNIT_ASSERT_VALUES_EQUAL(FailedAttempts + 1, env.ZeroCount);
        env.ZeroPromises.back().SetValue(TZeroBlocksLocalResponse());

        UNIT_ASSERT(future.HasValue());
        const auto& result = future.GetValue(WaitTimeout);
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result.Error.GetCode(),
            FormatError(result.Error));
    }

    Y_UNIT_TEST(ShouldNotRetryNeverRetriableErrors)
    {
        const EWellKnownResultCodes neverRetriable[] = {
            E_CANCELLED,
            E_ARGUMENT,
            E_IO_SILENT,
            E_IO,
        };

        for (auto code: neverRetriable) {
            TTestEnvironment env;

            auto future = env.Wrapper->ZeroBlocksLocal(
                MakeIntrusive<TCallContext>(),
                env.MakeZeroRequest());

            UNIT_ASSERT_VALUES_EQUAL(1, env.ZeroCount);

            env.ZeroPromises.front().SetValue(
                TZeroBlocksLocalResponse{.Error = MakeError(code)});

            // Error must be propagated immediately without any retry.
            UNIT_ASSERT_C(future.HasValue(), FormatResultCode(code));
            const auto& result = future.GetValue(WaitTimeout);
            UNIT_ASSERT_VALUES_EQUAL_C(
                code,
                result.Error.GetCode(),
                FormatError(result.Error));
            UNIT_ASSERT_VALUES_EQUAL(1, env.ZeroCount);
        }
    }

    Y_UNIT_TEST(ShouldRetryReadRequests)
    {
        TTestEnvironment env;

        auto future = env.Wrapper->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(),
            env.MakeReadRequest());

        UNIT_ASSERT_VALUES_EQUAL(1, env.ReadCount);

        env.ReadPromises.front().SetValue(
            TReadBlocksLocalResponse{.Error = MakeError(E_REJECTED)});
        UNIT_ASSERT(!future.HasValue());

        env.Scheduler->RunAllScheduledTasks();
        UNIT_ASSERT_VALUES_EQUAL(2, env.ReadCount);

        env.ReadPromises.back().SetValue(TReadBlocksLocalResponse());

        UNIT_ASSERT(future.HasValue());
        const auto& result = future.GetValue(WaitTimeout);
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result.Error.GetCode(),
            FormatError(result.Error));
    }

    Y_UNIT_TEST(ShouldRetryWriteRequests)
    {
        TTestEnvironment env;

        auto future = env.Wrapper->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(),
            env.MakeWriteRequest());

        UNIT_ASSERT_VALUES_EQUAL(1, env.WriteCount);

        env.WritePromises.front().SetValue(
            TWriteBlocksLocalResponse{.Error = MakeError(E_REJECTED)});
        UNIT_ASSERT(!future.HasValue());

        env.Scheduler->RunAllScheduledTasks();
        UNIT_ASSERT_VALUES_EQUAL(2, env.WriteCount);

        env.WritePromises.back().SetValue(TWriteBlocksLocalResponse());

        UNIT_ASSERT(future.HasValue());
        const auto& result = future.GetValue(WaitTimeout);
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result.Error.GetCode(),
            FormatError(result.Error));
    }

    Y_UNIT_TEST(ShouldHandleConcurrentRequestsIndependently)
    {
        TTestEnvironment env;

        auto future1 = env.Wrapper->ZeroBlocksLocal(
            MakeIntrusive<TCallContext>(),
            env.MakeZeroRequest());
        auto future2 = env.Wrapper->ZeroBlocksLocal(
            MakeIntrusive<TCallContext>(),
            env.MakeZeroRequest());

        UNIT_ASSERT_VALUES_EQUAL(2, env.ZeroCount);
        UNIT_ASSERT_VALUES_EQUAL(2, env.ZeroPromises.size());

        // First request succeeds immediately.
        env.ZeroPromises[0].SetValue(TZeroBlocksLocalResponse());
        UNIT_ASSERT(future1.HasValue());
        UNIT_ASSERT(!future2.HasValue());

        // Second request fails and gets retried.
        env.ZeroPromises[1].SetValue(
            TZeroBlocksLocalResponse{.Error = MakeError(E_REJECTED)});
        UNIT_ASSERT(!future2.HasValue());

        env.Scheduler->RunAllScheduledTasks();
        UNIT_ASSERT_VALUES_EQUAL(3, env.ZeroCount);

        env.ZeroPromises.back().SetValue(TZeroBlocksLocalResponse());
        UNIT_ASSERT(future2.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            future2.GetValue(WaitTimeout).Error.GetCode());
    }

    Y_UNIT_TEST(ShouldScheduleRetryWithBackoffDelay)
    {
        // The initial backoff delay in the wrapper is 100ms.
        constexpr auto InitialDelay = TDuration::MilliSeconds(100);

        TTestEnvironment env;

        // Timer starts at 0, so the retry is scheduled at deadline == 100ms.
        auto future = env.Wrapper->ZeroBlocksLocal(
            MakeIntrusive<TCallContext>(),
            env.MakeZeroRequest());

        env.ZeroPromises.front().SetValue(
            TZeroBlocksLocalResponse{.Error = MakeError(E_REJECTED)});

        // Before the delay elapses the retry must not fire.
        env.Scheduler->AdvanceTime(InitialDelay - TDuration::MilliSeconds(1));
        env.Scheduler->RunAllScheduledTasksUntilNow();
        UNIT_ASSERT_VALUES_EQUAL(1, env.ZeroCount);
        UNIT_ASSERT(!future.HasValue());

        // Once the delay is reached the retry fires.
        env.Scheduler->AdvanceTime(TDuration::MilliSeconds(1));
        env.Scheduler->RunAllScheduledTasksUntilNow();
        UNIT_ASSERT_VALUES_EQUAL(2, env.ZeroCount);

        env.ZeroPromises.back().SetValue(TZeroBlocksLocalResponse());
        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            future.GetValue(WaitTimeout).Error.GetCode());
    }

    Y_UNIT_TEST(ShouldForwardReportIOError)
    {
        TTestEnvironment env;

        UNIT_ASSERT_VALUES_EQUAL(0, env.Storage->ErrorCount);
        env.Wrapper->ReportIOError();
        UNIT_ASSERT_VALUES_EQUAL(1, env.Storage->ErrorCount);
        env.Wrapper->ReportIOError();
        UNIT_ASSERT_VALUES_EQUAL(2, env.Storage->ErrorCount);
    }
}

}   // namespace NYdb::NBS::NBlockStore
