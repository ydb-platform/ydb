#include "executor.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>

#include <memory>

namespace NYdb::NBS {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration WaitTimeout = TDuration::Seconds(5);

struct TTestRequest
{
};

using TTestResponse = TResultOrError<int>;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TExecutorTest)
{
    Y_UNIT_TEST(ShouldHandleRequests)
    {
        auto executor = TExecutor::Create("TEST");
        executor->Start();

        auto future = executor->Execute([] { return 42; });

        auto result = future.GetValue(WaitTimeout);
        UNIT_ASSERT(result == 42);

        executor->Stop();
    }

    Y_UNIT_TEST(ShouldWaitForFuture)
    {
        auto executor = TExecutor::Create("TEST");
        executor->Start();

        auto request = NewPromise<TTestRequest>();
        auto response = NewPromise<TTestResponse>();

        auto future = executor->Execute(
            [=]() mutable
            {
                request.SetValue({});

                auto resp = executor->ExtractResponse(response.GetFuture());
                UNIT_ASSERT(!HasError(resp));

                return resp.GetResult();
            });

        request.GetFuture().GetValue(WaitTimeout);
        response.SetValue({42});

        auto result = future.GetValue(WaitTimeout);
        UNIT_ASSERT(result == 42);

        executor->Stop();
    }

    Y_UNIT_TEST(ShouldExtractResponse)
    {
        auto executor = TExecutor::Create("TEST");
        executor->Start();

        auto request = NewPromise<TTestRequest>();
        auto response = NewPromise<TTestResponse>();

        auto future = executor->Execute(
            [=]() mutable
            {
                request.SetValue({});

                auto resp = executor->ExtractResponse(response.GetFuture());
                UNIT_ASSERT(!HasError(resp));

                return resp.GetResult();
            });

        request.GetFuture().GetValue(WaitTimeout);
        response.SetValue({42});

        auto result = future.GetValue(WaitTimeout);
        UNIT_ASSERT(result == 42);

        executor->Stop();
    }

    Y_UNIT_TEST(ShouldGetResultOrError)
    {
        auto executor = TExecutor::Create("TEST");
        executor->Start();

        auto request = NewPromise<TTestRequest>();
        auto response = NewPromise<int>();

        auto future = executor->Execute(
            [=]() mutable
            {
                request.SetValue({});

                auto resp = executor->ResultOrError(response.GetFuture());
                UNIT_ASSERT(!HasError(resp));

                return resp.GetResult();
            });

        request.GetFuture().GetValue(WaitTimeout);
        response.SetValue({42});

        auto result = future.GetValue(WaitTimeout);
        UNIT_ASSERT(result == 42);

        executor->Stop();
    }

    // Stop must cancel every WaitFor, including one started after the first
    // Abort. A second wait used to block the executor thread forever, so the
    // direct block group that owned the executor was never destroyed.
    Y_UNIT_TEST(ShouldCancelRepeatedWaitForOnStop)
    {
        auto executor = TExecutor::Create("TEST");
        executor->Start();

        auto started = NewPromise<void>();
        auto pending = NewPromise<void>();

        auto finished = executor->Execute(
            [executor, started, pending]() mutable
            {
                started.SetValue();
                executor->WaitFor(pending.GetFuture());
                executor->WaitFor(pending.GetFuture());
            });

        started.GetFuture().GetValue(WaitTimeout);
        executor->Stop();
        finished.GetValue(WaitTimeout);
    }

    // A task queued from a coroutine that Abort just resumed used to stay in
    // the dispatcher queue. The queue destructor dropped the pointer, so the
    // task and everything it owned leaked.
    Y_UNIT_TEST(ShouldDropTasksQueuedAfterStop)
    {
        auto executor = TExecutor::Create("TEST");
        executor->Start();

        auto started = NewPromise<void>();
        auto pending = NewPromise<void>();
        auto keptAlive = std::make_shared<int>(1);
        std::weak_ptr<int> weak = keptAlive;

        auto finished = executor->Execute(
            [executor, started, pending, keptAlive]() mutable
            {
                started.SetValue();
                executor->WaitFor(pending.GetFuture());
                executor->ExecuteSimple([keptAlive] { Y_UNUSED(keptAlive); });
            });

        started.GetFuture().GetValue(WaitTimeout);
        executor->Stop();
        finished.GetValue(WaitTimeout);

        keptAlive.reset();
        UNIT_ASSERT(weak.expired());
    }
}

}   // namespace NYdb::NBS
