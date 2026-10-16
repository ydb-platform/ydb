#include "executor.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>

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
    Y_UNIT_TEST(ShouldReleaseTasksEnqueuedAfterStop)
    {
        auto alive = std::make_shared<int>(0);
        std::weak_ptr<int> weak = alive;
        bool ran = false;
        {
            auto executor = TExecutor::Create("TEST");
            executor->Start();
            executor->Stop();

            executor->ExecuteSimple([alive = std::move(alive), &ran] { ran = true; });
        }
        UNIT_ASSERT_C(weak.expired(), "a task enqueued after Stop was leaked");
        UNIT_ASSERT_C(!ran, "a task enqueued after Stop was executed");
    }

    Y_UNIT_TEST(ShouldReleaseFutureTasksEnqueuedAfterStop)
    {
        auto alive = std::make_shared<int>(0);
        std::weak_ptr<int> weak = alive;
        bool ran = false;
        TFuture<int> future;
        {
            auto executor = TExecutor::Create("TEST");
            executor->Start();
            executor->Stop();

            future = executor->Execute([alive = std::move(alive), &ran] { ran = true; return 42; });
        }
        // The captures are released; the future is not completed.
        UNIT_ASSERT_C(weak.expired(), "a future task enqueued after Stop was leaked");
        UNIT_ASSERT_C(!ran, "a future task enqueued after Stop was executed");
        UNIT_ASSERT(!future.HasValue() && !future.HasException());
    }

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
}

}   // namespace NYdb::NBS
