#include "executor.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>

namespace NYdb::NBS {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration WaitTimeout = TDuration::Seconds(5);

struct TTestRequest {};
using TTestResponse = TResultOrError<int>;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TExecutorTest)
{
    Y_UNIT_TEST(ShouldHandleRequests)
    {
        auto executor = TExecutor::Create("TEST");
        executor->Start();

        auto future = executor->Execute([] {
            return 42;
        });

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

        auto future = executor->Execute([=] () mutable {
            request.SetValue({});

            auto resp = executor->ExtractResponse(response.GetFuture());
            UNIT_ASSERT(!HasError(resp));

            return resp.GetResult();
        });

        request.GetFuture().GetValue(WaitTimeout);
        response.SetValue({ 42 });

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

        auto future = executor->Execute([=] () mutable {
            request.SetValue({});

            auto resp = executor->ExtractResponse(response.GetFuture());
            UNIT_ASSERT(!HasError(resp));

            return resp.GetResult();
        });

        request.GetFuture().GetValue(WaitTimeout);
        response.SetValue({ 42 });

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

        auto future = executor->Execute([=] () mutable {
            request.SetValue({});

            auto resp = executor->ResultOrError(response.GetFuture());
            UNIT_ASSERT(!HasError(resp));

            return resp.GetResult();
        });

        request.GetFuture().GetValue(WaitTimeout);
        response.SetValue({ 42 });

        auto result = future.GetValue(WaitTimeout);
        UNIT_ASSERT(result == 42);

        executor->Stop();
    }
}

}   // namespace NYdb::NBS
