#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/actions/invoker.h>
#include <yt/yt/core/actions/future.h>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TActionQueueTest, CurrentInvoker)
{
    auto queue = New<TActionQueue>("Test");
    auto invoker = queue->GetInvoker();

    auto future = BIND([&] {
        EXPECT_EQ(invoker.Get(), GetCurrentInvoker());
        // The invoker must remain current across a context switch.
        Yield();
        EXPECT_EQ(invoker.Get(), GetCurrentInvoker());
    })
        .AsyncVia(invoker)
        .Run();

    WaitUntilSet(future);
    queue->Shutdown();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency
