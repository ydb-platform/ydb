#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/watchdog_invoker.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/actions/invoker.h>
#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TWatchdogInvokerTest, CurrentInvoker)
{
    auto queue = New<TActionQueue>("Test");
    auto invoker = CreateWatchdogInvoker(
        queue->GetInvoker(),
        NLogging::TLogger("Test"),
        TDuration::Seconds(60));

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
