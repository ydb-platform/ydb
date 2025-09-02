#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/signals/signal_registry.h>

#include <signal.h>

namespace NYT::NSignals {

////////////////////////////////////////////////////////////////////////////////

#ifndef _win_

TEST(TSignalRegistryTest, PushCallbackJustWorks)
{
    auto promise = NewPromise<void>();

    TSignalRegistry::Get()->PushCallback(SIGRTMIN + 1, [promise] () {
        promise.Set();
    });

    auto future = promise.ToFuture();

    raise(SIGRTMIN + 1);

    EXPECT_TRUE(future.WithTimeout(TDuration::Seconds(5)).Get().IsOK());
}

#endif // _win_

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignals
