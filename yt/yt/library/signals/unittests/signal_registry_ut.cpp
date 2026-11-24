#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/signals/signal_registry.h>

#include <signal.h>

namespace NYT::NSignals {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

#ifndef _win_

TEST(TSignalRegistryTest, PushCallbackJustWorks)
{
#ifdef _linux_
    auto signal = SIGRTMIN + 1;
#elif defined(_darwin_)
    auto signal = SIGUSR2;
#else
    #error Unsupported platform
#endif
    auto promise = NewPromise<void>();

    TSignalRegistry::Get()->PushCallback(signal, [promise] () {
        promise.Set();
    });

    auto future = promise.ToFuture();

    raise(signal);

    EXPECT_TRUE(WaitForFast(future.WithTimeout(TDuration::Seconds(5))).IsOK());
}

#endif // _win_

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignals
