#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/fiber.h>

#include <yt/yt/core/misc/finally.h>

#include <library/cpp/yt/system/event_count.h>

#include <thread>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TFiberTest, SetRunningTransitions)
{
    auto* fiber = TFiber::CreateFiber();
    fiber->SetRunning();
    auto releaseFiber = Finally([&] {
        TFiber::ReleaseFiber(fiber);
    });

    auto noop = [] { };
    EFiberState state;
    EXPECT_FALSE(fiber->TryLockForIntrospection(&state, noop));
    EXPECT_EQ(EFiberState::Running, state);

    fiber->SetIdle();
    fiber->SetRunning();
    EXPECT_FALSE(fiber->TryLockForIntrospection(&state, noop));
    EXPECT_EQ(EFiberState::Running, state);

    fiber->SetWaiting();
    EXPECT_TRUE(fiber->TryLockForIntrospection(&state, noop));
    fiber->SetRunning();
    EXPECT_FALSE(fiber->TryLockForIntrospection(&state, noop));
    EXPECT_EQ(EFiberState::Running, state);
}

TEST(TFiberTest, SetRunningWaitsForIntrospection)
{
    auto* fiber = TFiber::CreateFiber();
    fiber->SetRunning();
    auto releaseFiber = Finally([&] {
        TFiber::ReleaseFiber(fiber);
    });
    fiber->SetWaiting();

    TEvent started;
    TEvent resumed;
    std::thread scheduler;
    auto noop = [] { };
    EFiberState state;
    auto introspect = [&] {
        scheduler = std::thread([&] {
            started.NotifyAll();
            fiber->SetRunning();
            resumed.NotifyAll();
        });
        started.Wait();
        EXPECT_FALSE(resumed.Wait(TDuration::MilliSeconds(200)));

        EFiberState lockedState;
        EXPECT_FALSE(fiber->TryLockForIntrospection(&lockedState, noop));
        EXPECT_EQ(EFiberState::Introspecting, lockedState);
    };
    EXPECT_TRUE(fiber->TryLockForIntrospection(&state, introspect));

    scheduler.join();
    EXPECT_FALSE(fiber->TryLockForIntrospection(&state, noop));
    EXPECT_EQ(EFiberState::Running, state);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency
