#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/misc/lazy_ptr.h>
#include <yt/yt/core/misc/blob_output.h>
#include <yt/yt/core/misc/phoenix.h>

#include <util/generic/cast.h>

namespace NYT::NProfiling {
namespace {

using namespace NConcurrency;
using namespace NPhoenix;

////////////////////////////////////////////////////////////////////////////////

static const auto SleepQuantum = TDuration::MilliSeconds(100);

////////////////////////////////////////////////////////////////////////////////

void PersistWaitRestore(TWallTimer& timer)
{
    TBlobOutput output;
    TSaveContext saveContext(&output);
    Save(saveContext, timer);
    saveContext.Finish();
    auto blob = output.Flush();

    TDelayedExecutor::WaitForDuration(SleepQuantum);

    TMemoryInput input(blob.Begin(), blob.Size());
    TLoadContext loadContext(&input);
    Load(loadContext, timer);
}

////////////////////////////////////////////////////////////////////////////////

class TTimerTest
    : public ::testing::Test
{
protected:
    TLazyIntrusivePtr<TActionQueue> Queue_;

    virtual void TearDown()
    {
        if (Queue_.HasValue()) {
            Queue_->Shutdown();
        }
    }
};

TEST_F(TTimerTest, CpuEmpty)
{
    TValue cpu = 0;
    BIND([&] {
        TFiberWallTimer cpuTimer;
        cpu = cpuTimer.GetElapsedValue();
    })
        .AsyncVia(Queue_->GetInvoker())
        .Run()
        .Get();

    EXPECT_LT(cpu, 10'000);
}

TEST_F(TTimerTest, CpuWallCompare)
{
    TValue cpu = 0;
    TValue wall = 0;
    BIND([&] {
        TFiberWallTimer cpuTimer;
        TWallTimer wallTimer;

        TDelayedExecutor::WaitForDuration(SleepQuantum);

        cpu = cpuTimer.GetElapsedValue();
        wall = wallTimer.GetElapsedValue();
    })
        .AsyncVia(Queue_->GetInvoker())
        .Run()
        .Get();

    EXPECT_LT(cpu, 10'000);
    EXPECT_GT(wall, 80'000);
    EXPECT_LT(wall, 120'000);
}

TEST_F(TTimerTest, PersistAndRestoreActive)
{
    TWallTimer wallTimer;
    TDelayedExecutor::WaitForDuration(SleepQuantum);

    PersistWaitRestore(wallTimer);

    TDelayedExecutor::WaitForDuration(SleepQuantum);

    auto wall = wallTimer.GetElapsedValue();

    EXPECT_GT(wall, 150'000);
    EXPECT_LT(wall, 250'000);
}

TEST_F(TTimerTest, PersistAndRestoreNonActive)
{
    TWallTimer wallTimer;
    TDelayedExecutor::WaitForDuration(SleepQuantum);

    wallTimer.Stop();

    TDelayedExecutor::WaitForDuration(SleepQuantum);

    PersistWaitRestore(wallTimer);

    TDelayedExecutor::WaitForDuration(SleepQuantum);

    wallTimer.Start();

    TDelayedExecutor::WaitForDuration(SleepQuantum);

    auto wall = wallTimer.GetElapsedValue();

    EXPECT_GT(wall, 150'000);
    EXPECT_LT(wall, 250'000);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NProfiling
