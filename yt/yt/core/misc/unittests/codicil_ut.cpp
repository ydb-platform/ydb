#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/crash_handler.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/future.h>

namespace NYT {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TEST(TCodicilTest, Simple)
{
    const auto codicil1 = TString("codicil1");
    const auto codicil2 = TString("codicil2");
    TCodicilGuard guard1(codicil1);
    TCodicilGuard guard2(codicil2);
    EXPECT_EQ(GetCodicils(), (std::vector{codicil1, codicil2}));
}

TEST(TCodicilTest, CodicilGuardedInvoker)
{
    const auto codicil = TString("codicil");
    auto actionQueue = New<TActionQueue>("ActionQueue");
    auto invoker = CreateCodicilGuardedInvoker(actionQueue->GetInvoker(), codicil);
    BIND([&] {
        EXPECT_EQ(GetCodicils(), (std::vector{codicil}));
        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(100));
        EXPECT_EQ(GetCodicils(), (std::vector{codicil}));
    })
        .AsyncVia(invoker)
        .Run()
        .Get();
    actionQueue->Shutdown();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
