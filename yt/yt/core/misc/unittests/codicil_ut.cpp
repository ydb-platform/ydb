#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/codicil.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/codicil_guarded_invoker.h>
#include <yt/yt/core/actions/future.h>

namespace NYT {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TEST(TCodicilTest, Simple)
{
    const auto codicil1 = std::string("codicil1");
    const auto codicil2 = std::string("codicil2");
    auto guard1 = TCodicilGuard(MakeOwningCodicilBuilder(codicil1));
    auto guard2 = TCodicilGuard(MakeOwningCodicilBuilder(codicil2));
    EXPECT_EQ(BuildCodicils(), (std::vector{codicil1, codicil2}));
}

TEST(TCodicilTest, MaxLength)
{
    const auto codicil = std::string(MaxCodicilLength + 1, 'x');
    auto guard = TCodicilGuard(MakeOwningCodicilBuilder(codicil));
    auto codicils = BuildCodicils();
    EXPECT_EQ(std::ssize(codicils), 1);
    EXPECT_EQ(codicils[0], codicil.substr(0, MaxCodicilLength));
}

TEST(TCodicilTest, CodicilGuardedInvoker)
{
    const auto codicil = std::string("codicil");
    auto actionQueue = New<TActionQueue>("ActionQueue");
    auto invoker = CreateCodicilGuardedInvoker(actionQueue->GetInvoker(), codicil);
    BIND([&] {
        EXPECT_EQ(BuildCodicils(), (std::vector{codicil}));
        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(100));
        EXPECT_EQ(BuildCodicils(), (std::vector{codicil}));
    })
        .AsyncVia(invoker)
        .Run()
        .Get();
    actionQueue->Shutdown();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
