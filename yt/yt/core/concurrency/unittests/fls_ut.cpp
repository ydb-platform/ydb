#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/fls.h>
#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/actions/callback.h>
#include <yt/yt/core/actions/future.h>

#include <util/system/yield.h>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

std::atomic<int> CtorCalls;
std::atomic<int> DtorCalls;

struct TMyValue
{
    TString Value;
    static void Reset()
    {
        CtorCalls = 0;
        DtorCalls = 0;
    }

    TMyValue()
    {
        ++CtorCalls;
    }

    ~TMyValue()
    {
        ++DtorCalls;
    }
};

class TFlsTest
    : public ::testing::Test
{
protected:
    const TActionQueuePtr ActionQueue = New<TActionQueue>();

    void SetUp() override
    {
        TMyValue::Reset();
    }

    void TearDown() override
    {
        ActionQueue->Shutdown();
    }
};

TFlsSlot<TMyValue> Slot;

TEST_F(TFlsTest, IsInitialized)
{
    BIND([&] {
        EXPECT_FALSE(Slot.IsInitialized());
    })
        .AsyncVia(ActionQueue->GetInvoker())
        .Run()
        .Get();

    EXPECT_EQ(CtorCalls, 0);
    EXPECT_EQ(DtorCalls, 0);
}

TEST_F(TFlsTest, TwoFibers)
{
    auto p1 = NewPromise<void>();
    auto p2 = NewPromise<void>();

    auto f1 = BIND([&] {
        Slot->Value = "fiber1";
        WaitFor(p1.ToFuture())
            .ThrowOnError();
        EXPECT_EQ("fiber1", Slot->Value);
    })
        .AsyncVia(ActionQueue->GetInvoker())
        .Run();

    auto f2 = BIND([&] {
        Slot->Value = "fiber2";
        WaitFor(p2.ToFuture())
            .ThrowOnError();
        EXPECT_EQ("fiber2", Slot->Value);
    })
        .AsyncVia(ActionQueue->GetInvoker())
        .Run();

    p1.Set();
    p2.Set();

    WaitFor(f1)
        .ThrowOnError();
    WaitFor(f2)
        .ThrowOnError();

    EXPECT_EQ(CtorCalls, 2);
    while (DtorCalls != 2) {
        ThreadYield();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency

