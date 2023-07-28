#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/invoker_alarm.h>
#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/actions/invoker.h>
#include <yt/yt/core/actions/future.h>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TInvokerAlarmTest
    : public ::testing::Test
{
protected:
    const TActionQueuePtr ActionQueue_ = New<TActionQueue>();
    const ISuspendableInvokerPtr Invoker_ = CreateSuspendableInvoker(ActionQueue_->GetInvoker());
    const TInvokerAlarmPtr Alarm_ = New<TInvokerAlarm>(Invoker_);

    template <class F>
    void Do(F func)
    {
        BIND(func)
            .AsyncVia(ActionQueue_->GetInvoker())
            .Run()
            .Get();
    }
};

TEST_F(TInvokerAlarmTest, CheckUnarmed)
{
    Do([&] {
        EXPECT_FALSE(Alarm_->Check());
    });
}

TEST_F(TInvokerAlarmTest, ArmDisarm)
{
    Do([&] {
        auto invoked = std::make_shared<std::atomic<bool>>();
        Alarm_->Arm(BIND([=] { *invoked = true; }), TDuration::MilliSeconds(100));
        Alarm_->Disarm();
        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));
        EXPECT_FALSE(*invoked);
    });
}

TEST_F(TInvokerAlarmTest, DisarmUnarmed)
{
    Do([&] {
        EXPECT_FALSE(Alarm_->IsArmed());
        Alarm_->Disarm();
        EXPECT_FALSE(Alarm_->IsArmed());
    });
}

TEST_F(TInvokerAlarmTest, ArmSchedulesCallback)
{
    Do([&] {
        auto invoked = std::make_shared<std::atomic<bool>>();
        Alarm_->Arm(BIND([=] { *invoked = true; }), TDuration::MilliSeconds(100));
        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));
        EXPECT_TRUE(*invoked);
        EXPECT_FALSE(Alarm_->IsArmed());
    });
}

TEST_F(TInvokerAlarmTest, CheckInvokesCallback)
{
    Do([&] {
        auto invoked = std::make_shared<std::atomic<bool>>();
        Alarm_->Arm(BIND([=] { *invoked = true; }), TDuration::MilliSeconds(100));
        Invoker_->Suspend().Get();
        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));
        EXPECT_FALSE(*invoked);
        EXPECT_TRUE(Alarm_->Check());
        EXPECT_TRUE(*invoked);
        EXPECT_FALSE(Alarm_->IsArmed());
        Invoker_->Resume();
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency
