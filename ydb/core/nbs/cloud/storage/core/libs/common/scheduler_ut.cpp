#include "scheduler.h"

#include "task_queue.h"
#include "timer_test.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>
#include <util/system/event.h>

namespace NYdb::NBS {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr auto WAIT_SHORT_TIMEOUT = TDuration::MilliSeconds(100);
constexpr auto WAIT_LONG_TIMEOUT = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////////////////

class TTestTaskQueue final: public ITaskQueue
{
private:
    ITaskPtr Task;
    TManualEvent Event;

public:
    void Start() override
    {}

    void Stop() override
    {}

    void Enqueue(ITaskPtr task) override
    {
        Task = std::move(task);
        Event.Signal();
    }

    ITaskPtr Dequeue(TDuration waitTimeout)
    {
        Event.WaitT(waitTimeout);
        return std::move(Task);
    }
};

struct TTestEnv
{
    TString Result;
    TAutoEvent Event;
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TSchedulerTest)
{
    Y_UNIT_TEST(ShouldScheduleTask)
    {
        auto timer = std::make_shared<TTestTimer>();
        auto scheduler = CreateScheduler(timer);
        scheduler->Start();
        Y_DEFER
        {
            scheduler->Stop();
        };

        TManualEvent event;
        scheduler->Schedule(
            TInstant::MilliSeconds(10),
            [&] { event.Signal(); });

        timer->AdvanceTime(TDuration::MilliSeconds(9));
        UNIT_ASSERT(!event.WaitT(WAIT_SHORT_TIMEOUT));

        timer->AdvanceTime(TDuration::MilliSeconds(1));
        UNIT_ASSERT(event.WaitT(WAIT_LONG_TIMEOUT));
    }

    Y_UNIT_TEST(ShouldScheduleTasksInOrder)
    {
        auto timer = std::make_shared<TTestTimer>();
        auto scheduler = CreateScheduler(timer);
        scheduler->Start();
        Y_DEFER
        {
            scheduler->Stop();
        };

        auto env = std::make_shared<TTestEnv>();

        scheduler->Schedule(
            TInstant::MilliSeconds(30),
            [env]()
            {
                env->Result += "3";
                env->Event.Signal();
            });
        scheduler->Schedule(
            TInstant::MilliSeconds(20),
            [env]()
            {
                env->Result += "2";
                env->Event.Signal();
            });
        scheduler->Schedule(
            TInstant::MilliSeconds(10),
            [env]()
            {
                env->Result += "1";
                env->Event.Signal();
            });

        timer->AdvanceTime(TDuration::MilliSeconds(9));
        UNIT_ASSERT(!env->Event.WaitT(WAIT_SHORT_TIMEOUT));
        UNIT_ASSERT(env->Result.empty());

        timer->AdvanceTime(TDuration::MilliSeconds(1));
        UNIT_ASSERT(env->Event.WaitT(WAIT_LONG_TIMEOUT));
        UNIT_ASSERT_VALUES_EQUAL("1", env->Result);

        timer->AdvanceTime(TDuration::MilliSeconds(9));
        UNIT_ASSERT(!env->Event.WaitT(WAIT_SHORT_TIMEOUT));
        UNIT_ASSERT_VALUES_EQUAL("1", env->Result);

        timer->AdvanceTime(TDuration::MilliSeconds(1));
        UNIT_ASSERT(env->Event.WaitT(WAIT_LONG_TIMEOUT));
        UNIT_ASSERT_VALUES_EQUAL("12", env->Result);

        timer->AdvanceTime(TDuration::MilliSeconds(9));
        UNIT_ASSERT(!env->Event.WaitT(WAIT_SHORT_TIMEOUT));
        UNIT_ASSERT_VALUES_EQUAL("12", env->Result);

        timer->AdvanceTime(TDuration::MilliSeconds(1));
        UNIT_ASSERT(env->Event.WaitT(WAIT_LONG_TIMEOUT));
        UNIT_ASSERT_VALUES_EQUAL("123", env->Result);
    }

    Y_UNIT_TEST(ShouldExecuteTaskViaTaskQueue)
    {
        auto timer = std::make_shared<TTestTimer>();
        auto scheduler = CreateScheduler(timer);
        scheduler->Start();
        Y_DEFER
        {
            scheduler->Stop();
        };

        TTestTaskQueue taskQueue;
        scheduler->Schedule(
            &taskQueue,
            TInstant::MilliSeconds(10),
            []
            {
                // nothing to do
            });

        timer->AdvanceTime(TDuration::MilliSeconds(9));
        UNIT_ASSERT(!taskQueue.Dequeue(WAIT_SHORT_TIMEOUT));

        timer->AdvanceTime(TDuration::MilliSeconds(1));
        UNIT_ASSERT(taskQueue.Dequeue(WAIT_LONG_TIMEOUT));
    }
}

}   // namespace NYdb::NBS
