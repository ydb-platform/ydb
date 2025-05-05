#include "task_scheduler.h"

#include <library/cpp/deprecated/atomic/atomic.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>
#include <util/stream/output.h>
#include <util/system/thread.h>

Y_UNIT_TEST_SUITE(TaskSchedulerTest) {
    class TCheckTask: public TTaskScheduler::IRepeatedTask {
        public:
            TCheckTask(const TDuration& delay)
                : Start_(Now())
                , Delay_(delay)
            {
                AtomicIncrement(ScheduledTaskCounter_);
            }

            ~TCheckTask() override {
            }

            bool Process() override {
                const TDuration delay = Now() - Start_;

                if (delay < Delay_) {
                    AtomicIncrement(BadTimeoutCounter_);
                }

                AtomicIncrement(ExecutedTaskCounter_);

                return false;
            }

            static bool AllTaskExecuted() {
                return AtomicGet(ScheduledTaskCounter_) == AtomicGet(ExecutedTaskCounter_);
            }

            static size_t BadTimeoutCount() {
                return AtomicGet(BadTimeoutCounter_);
            }

        private:
            TInstant Start_;
            TDuration Delay_;
            static inline TAtomic BadTimeoutCounter_ = 0;
            static inline TAtomic ScheduledTaskCounter_ = 0;
            static inline TAtomic ExecutedTaskCounter_ = 0;
    };

    void ScheduleCheckTask(TTaskScheduler& scheduler, size_t delay) {
        TDuration d = TDuration::MicroSeconds(delay);

        scheduler.Add(new TCheckTask(d), d);
    }

    Y_UNIT_TEST(RepeatedTasks) {
        TTaskScheduler scheduler;

        ScheduleCheckTask(scheduler, 200);
        ScheduleCheckTask(scheduler, 100);
        ScheduleCheckTask(scheduler, 1000);
        ScheduleCheckTask(scheduler, 10000);
        ScheduleCheckTask(scheduler, 5000);

        scheduler.Start();

        usleep(1000000);

        UNIT_ASSERT_EQUAL(TCheckTask::BadTimeoutCount(), 0);
        UNIT_ASSERT(TCheckTask::AllTaskExecuted());
    }

    Y_UNIT_TEST(FunctionWrappers) {
        TTaskScheduler scheduler;

        std::atomic<size_t> oneshotCount = 0;
        std::atomic<size_t> repeatedCount = 0;

        scheduler.SafeAddFunc([&, now = Now()]() {
            Y_ABORT_UNLESS(Now() - now < TDuration::MilliSeconds(300));
            if (oneshotCount.fetch_add(1) == 0) {
                return Now() + TDuration::MilliSeconds(100);
            } else {
                return TInstant::Max();
            }
        }, Now() + TDuration::MilliSeconds(100));

        scheduler.SafeAddRepeatedFunc([&repeatedCount, now = Now()]() mutable -> bool {
            TDuration delta = Now() - now;
            Y_ABORT_UNLESS(delta > TDuration::MilliSeconds(50));
            Y_ABORT_UNLESS(delta < TDuration::MilliSeconds(150));
            now += delta;
            return repeatedCount.fetch_add(1) < 3;
        }, TDuration::MilliSeconds(100));

        scheduler.Start();

        Sleep(TDuration::Seconds(2));

        UNIT_ASSERT_EQUAL(oneshotCount.load(), 2);
        UNIT_ASSERT_EQUAL(repeatedCount.load(), 4);

        scheduler.Stop();
    }

    Y_UNIT_TEST(TaskLimit) {
        TTaskScheduler scheduler{1, 2};
        scheduler.Start();

        auto function = [] { return TInstant::Max(); };
        TInstant expire = Now() + TDuration::MilliSeconds(100);

        UNIT_ASSERT(scheduler.AddFunc(function, expire));
        UNIT_ASSERT_NO_EXCEPTION(scheduler.SafeAddFunc(function, expire));
        UNIT_ASSERT(!scheduler.AddFunc(function, expire));
        UNIT_ASSERT_EXCEPTION(scheduler.SafeAddFunc(function, expire), TTaskScheduler::TTaskSchedulerTaskLimitReached);
    }
}
