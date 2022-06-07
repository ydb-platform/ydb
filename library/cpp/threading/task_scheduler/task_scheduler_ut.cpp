#include <algorithm>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/output.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/generic/vector.h>

#include "task_scheduler.h"

class TTaskSchedulerTest: public TTestBase {
        UNIT_TEST_SUITE(TTaskSchedulerTest);
            UNIT_TEST(Test);
        UNIT_TEST_SUITE_END();

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
                static TAtomic BadTimeoutCounter_;
                static TAtomic ScheduledTaskCounter_;
                static TAtomic ExecutedTaskCounter_;
        };

    public:
        inline void Test() {
            ScheduleCheckTask(200);
            ScheduleCheckTask(100);
            ScheduleCheckTask(1000);
            ScheduleCheckTask(10000);
            ScheduleCheckTask(5000);

            Scheduler_.Start();

            usleep(1000000);

            UNIT_ASSERT_EQUAL(TCheckTask::BadTimeoutCount(), 0);
            UNIT_ASSERT(TCheckTask::AllTaskExecuted());
        }

    private:
        void ScheduleCheckTask(size_t delay) {
            TDuration d = TDuration::MicroSeconds(delay);

            Scheduler_.Add(new TCheckTask(d), d);
        }

    private:
        TTaskScheduler Scheduler_;
};

TAtomic TTaskSchedulerTest::TCheckTask::BadTimeoutCounter_ = 0;
TAtomic TTaskSchedulerTest::TCheckTask::ScheduledTaskCounter_ = 0;
TAtomic TTaskSchedulerTest::TCheckTask::ExecutedTaskCounter_ = 0;

UNIT_TEST_SUITE_REGISTRATION(TTaskSchedulerTest);
