#include <library/cpp/testing/unittest/registar.h>

#include "scheduler.h"

#include <library/cpp/messagebus/misc/test_sync.h>

using namespace NBus;
using namespace NBus::NPrivate;

Y_UNIT_TEST_SUITE(TSchedulerTests) {
    struct TSimpleScheduleItem: public IScheduleItem {
        TTestSync* const TestSync;

        TSimpleScheduleItem(TTestSync* testSync)
            : IScheduleItem((TInstant::Now() + TDuration::MilliSeconds(1)))
            , TestSync(testSync)
        {
        }

        void Do() override {
            TestSync->WaitForAndIncrement(0);
        }
    };

    Y_UNIT_TEST(Simple) {
        TTestSync testSync;

        TScheduler scheduler;

        scheduler.Schedule(new TSimpleScheduleItem(&testSync));

        testSync.WaitForAndIncrement(1);

        scheduler.Stop();
    }
}
