#include <ydb/core/blobstorage/ut_pdiskfit/lib/fail_injection_test.h>
#include <ydb/core/blobstorage/ut_pdiskfit/lib/basic_test.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/system/sanitizers.h>

class TWatchdogThread : public ISimpleThread {
    TMutex Mutex;
    TCondVar Stop;
    TAtomic QuitFlag = 0;

public:
    ~TWatchdogThread() {
        AtomicSet(QuitFlag, 1);
        with_lock (Mutex) {
            Stop.Signal();
        }

        Join();
    }

    void *ThreadProc() override {
        with_lock (Mutex) {
            do {
                Cerr << Sprintf("Watchdog# %s\n", TInstant::Now().ToString().data());
            } while (!AtomicGet(QuitFlag) && !Stop.WaitT(Mutex, TDuration::Seconds(5)));
        }

        return nullptr;
    }
};

Y_UNIT_TEST_SUITE(TPDiskFIT) {
    Y_UNIT_TEST(Basic) {
        TWatchdogThread watchdog;
        watchdog.Start();
        TPDiskFailureInjectionTest test;
        test.TestDuration = NSan::PlainOrUnderSanitizer(TDuration::Minutes(4), TDuration::Minutes(3));
        test.RunCycle<TBasicTest>(false, 8, false);
    }

    Y_UNIT_TEST(FailTest) {
        TWatchdogThread watchdog;
        watchdog.Start();
        TPDiskFailureInjectionTest test;
        test.TestDuration = NSan::PlainOrUnderSanitizer(TDuration::Minutes(4), TDuration::Minutes(3));
        test.RunCycle<TBasicTest>(true, 8, false);
    }

    Y_UNIT_TEST(LogSpliceError) {
        TWatchdogThread watchdog;
        watchdog.Start();
        TPDiskFailureInjectionTest test;
        test.TestDuration = NSan::PlainOrUnderSanitizer(TDuration::Minutes(4), TDuration::Minutes(3));
        test.RunCycle<TBasicTest>(true, 8, true);
    }
}
