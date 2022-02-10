#include "timer.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NMonitoring;
using namespace std::literals::chrono_literals;

class TCallback { 
public: 
    explicit TCallback(int value)
        : Value_(value){};
    void operator()(std::chrono::high_resolution_clock::duration duration) { 
        Value_ = duration.count(); 
    }; 
 
    int Value_; 
}; 
 
Y_UNIT_TEST_SUITE(TTimerTest) {
    Y_UNIT_TEST(RecordValue) {
        TTimerNs timerNs(1ns, 1s);
        UNIT_ASSERT(timerNs.RecordValue(10us));

        TTimerUs timerUs(1us, 1s);
        UNIT_ASSERT(timerUs.RecordValue(10us));

        THistogramSnapshot snapshot;
        timerNs.TakeSnapshot(&snapshot);
        UNIT_ASSERT_EQUAL(snapshot.Min, 10000);
        UNIT_ASSERT_EQUAL(snapshot.Max, 10007);
        UNIT_ASSERT_DOUBLES_EQUAL(snapshot.StdDeviation, 0.0, 1e-6);

        timerUs.TakeSnapshot(&snapshot);
        UNIT_ASSERT_EQUAL(snapshot.Min, 10);
        UNIT_ASSERT_EQUAL(snapshot.Max, 10);
        UNIT_ASSERT_DOUBLES_EQUAL(snapshot.StdDeviation, 0.0, 1e-6);
    }

    Y_UNIT_TEST(Measure) {
        TTimerNs timer(1ns, 1s);
        timer.Measure([]() {
            Sleep(TDuration::MilliSeconds(1));
        });
        THistogramSnapshot snapshot;
        timer.TakeSnapshot(&snapshot);

        UNIT_ASSERT(snapshot.Min > std::chrono::nanoseconds(1ms).count());
        UNIT_ASSERT(snapshot.Max > std::chrono::nanoseconds(1ms).count());
        UNIT_ASSERT_DOUBLES_EQUAL(snapshot.StdDeviation, 0.0, 1e-6);
    }

    Y_UNIT_TEST(TimerScope) {
        TTimerUs timer(1us, 1000s);
        {
            TTimerScope<TTimerUs> scope(&timer);
            Sleep(TDuration::MilliSeconds(10));
        }
        THistogramSnapshot snapshot;
        timer.TakeSnapshot(&snapshot);

        UNIT_ASSERT(snapshot.Min > std::chrono::microseconds(10ms).count());
        UNIT_ASSERT(snapshot.Max > std::chrono::microseconds(10ms).count());
        UNIT_ASSERT_DOUBLES_EQUAL(snapshot.StdDeviation, 0.0, 1e-6);
    }
 
    Y_UNIT_TEST(TimerScopeWithCallback) {
        TCallback callback(0); 
        TTimerUs timer(1us, 1000s); 
        { 
            TTimerScope<TTimerUs, TCallback> scope(&timer, &callback); 
            Sleep(TDuration::MilliSeconds(10)); 
        } 
        THistogramSnapshot snapshot; 
        timer.TakeSnapshot(&snapshot); 
 
        UNIT_ASSERT(snapshot.Min > std::chrono::microseconds(10ms).count()); 
        UNIT_ASSERT(snapshot.Max > std::chrono::microseconds(10ms).count()); 
        UNIT_ASSERT_DOUBLES_EQUAL(snapshot.StdDeviation, 0.0, 1e-6); 
        UNIT_ASSERT(callback.Value_ > std::chrono::microseconds(10ms).count()); 
    } 
}
