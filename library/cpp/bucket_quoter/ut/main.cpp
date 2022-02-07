#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/bucket_quoter/bucket_quoter.h>

#include "test_namespace.h"


using NBucketQuoterTest::TMockTimer;

#define TEST_RPS_EXACT(THREADS, RPS, EXPECTED_RESULT, TIMER)  \
Y_UNIT_TEST(Test##TIMER##RPS##Rps##THREADS##Threads) {           \
    ui32 seconds = 10;                  \
    ui32 summary_requests = NBucketQuoterTest::Run<TIMER, NBucketQuoterTest::TTestScenario>\
        (THREADS, RPS, seconds);\
    ui32 total_rps = summary_requests / seconds;     \
    UNIT_ASSERT_EQUAL(total_rps, EXPECTED_RESULT);\
}

#define TEST_RPS_LESS(THREADS, RPS, UPPER_BOUND, TIMER)  \
Y_UNIT_TEST(Test##TIMER##RPS##rps##THREADS##threads) {           \
    ui32 seconds = 10;                  \
    ui32 summary_requests = NBucketQuoterTest::Run<TIMER, NBucketQuoterTest::TTestScenario>\
        (THREADS, RPS, seconds);   \
    ui32 total_rps = summary_requests / seconds;              \
    UNIT_ASSERT_LE(total_rps, UPPER_BOUND);     \
}

Y_UNIT_TEST_SUITE(TMockTimerTest) {
    TEST_RPS_EXACT(1, 100, 100, TMockTimer)
    TEST_RPS_EXACT(1, 120, 60, TMockTimer)
    TEST_RPS_EXACT(1, 200, 200, TMockTimer)
    TEST_RPS_EXACT(1, 220, 110, TMockTimer)
    TEST_RPS_EXACT(1, 240, 120, TMockTimer)
    TEST_RPS_EXACT(1, 300, 150, TMockTimer)
    TEST_RPS_EXACT(1, 320, 320, TMockTimer)
    TEST_RPS_EXACT(1, 480, 240, TMockTimer)

}

Y_UNIT_TEST_SUITE(TInstantTimerTest) {
    TEST_RPS_LESS(1, 360, 300, TInstantTimerMs)
    TEST_RPS_LESS(1, 480, 400, TInstantTimerMs)
    TEST_RPS_LESS(5, 420, 350, TInstantTimerMs)
    TEST_RPS_LESS(5, 220, 185, TInstantTimerMs)
}
