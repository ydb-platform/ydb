
#include "topic_workload_params.h"

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/getopt/small/last_getopt_support.h>

namespace NYdb::NConsoleClient {
    class TCommandWorkloadTopicParamsTests: public NUnitTest::TTestBase {
    public:
        UNIT_TEST_SUITE(TCommandWorkloadTopicParamsTests)
        UNIT_TEST(TestRun_StrToBytes_Simple);
        UNIT_TEST(TestRun_StrToBytes_Kilo);
        UNIT_TEST(TestRun_StrToBytes_Mega);
        UNIT_TEST(TestRun_StrToBytes_Giga);
        UNIT_TEST(TestRun_StrToBytes_Error);
        UNIT_TEST_SUITE_END();

        void TestRun_StrToBytes_Simple() {
            UNIT_ASSERT_EQUAL(TCommandWorkloadTopicParams::StrToBytes("12345"), 12345);
        }

        void TestRun_StrToBytes_Kilo() {
            UNIT_ASSERT_EQUAL(TCommandWorkloadTopicParams::StrToBytes("12345k"), 12345ull * 1024);
            UNIT_ASSERT_EQUAL(TCommandWorkloadTopicParams::StrToBytes("12345K"), 12345ull * 1024);
        }

        void TestRun_StrToBytes_Mega() {
            UNIT_ASSERT_EQUAL(TCommandWorkloadTopicParams::StrToBytes("12345m"), 12345ull * 1024 * 1024);
            UNIT_ASSERT_EQUAL(TCommandWorkloadTopicParams::StrToBytes("12345M"), 12345ull * 1024 * 1024);
        }

        void TestRun_StrToBytes_Giga() {
            UNIT_ASSERT_EQUAL(TCommandWorkloadTopicParams::StrToBytes("12345g"), 12345ull * 1024 * 1024 * 1024);
            UNIT_ASSERT_EQUAL(TCommandWorkloadTopicParams::StrToBytes("12345G"), 12345ull * 1024 * 1024 * 1024);
        }

        void TestRun_StrToBytes_Error() {
            UNIT_ASSERT_EXCEPTION(TCommandWorkloadTopicParams::StrToBytes("WrongNumber"), NLastGetopt::TException);
        }
    };

    UNIT_TEST_SUITE_REGISTRATION(TCommandWorkloadTopicParamsTests);
} // namespace NYdb::NConsoleClient
