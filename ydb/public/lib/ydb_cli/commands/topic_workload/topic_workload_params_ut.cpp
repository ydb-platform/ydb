
#include "topic_workload_params.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/getopt/small/last_getopt_support.h>

using namespace NYdb::NTopic;

namespace NYdb::NConsoleClient {
    class TCommandWorkloadTopicParamsTests: public NUnitTest::TTestBase {
    public:
        UNIT_TEST_SUITE(TCommandWorkloadTopicParamsTests)
        UNIT_TEST(TestRun_StrToBytes_Simple);
        UNIT_TEST(TestRun_StrToBytes_Kilo);
        UNIT_TEST(TestRun_StrToBytes_Mega);
        UNIT_TEST(TestRun_StrToBytes_Giga);
        UNIT_TEST(TestRun_StrToBytes_Error);
        UNIT_TEST(TestRun_StrToCodec);
        UNIT_TEST(TestRun_StrToBatchInnerCodec);
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

        void TestRun_StrToCodec() {
            UNIT_ASSERT_EQUAL(TCommandWorkloadTopicParams::StrToCodec("raw"), (ui32)NTopic::ECodec::RAW);
            UNIT_ASSERT_EQUAL(TCommandWorkloadTopicParams::StrToCodec("kafka-batch"), (ui32)NTopic::ECodec::KAFKA_BATCH);
            UNIT_ASSERT_EXCEPTION(TCommandWorkloadTopicParams::StrToCodec("unknown"), yexception);
        }

        void TestRun_StrToBatchInnerCodec() {
            UNIT_ASSERT_EQUAL(TCommandWorkloadTopicParams::StrToBatchInnerCodec("gzip"), (ui32)NTopic::ECodec::GZIP);
            UNIT_ASSERT_EQUAL(TCommandWorkloadTopicParams::StrToBatchInnerCodec("zstd"), (ui32)NTopic::ECodec::ZSTD);
            UNIT_ASSERT_EXCEPTION(TCommandWorkloadTopicParams::StrToBatchInnerCodec("raw"), yexception);
        }
    };

    UNIT_TEST_SUITE_REGISTRATION(TCommandWorkloadTopicParamsTests);
} // namespace NYdb::NConsoleClient
