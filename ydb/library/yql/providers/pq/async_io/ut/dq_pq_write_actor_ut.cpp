#include "ut_helpers.h"

#include <ydb/library/yql/utils/yql_panic.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NDq {

constexpr TDuration WaitTimeout = TDuration::MilliSeconds(10000);

Y_UNIT_TEST_SUITE(TPqWriterTest) {
    Y_UNIT_TEST_F(TestWriteToTopic, TPqIoTestFixture) {
        const TString topicName = "WriteToTopic";
        InitSink(topicName);

        const std::vector<TString> data = { "1", "2", "3", "4" };

        SinkWrite(data);
        auto result = PQReadUntil(topicName, 4);
        UNIT_ASSERT_EQUAL(result, data);
    }

    Y_UNIT_TEST_F(TestWriteToTopicMultiBatch, TPqIoTestFixture) {
        const TString topicName = "WriteToTopicMultiBatch";
        InitSink(topicName);

        const std::vector<TString> data1 = { "1" };
        const std::vector<TString> data2 = { "2" };
        const std::vector<TString> data3 = { "3" };

        SinkWrite(data1);
        SinkWrite(data2);
        SinkWrite(data3);
        auto result = PQReadUntil(topicName, 3);

        std::vector<TString> expected = { "1", "2", "3" };
        UNIT_ASSERT_EQUAL(result, expected);
    }

    Y_UNIT_TEST_F(TestDeferredWriteToTopic, TPqIoTestFixture) {
        // In this case we are checking free space overflow
        const TString topicName = "DeferredWriteToTopic";
        InitSink(topicName, 1);

        const std::vector<TString> data = { "1", "2", "3" };

        auto future = CaSetup->SinkPromises.ResumeExecution.GetFuture();
        SinkWrite(data);
        auto result = PQReadUntil(topicName, 3);

        UNIT_ASSERT_EQUAL(result, data);
        UNIT_ASSERT(future.Wait(WaitTimeout)); // Resume execution should be called

        const std::vector<TString> data2 = { "4", "5", "6" };

        SinkWrite(data2);
        auto result2 = PQReadUntil(topicName, 6);
        const std::vector<TString> expected = { "1", "2", "3", "4", "5", "6" };
        UNIT_ASSERT_EQUAL(result2, expected);
    }

    Y_UNIT_TEST_F(WriteNonExistentTopic, TPqIoTestFixture) {
        const TString topicName = "NonExistentTopic";
        InitSink(topicName);

        const std::vector<TString> data = { "1" };
        auto future = CaSetup->SinkPromises.Issue.GetFuture();
        SinkWrite(data);

        UNIT_ASSERT(future.Wait(WaitTimeout));
        UNIT_ASSERT_STRING_CONTAINS(future.GetValue().ToString(), "Write session to topic \"NonExistentTopic\" was closed");
    }

    Y_UNIT_TEST(TestCheckpoints) {
        const TString topicName = "Checkpoints";

        NDqProto::TSinkState state1;
        {
            TPqIoTestFixture setup;
            setup.InitSink(topicName);

            const std::vector<TString> data1 = { "1" };
            setup.SinkWrite(data1);

            const std::vector<TString> data2 = { "2", "3" };
            auto checkpoint = CreateCheckpoint();
            auto future = setup.CaSetup->SinkPromises.StateSaved.GetFuture();
            setup.SinkWrite(data2, checkpoint);

            UNIT_ASSERT(future.Wait(WaitTimeout));
            state1 = future.GetValue();
        }

        {
            TPqIoTestFixture setup;
            setup.InitSink(topicName);
            setup.LoadSink(state1);

            const std::vector<TString> data3 = { "4", "5" };
            setup.SinkWrite(data3);

            auto result = PQReadUntil(topicName, 5);
            const std::vector<TString> expected = { "1", "2", "3", "4", "5" };
            UNIT_ASSERT_EQUAL(result, expected);
        }

        {
            TPqIoTestFixture setup;
            setup.InitSink(topicName);
            setup.LoadSink(state1);

            const std::vector<TString> data4 = { "4", "5" };
            setup.SinkWrite(data4); // This write should be deduplicated

            auto result = PQReadUntil(topicName, 4);
            const std::vector<TString> expected = { "1", "2", "3", "4", "5" };
            UNIT_ASSERT_EQUAL(result, expected);
        }
    }

    Y_UNIT_TEST_F(TestCheckpointWithEmptyBatch, TPqIoTestFixture) {
        const TString topicName = "Checkpoints";

        NDqProto::TSinkState state1;
        {
            InitSink(topicName);

            const std::vector<TString> data = {};
            auto checkpoint = CreateCheckpoint();
            auto future = CaSetup->SinkPromises.StateSaved.GetFuture();
            SinkWrite(data, checkpoint);

            UNIT_ASSERT(future.Wait(WaitTimeout));
            state1 = future.GetValue();
        }
    }
}

} // NYql::NDq
