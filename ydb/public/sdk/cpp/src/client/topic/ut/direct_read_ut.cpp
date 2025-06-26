#include "ut_utils/topic_sdk_test_setup.h"
#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/ut_utils.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <library/cpp/testing/unittest/registar.h>


using namespace std::chrono_literals;
namespace NYdb::inline Dev::NTopic::NTests {

Y_UNIT_TEST_SUITE(DirectReadWithServer) {

    /*
    This suite tests direct read session interaction with server.

    It complements tests from basic_usage_ut.cpp etc, as we run them with direct read disabled/enabled.
    */

    Y_UNIT_TEST(KillPQTablet) {
        /*
        A read session should keep working if a partition tablet is killed and moved to another node.
        */
        auto setup = TTopicSdkTestSetup(TEST_CASE_NAME);
        auto client = setup.MakeClient();
        auto nextMessageId = 0;

        auto writeMessages = [&](size_t n) {
            auto writer = client.CreateSimpleBlockingWriteSession(TWriteSessionSettings()
                .Path(setup.GetTopicPath())
                .MessageGroupId(TEST_MESSAGE_GROUP_ID)
                .ProducerId(TEST_MESSAGE_GROUP_ID));

            for (size_t i = 0; i < n; ++i, ++nextMessageId) {
                auto res = writer->Write(TStringBuilder() << "message-" << nextMessageId);
                UNIT_ASSERT(res);
            }

            writer->Close();
        };

        writeMessages(1);

        auto gotFirstMessage = NThreading::NewPromise();
        auto gotSecondMessage = NThreading::NewPromise();

        auto readerSettings = TReadSessionSettings()
            .ConsumerName(setup.GetConsumerName())
            .AppendTopics(setup.GetTopicPath())
            // .DirectRead(true)
            ;

        TIntrusivePtr<TPartitionSession> partitionSession;

        readerSettings.EventHandlers_
            .DataReceivedHandler([&](TReadSessionEvent::TDataReceivedEvent& e) {
                switch (e.GetMessages()[0].GetSeqNo()) {
                case 1:
                    gotFirstMessage.SetValue();
                    break;
                case 2:
                    gotSecondMessage.SetValue();
                    break;
                }
                e.Commit();
            })
            .StartPartitionSessionHandler([&](TReadSessionEvent::TStartPartitionSessionEvent& e) {
                e.Confirm();
                partitionSession = e.GetPartitionSession();
            })
            .StopPartitionSessionHandler([&](TReadSessionEvent::TStopPartitionSessionEvent& e) {
                e.Confirm();
            })
            ;

        auto reader = client.CreateReadSession(readerSettings);

        gotFirstMessage.GetFuture().Wait();

        auto getPartitionGeneration = [&client, &setup]() {
            auto description = client.DescribePartition(setup.GetTopicPath(), 0, TDescribePartitionSettings().IncludeLocation(true)).GetValueSync();
            return description.GetPartitionDescription().GetPartition().GetPartitionLocation()->GetGeneration();
        };

        auto firstGenerationId = getPartitionGeneration();

        setup.GetServer().KillTopicPqTablets(setup.GetFullTopicPath());

        while (firstGenerationId == getPartitionGeneration()) {
            std::this_thread::sleep_for(100ms);
        }

        writeMessages(1);

        gotSecondMessage.GetFuture().Wait();

        reader->Close();
    }

    Y_UNIT_TEST(KillPQRBTablet) {
        /*
        A read session should keep working if a balancer tablet is killed and moved to another node.
        */
        // TODO
        return;
        auto setup = TTopicSdkTestSetup(TEST_CASE_NAME);
        auto client = setup.MakeClient();
        auto nextMessageId = 0;

        auto writeMessages = [&](size_t n) {
            auto writer = client.CreateSimpleBlockingWriteSession(TWriteSessionSettings()
                .Path(setup.GetTopicPath())
                .MessageGroupId(TEST_MESSAGE_GROUP_ID)
                .ProducerId(TEST_MESSAGE_GROUP_ID));

            for (size_t i = 0; i < n; ++i, ++nextMessageId) {
                auto res = writer->Write(TStringBuilder() << "message-" << nextMessageId);
                UNIT_ASSERT(res);
            }

            writer->Close();
        };

        writeMessages(1);

        auto gotFirstMessage = NThreading::NewPromise();
        auto gotSecondMessage = NThreading::NewPromise();

        auto readerSettings = TReadSessionSettings()
            .ConsumerName(setup.GetConsumerName())
            .AppendTopics(setup.GetTopicPath())
            // .DirectRead(true)
            ;

        TIntrusivePtr<TPartitionSession> partitionSession;

        readerSettings.EventHandlers_
            .DataReceivedHandler([&](TReadSessionEvent::TDataReceivedEvent& e) {
                switch (e.GetMessages()[0].GetSeqNo()) {
                case 1:
                    gotFirstMessage.SetValue();
                    break;
                case 2:
                    gotSecondMessage.SetValue();
                    break;
                }
                e.Commit();
            })
            .StartPartitionSessionHandler([&](TReadSessionEvent::TStartPartitionSessionEvent& e) {
                e.Confirm();
                partitionSession = e.GetPartitionSession();
            })
            ;

        auto reader = client.CreateReadSession(readerSettings);

        gotFirstMessage.GetFuture().Wait();

        auto getPartitionGeneration = [&client, &setup]() {
            auto description = client.DescribePartition(setup.GetTopicPath(), 0, TDescribePartitionSettings().IncludeLocation(true)).GetValueSync();
            return description.GetPartitionDescription().GetPartition().GetPartitionLocation()->GetGeneration();
        };

        auto firstGenerationId = getPartitionGeneration();

        setup.GetServer().KillTopicPqrbTablet(setup.GetFullTopicPath());

        while (firstGenerationId == getPartitionGeneration()) {
            std::this_thread::sleep_for(100ms);
        }

        writeMessages(1);

        gotSecondMessage.GetFuture().Wait();

        reader->Close();
    }
} // Y_UNIT_TEST_SUITE_F(DirectReadWithServer)

} // namespace NYdb::NTopic::NTests
