#include "ut_utils/managed_executor.h"
#include "ut_utils/topic_sdk_test_setup.h"
#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/ut/ut_utils/ut_utils.h>

#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>

#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/persqueue.h>

#include <ydb/public/sdk/cpp/client/ydb_topic/impl/common.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/common/executor_impl.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/impl/write_session.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/impl/write_session.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/threading/future/future.h>
#include <library/cpp/threading/future/async.h>

#include <future>

namespace NYdb::NTopic::NTests {

Y_UNIT_TEST_SUITE(DirectRead) {
    Y_UNIT_TEST(WriteReadOneMessage) {
        TTopicSdkTestSetup setup(TEST_CASE_NAME);
        TTopicClient client = setup.MakeClient();

        {
            // Write a message:

            auto settings = TWriteSessionSettings()
                .Path(TEST_TOPIC)
                .ProducerId(TEST_MESSAGE_GROUP_ID)
                .MessageGroupId(TEST_MESSAGE_GROUP_ID);
            Cerr << ">>> Open write session\n";
            auto writer = client.CreateSimpleBlockingWriteSession(settings);
            UNIT_ASSERT(writer->Write("message_using_MessageGroupId"));
            Cerr << ">>> Write session: message written\n";
            writer->Close();
            Cerr << ">>> Write session closed\n";
        }

        {
            // Read the message:

            auto settings = TReadSessionSettings()
                .ConsumerName(TEST_CONSUMER)
                .AppendTopics(TEST_TOPIC)
                .DirectRead(true);
            auto reader = client.CreateReadSession(settings);

            auto event = reader->GetEvent(true);
            UNIT_ASSERT(event.Defined());

            auto& startPartitionSession = std::get<TReadSessionEvent::TStartPartitionSessionEvent>(*event);
            startPartitionSession.Confirm();

            event = reader->GetEvent(true);
            UNIT_ASSERT(event.Defined());

            auto& dataReceived = std::get<TReadSessionEvent::TDataReceivedEvent>(*event);
            dataReceived.Commit();

            auto& messages = dataReceived.GetMessages();
            UNIT_ASSERT_EQUAL(messages.size(), 1);
            Cerr << dataReceived.DebugString() << Endl;
        }
    }
} // Y_UNIT_TEST_SUITE(DirectRead)

} // namespace
