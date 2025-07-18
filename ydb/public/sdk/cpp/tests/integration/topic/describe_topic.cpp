#include "setup/fixture.h"
#include "utils/describe.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <thread>

namespace NYdb::inline Dev::NTopic::NTests {

class Describe : public TTopicTestFixture {};

TEST_F(Describe, TEST_NAME(Basic)) {
    TTopicClient client(MakeDriver());

    try {
        DescribeTopicTest(*this, client, false, false, false);
        DescribeConsumerTest(*this, client, false, false, false);
        DescribePartitionTest(*this, client, false, false, false);
    } catch (const yexception& e) {
        ASSERT_TRUE(false) << e.what();
    }
}

TEST_F(Describe, TEST_NAME(Statistics)) {
    // TODO(abcdef): temporarily deleted
    GTEST_SKIP() << "temporarily deleted";

    TTopicClient client(MakeDriver());

    // Get empty description
    try {
        DescribeTopicTest(*this, client, true, false, false);
        DescribeConsumerTest(*this, client, true, false, false);
        DescribePartitionTest(*this, client, true, false, false);
    } catch (const yexception& e) {
        ASSERT_TRUE(false) << e.what();
    }

    const size_t messagesCount = 1;

    // Write a message
    {
        auto writeSettings = TWriteSessionSettings().Path(GetTopicPath()).MessageGroupId(TEST_MESSAGE_GROUP_ID).Codec(ECodec::RAW);
        auto writeSession = client.CreateSimpleBlockingWriteSession(writeSettings);
        std::string message(32_MB, 'x');

        for (size_t i = 0; i < messagesCount; ++i) {
            EXPECT_TRUE(writeSession->Write(message, {}, TInstant::Now() - TDuration::Seconds(100)));
        }
        writeSession->Close();
    }

    // Read a message
    {
        auto readSettings = TReadSessionSettings().ConsumerName(GetConsumerName()).AppendTopics(GetTopicPath());
        auto readSession = client.CreateReadSession(readSettings);

        // Event 1: start partition session
        {
            std::optional<TReadSessionEvent::TEvent> event = readSession->GetEvent(true);
            EXPECT_TRUE(event);
            auto startPartitionSession = std::get_if<TReadSessionEvent::TStartPartitionSessionEvent>(&event.value());
            EXPECT_TRUE(startPartitionSession) << DebugString(*event);

            startPartitionSession->Confirm();
        }

        // Event 2: data received
        {
            std::optional<TReadSessionEvent::TEvent> event = readSession->GetEvent(true);
            EXPECT_TRUE(event);
            auto dataReceived = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&event.value());
            EXPECT_TRUE(dataReceived) << DebugString(*event);

            dataReceived->Commit();
        }

        // Event 3: commit acknowledgement
        {
            std::optional<TReadSessionEvent::TEvent> event = readSession->GetEvent(true);
            EXPECT_TRUE(event);
            auto commitOffsetAck = std::get_if<TReadSessionEvent::TCommitOffsetAcknowledgementEvent>(&event.value());

            EXPECT_TRUE(commitOffsetAck) << DebugString(*event);

            EXPECT_EQ(commitOffsetAck->GetCommittedOffset(), messagesCount);
        }
    }

    // Additional write
    {
        auto writeSettings = TWriteSessionSettings().Path(GetTopicPath()).MessageGroupId(TEST_MESSAGE_GROUP_ID).Codec(ECodec::RAW);
        auto writeSession = client.CreateSimpleBlockingWriteSession(writeSettings);
        std::string message(32, 'x');

        for(size_t i = 0; i < messagesCount; ++i) {
            EXPECT_TRUE(writeSession->Write(message));
        }
        writeSession->Close();
    }
    std::this_thread::sleep_for(std::chrono::seconds(3));

    // Get non-empty description
    try {
        DescribeTopicTest(*this, client, true, true, false);
        DescribeConsumerTest(*this, client, true, true, false);
        DescribePartitionTest(*this, client, true, true, false);
    } catch (const yexception& e) {
        ASSERT_TRUE(false) << e.what();
    }
}

TEST_F(Describe, TEST_NAME(Location)) {
    TTopicClient client(MakeDriver());

    try {
        DescribeTopicTest(*this, client, false, false, true);
        DescribeConsumerTest(*this, client, false, false, true);
        DescribePartitionTest(*this, client, false, false, true);
    } catch (const yexception& e) {
        ASSERT_TRUE(false) << e.what();
    }
}

}
