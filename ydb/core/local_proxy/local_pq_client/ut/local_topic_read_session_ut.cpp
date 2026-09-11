#include "common.h"

namespace NKikimr::NKqp::NLocalTopicTests {

Y_UNIT_TEST_SUITE(TLocalTopicReadSession) {
    Y_UNIT_TEST_F(ReadMessagesAndCounters, TLocalTopicClientFixture) {
        WriteTopicMessages({"first", "second"});
        auto session = CreateReadSession();
        const auto sessionId = session->GetSessionId();
        UNIT_ASSERT(!sessionId.empty());
        const auto messages = ReadMessages(*session, 2);
        UNIT_ASSERT_VALUES_EQUAL(messages[0].GetData(), "first");
        UNIT_ASSERT_VALUES_EQUAL(messages[1].GetData(), "second");
        for (size_t i = 0; i < messages.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(messages[i].GetOffset(), i);
            UNIT_ASSERT_VALUES_EQUAL(messages[i].GetSeqNo(), i + 1);
            UNIT_ASSERT_VALUES_EQUAL(messages[i].GetPartitionSession()->GetPartitionId(), 0);
            UNIT_ASSERT_VALUES_EQUAL(messages[i].GetPartitionSession()->GetTopicPath(), "topic");
        }
        UNIT_ASSERT_VALUES_EQUAL(session->GetSessionId(), sessionId);
        UNIT_ASSERT_VALUES_EQUAL(session->GetCounters()->MessagesRead->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(session->GetCounters()->BytesRead->Val(), 11);
        CloseSession(*session);
        AssertSessionClosed(*session);
    }

    Y_UNIT_TEST_F(ReadMessageMetadata, TLocalTopicClientFixture) {
        auto writer = TopicClient->CreateSimpleBlockingWriteSession(WriteSettings()
            .AppendSessionMeta("session-key", "session-value"));
        const auto createdAt = TInstant::MilliSeconds(1700000000123);
        auto message = TWriteMessage("metadata message");
        message.SeqNo(42).CreateTimestamp(createdAt).MessageMeta({{"message-key", "message-value"}});
        UNIT_ASSERT(writer->Write(std::move(message), nullptr, TEST_TIMEOUT));
        UNIT_ASSERT(writer->Close(TEST_TIMEOUT));

        auto session = CreateReadSession();
        const auto messages = ReadMessages(*session, 1);
        const auto& received = messages[0];
        UNIT_ASSERT_VALUES_EQUAL(received.GetData(), "metadata message");
        UNIT_ASSERT_VALUES_EQUAL(received.GetProducerId(), "producer");
        UNIT_ASSERT_VALUES_EQUAL(received.GetSeqNo(), 42);
        UNIT_ASSERT_VALUES_EQUAL(received.GetCreateTime(), createdAt);
        UNIT_ASSERT(received.GetWriteTime() >= createdAt);
        UNIT_ASSERT(received.GetMeta());
        UNIT_ASSERT_VALUES_EQUAL(received.GetMeta()->Fields.at("session-key"), "session-value");
        UNIT_ASSERT(received.GetMessageMeta());
        UNIT_ASSERT_VALUES_EQUAL(received.GetMessageMeta()->Fields.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(received.GetMessageMeta()->Fields[0].first, "message-key");
        UNIT_ASSERT_VALUES_EQUAL(received.GetMessageMeta()->Fields[0].second, "message-value");
        CloseSession(*session);
    }

    Y_UNIT_TEST_F(CommitMessageAndResumeConsumer, TLocalTopicClientFixture) {
        WriteTopicMessages({"committed", "remaining-1", "remaining-2"});
        {
            auto session = CreateReadSession();
            auto messages = ReadMessages(*session, 3);
            messages[0].Commit();
            const auto ack = WaitForReadEvent<TReadSessionEvent::TCommitOffsetAcknowledgementEvent>(*session);
            UNIT_ASSERT_VALUES_EQUAL(ack.GetCommittedOffset(), 1);
            UNIT_ASSERT_VALUES_EQUAL(ack.GetPartitionSession()->GetPartitionId(), 0);
            CloseSession(*session);
        }

        auto session = CreateReadSession();
        auto start = WaitForReadEvent<TReadSessionEvent::TStartPartitionSessionEvent>(*session);
        UNIT_ASSERT_VALUES_EQUAL(start.GetCommittedOffset(), 1);
        UNIT_ASSERT_VALUES_EQUAL(start.GetEndOffset(), 3);
        start.Confirm();
        const auto messages = ReadMessages(*session, 2);
        UNIT_ASSERT_VALUES_EQUAL(messages[0].GetOffset(), 1);
        UNIT_ASSERT_VALUES_EQUAL(messages[0].GetData(), "remaining-1");
        UNIT_ASSERT_VALUES_EQUAL(messages[1].GetOffset(), 2);
        UNIT_ASSERT_VALUES_EQUAL(messages[1].GetData(), "remaining-2");
        CloseSession(*session);
    }

    Y_UNIT_TEST_F(CommitDataEventAndRequestPartitionStatus, TLocalTopicClientFixture) {
        WriteTopicMessages({"message"});
        auto session = CreateReadSession();
        auto data = WaitForReadEvent<TReadSessionEvent::TDataReceivedEvent>(*session);
        UNIT_ASSERT_VALUES_EQUAL(data.GetMessagesCount(), 1);
        data.Commit();
        const auto ack = WaitForReadEvent<TReadSessionEvent::TCommitOffsetAcknowledgementEvent>(*session);
        UNIT_ASSERT_VALUES_EQUAL(ack.GetCommittedOffset(), 1);

        data.GetPartitionSession()->RequestStatus();
        const auto status = WaitForReadEvent<TReadSessionEvent::TPartitionSessionStatusEvent>(*session);
        UNIT_ASSERT_VALUES_EQUAL(status.GetPartitionSession()->GetPartitionSessionId(), data.GetPartitionSession()->GetPartitionSessionId());
        UNIT_ASSERT_VALUES_EQUAL(status.GetCommittedOffset(), 1);
        UNIT_ASSERT_VALUES_EQUAL(status.GetReadOffset(), 1);
        UNIT_ASSERT_VALUES_EQUAL(status.GetEndOffset(), 1);
        CloseSession(*session);
    }

    Y_UNIT_TEST_F(StartFromExplicitOffsetWithoutConsumer, TLocalTopicClientFixture) {
        WriteTopicMessages({"skip", "read-1", "read-2"});
        auto settings = ReadSettings().WithoutConsumer();
        settings.Topics_[0].AppendPartitionIds(0);
        auto session = CreateReadSession(settings);
        auto start = WaitForReadEvent<TReadSessionEvent::TStartPartitionSessionEvent>(*session);
        start.Confirm(/* readOffset */ 1);
        const auto messages = ReadMessages(*session, 2);
        UNIT_ASSERT_VALUES_EQUAL(messages[0].GetOffset(), 1);
        UNIT_ASSERT_VALUES_EQUAL(messages[0].GetData(), "read-1");
        UNIT_ASSERT_VALUES_EQUAL(messages[1].GetOffset(), 2);
        UNIT_ASSERT_VALUES_EQUAL(messages[1].GetData(), "read-2");
        CloseSession(*session);
    }

    Y_UNIT_TEST_F(ReadSelectedPartition, TLocalTopicClientFixture) {
        constexpr char topic[] = "/Root/partitioned-topic";
        CreateTopic(topic, 2);
        WriteTopicMessages({"partition-0"}, WriteSettings(topic).MessageGroupId("").PartitionId(0));
        WriteTopicMessages({"partition-1"}, WriteSettings(topic).MessageGroupId("").PartitionId(1));
        auto settings = ReadSettings(topic);
        settings.Topics_[0].AppendPartitionIds(1);
        auto session = CreateReadSession(settings);
        const auto messages = ReadMessages(*session, 1);
        UNIT_ASSERT_VALUES_EQUAL(messages[0].GetData(), "partition-1");
        UNIT_ASSERT_VALUES_EQUAL(messages[0].GetPartitionSession()->GetPartitionId(), 1);
        CloseSession(*session);
    }

    Y_UNIT_TEST_F(DecompressGzipMessages, TLocalTopicClientFixture) {
        const std::string payload(4096, 'x');
        WriteTopicMessages({payload}, WriteSettings().Codec(ECodec::GZIP));
        auto session = CreateReadSession();
        const auto messages = ReadMessages(*session, 1);
        UNIT_ASSERT(!messages[0].HasException());
        UNIT_ASSERT_VALUES_EQUAL(messages[0].GetData(), payload);
        UNIT_ASSERT_VALUES_EQUAL(session->GetCounters()->BytesRead->Val(), payload.size());
        CloseSession(*session);
    }

    Y_UNIT_TEST_F(GetEventsWithCountAndByteLimits, TLocalTopicClientFixture) {
        WriteTopicMessages({"message"});
        auto session = CreateReadSession();
        UNIT_ASSERT(session->WaitEvent().Wait(TEST_TIMEOUT));
        UNIT_ASSERT(session->GetEvents(TReadSessionGetEventSettings().MaxEventsCount(0)).empty());
        auto events = session->GetEvents(TReadSessionGetEventSettings().MaxEventsCount(1));
        UNIT_ASSERT_VALUES_EQUAL(events.size(), 1);
        auto* start = std::get_if<TReadSessionEvent::TStartPartitionSessionEvent>(&events[0]);
        UNIT_ASSERT_C(start, DebugString(events[0]));
        start->Confirm();

        UNIT_ASSERT(session->WaitEvent().Wait(TEST_TIMEOUT));
        // A single data event may exceed the byte limit, but must still be returned.
        auto event = session->GetEvent(TReadSessionGetEventSettings().MaxByteSize(1));
        UNIT_ASSERT(event);
        const auto* data = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&*event);
        UNIT_ASSERT_C(data, DebugString(*event));
        UNIT_ASSERT_VALUES_EQUAL(data->GetMessagesCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(data->GetMessages()[0].GetData(), "message");
        CloseSession(*session);
    }

    Y_UNIT_TEST_F(MissingTopicClosesSession, TLocalTopicClientFixture) {
        auto session = CreateReadSession(ReadSettings("/Root/missing-topic"));
        AssertSessionClosed(*session, EStatus::SCHEME_ERROR);
        AssertSessionClosed(*session, EStatus::SCHEME_ERROR);
    }
}

} // namespace NKikimr::NKqp::NLocalTopicTests
