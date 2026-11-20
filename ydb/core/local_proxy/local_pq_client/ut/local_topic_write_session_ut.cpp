#include "common.h"

#include <string_view>

namespace NKikimr::NKqp::NLocalTopicTests {

namespace {

class TLocalTopicWriteSessionFixture : public TLocalTopicClientFixture {
public:
    static void AssertPendingInitSeqNo(IWriteSession& session) {
        // The first request after closure must return a pending future without
        // trying to contact the finished actor. Repeated requests must also work.
        const auto first = session.GetInitSeqNo();
        const auto second = session.GetInitSeqNo();
        UNIT_ASSERT(first.Initialized());
        UNIT_ASSERT(second.Initialized());
        UNIT_ASSERT(!first.IsReady());
        UNIT_ASSERT(!second.IsReady());
    }

    template <typename TWrite>
    void TestWriteAfterClose(TWrite&& write) {
        auto session = CreateWriteSession();
        auto token = WaitForContinuationToken(*session);
        CloseSession(*session);

        // A caller may still hold a valid continuation token when closure arrives.
        UNIT_ASSERT_NO_EXCEPTION(write(*session, std::move(token)));
        AssertSessionClosed(*session);

        AssertTopicEndOffset(0);
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TLocalTopicWriteSession) {
    Y_UNIT_TEST_F(WriteMessagesWithAutomaticSequenceNumbers, TLocalTopicClientFixture) {
        auto session = CreateWriteSession(WriteSettings().MaxInflightCount(1));
        TTestWriter writer(session);
        AssertAck(writer.Write(TWriteMessage("first")), 1, 0);
        AssertAck(writer.Write(TWriteMessage("second")), 2, 1);
        AssertAck(writer.Write(TWriteMessage("third")), 3, 2);
        UNIT_ASSERT_VALUES_EQUAL(session->GetCounters()->MessagesWritten->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(session->GetCounters()->BytesWritten->Val(), 16);
        UNIT_ASSERT_VALUES_EQUAL(session->GetCounters()->BytesInflightTotal->Val(), 0);
        CloseSession(*session);
        AssertTopicEndOffset(3);
        AssertTopicMessages({"first", "second", "third"});
    }

    Y_UNIT_TEST_F(DeduplicateAndResumeSequenceNumbers, TLocalTopicClientFixture) {
        {
            auto session = CreateWriteSession();
            UNIT_ASSERT_VALUES_EQUAL(session->GetInitSeqNo().GetValue(TEST_TIMEOUT), 0);
            TTestWriter writer(session);
            auto first = TWriteMessage("first");
            first.SeqNo(42);
            AssertAck(writer.Write(std::move(first)), 42, 0);

            auto duplicate = TWriteMessage("duplicate must be skipped");
            duplicate.SeqNo(42);
            const auto ack = writer.Write(std::move(duplicate));
            UNIT_ASSERT_VALUES_EQUAL(ack.SeqNo, 42);
            UNIT_ASSERT_VALUES_EQUAL(ack.State, TWriteSessionEvent::TWriteAck::EES_ALREADY_WRITTEN);
            UNIT_ASSERT(!ack.Details);
            CloseSession(*session);
        }

        auto session = CreateWriteSession();
        UNIT_ASSERT_VALUES_EQUAL(session->GetInitSeqNo().GetValue(TEST_TIMEOUT), 42);
        TTestWriter writer(session);
        auto next = TWriteMessage("next");
        next.SeqNo(43);
        AssertAck(writer.Write(std::move(next)), 43, 1);
        CloseSession(*session);
        AssertTopicEndOffset(2);
        AssertTopicMessages({"first", "next"});
    }

    Y_UNIT_TEST_F(WriteMetadataAndTimestamp, TLocalTopicClientFixture) {
        auto session = CreateWriteSession(WriteSettings().AppendSessionMeta("session-key", "session-value"));
        TTestWriter writer(session);
        const auto createdAt = TInstant::MilliSeconds(1700000000123);
        auto message = TWriteMessage("metadata message");
        message.SeqNo(42).CreateTimestamp(createdAt).MessageMeta({{"message-key", "message-value"}});
        AssertAck(writer.Write(std::move(message)), 42, 0);
        CloseSession(*session);

        auto reader = TopicClient->CreateReadSession(ReadSettings());
        const auto messages = ReadMessages(*reader, 1);
        const auto& received = messages[0];
        UNIT_ASSERT_VALUES_EQUAL(received.GetData(), "metadata message");
        UNIT_ASSERT_VALUES_EQUAL(received.GetProducerId(), "producer");
        UNIT_ASSERT_VALUES_EQUAL(received.GetSeqNo(), 42);
        UNIT_ASSERT_VALUES_EQUAL(received.GetCreateTime(), createdAt);
        UNIT_ASSERT(received.GetMeta());
        UNIT_ASSERT_VALUES_EQUAL(received.GetMeta()->Fields.at("session-key"), "session-value");
        UNIT_ASSERT(received.GetMessageMeta());
        UNIT_ASSERT_VALUES_EQUAL(received.GetMessageMeta()->Fields.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(received.GetMessageMeta()->Fields[0].first, "message-key");
        UNIT_ASSERT_VALUES_EQUAL(received.GetMessageMeta()->Fields[0].second, "message-value");
        UNIT_ASSERT(reader->Close(TEST_TIMEOUT));
    }

    Y_UNIT_TEST_F(WriteEncodedGzipMessage, TLocalTopicClientFixture) {
        const std::string payload(4096, 'x');
        TBuffer compressed;
        {
            auto coder = TCodecMap::GetTheCodecMap().GetOrThrow(static_cast<ui32>(ECodec::GZIP))->CreateCoder(compressed, 6);
            coder->Write(payload);
            coder->Finish();
        }
        auto session = CreateWriteSession();
        TTestWriter writer(session);
        const std::string_view data(compressed.Data(), compressed.Size());
        AssertAck(writer.Write(TWriteMessage::CompressedMessage(data, ECodec::GZIP, payload.size()), true), 1, 0);
        CloseSession(*session);
        AssertTopicMessages({payload});
    }

    Y_UNIT_TEST_F(WriteToExplicitPartitionWithoutDeduplication, TLocalTopicClientFixture) {
        constexpr char topic[] = "/Root/partitioned-topic";
        CreateTopic(topic, 2);
        auto session = CreateWriteSession(WriteSettings(topic)
            .ProducerId("").MessageGroupId("").PartitionId(1).DeduplicationEnabled(false));
        TTestWriter writer(session);
        AssertAck(writer.Write(TWriteMessage("partition message")), 1, 0, 1);
        CloseSession(*session);
        auto settings = ReadSettings(topic);
        settings.Topics_[0].AppendPartitionIds(1);
        auto reader = TopicClient->CreateReadSession(settings);
        const auto messages = ReadMessages(*reader, 1);
        UNIT_ASSERT_VALUES_EQUAL(messages[0].GetData(), "partition message");
        UNIT_ASSERT_VALUES_EQUAL(messages[0].GetPartitionSession()->GetPartitionId(), 1);
        UNIT_ASSERT(reader->Close(TEST_TIMEOUT));
    }

    Y_UNIT_TEST_F(GetInitSeqNoAfterClose, TLocalTopicWriteSessionFixture) {
        auto session = CreateWriteSession();
        WaitForContinuationToken(*session);
        CloseSession(*session);

        AssertPendingInitSeqNo(*session);
        AssertSessionClosed(*session);
    }

    Y_UNIT_TEST_F(GetInitSeqNoAfterInitializationFailure, TLocalTopicWriteSessionFixture) {
        auto session = CreateWriteSession("/Root/missing-topic");
        AssertSessionClosed(*session, EStatus::SCHEME_ERROR);

        AssertPendingInitSeqNo(*session);
        AssertSessionClosed(*session, EStatus::SCHEME_ERROR);
    }

    Y_UNIT_TEST_F(GetInitSeqNoRequestedBeforeClose, TLocalTopicWriteSessionFixture) {
        auto session = CreateWriteSession();
        const auto initSeqNo = session->GetInitSeqNo();
        UNIT_ASSERT(initSeqNo.Wait(TEST_TIMEOUT));
        UNIT_ASSERT_VALUES_EQUAL(initSeqNo.GetValue(), 0);
        WaitForContinuationToken(*session);
        CloseSession(*session);

        const auto afterClose = session->GetInitSeqNo();
        UNIT_ASSERT(afterClose.IsReady());
        UNIT_ASSERT_VALUES_EQUAL(afterClose.GetValue(), initSeqNo.GetValue());
        AssertSessionClosed(*session);
    }

    Y_UNIT_TEST_F(WriteMessageWithAutoSeqNoAfterClose, TLocalTopicWriteSessionFixture) {
        TestWriteAfterClose([](IWriteSession& session, TContinuationToken&& token) {
            session.Write(std::move(token), TWriteMessage("late message"));
        });
    }

    Y_UNIT_TEST_F(WriteMessageWithManualSeqNoAfterClose, TLocalTopicWriteSessionFixture) {
        TestWriteAfterClose([](IWriteSession& session, TContinuationToken&& token) {
            auto message = TWriteMessage("late message");
            message.SeqNo(1);
            session.Write(std::move(token), std::move(message));
        });
    }

    Y_UNIT_TEST_F(WriteDataAfterClose, TLocalTopicWriteSessionFixture) {
        TestWriteAfterClose([](IWriteSession& session, TContinuationToken&& token) {
            session.Write(std::move(token), "late message");
        });
    }

    Y_UNIT_TEST_F(WriteEncodedMessageAfterClose, TLocalTopicWriteSessionFixture) {
        TestWriteAfterClose([](IWriteSession& session, TContinuationToken&& token) {
            constexpr std::string_view data = "late message";
            session.WriteEncoded(std::move(token), TWriteMessage::CompressedMessage(data, ECodec::RAW, data.size()));
        });
    }

    Y_UNIT_TEST_F(WriteEncodedDataAfterClose, TLocalTopicWriteSessionFixture) {
        TestWriteAfterClose([](IWriteSession& session, TContinuationToken&& token) {
            constexpr std::string_view data = "late message";
            session.WriteEncoded(std::move(token), data, ECodec::RAW, data.size(), 1);
        });
    }
}

} // namespace NKikimr::NKqp::NLocalTopicTests
