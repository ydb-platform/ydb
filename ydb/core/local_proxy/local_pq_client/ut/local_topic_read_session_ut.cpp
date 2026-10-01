#include "common.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/library/testlib/helpers.h>
#include <ydb/public/sdk/cpp/src/library/kafka/kafka_messages_int.h>
#include <ydb/public/sdk/cpp/src/library/kafka/kafka_records.h>

#include <util/datetime/base.h>

#include <limits>
#include <string_view>

namespace NKikimr::NKqp::NLocalTopicTests {

namespace {

class TTimestampReadSessionFixture : public TLocalTopicClientFixture {
public:
    void CheckKafkaTimestamps() {
        // Store Kafka bytes under a test codec so the server does not cut the
        // batch before it reaches the reader's Kafka metadata handling.
        TCodecMap::GetTheCodecMap().Set(static_cast<ui32>(ECodec::CUSTOM), std::make_unique<TKafkaBatchCodec>());
        const auto altered = TopicClient->AlterTopic(TOPIC_PATH, TAlterTopicSettings()
            .SetSupportedCodecs({ECodec::CUSTOM}).ClientTimeout(TEST_TIMEOUT)).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(altered.GetStatus(), EStatus::SUCCESS, altered.GetIssues().ToString());

        constexpr i64 minTimestamp = std::numeric_limits<i64>::min();
        constexpr i64 maxTimestamp = std::numeric_limits<i64>::max();
        const std::vector<std::string> payloads = {"small message", std::string(512_KB + 1, 'x')};
        std::vector<TInstant> expectedTimestamps;
        auto writeSession = CreateWriteSession();
        TTestWriter writer(writeSession);
        i32 nextSequence = 1;
        for (const auto compression : {NKafka::ECompressionType::NONE, NKafka::ECompressionType::GZIP, NKafka::ECompressionType::ZSTD}) {
            for (const i64 baseTimestamp : {minTimestamp, maxTimestamp}) {
                for (size_t i = 0; i < payloads.size(); ++i) {
                    NKafka::TKafkaRecordBatch batch;
                    batch.Magic = 2;
                    batch.Attributes = static_cast<i16>(compression);
                    batch.ProducerId = 42;
                    batch.ProducerEpoch = 0;
                    batch.BaseSequence = nextSequence;
                    batch.BaseTimestamp = baseTimestamp;
                    batch.MaxTimestamp = maxTimestamp;
                    NKafka::TKafkaRecord record;
                    record.OffsetDelta = 0;
                    record.TimestampDelta = i == 0 ? 0 : (baseTimestamp == minTimestamp ? -1 : 1);
                    record.SetValue(TString(payloads[i]));
                    record.Length = record.Size(2) - NKafka::NPrivate::SizeOfVarint<NKafka::TKafkaRecord::LengthMeta::Type>(0);
                    batch.Records.push_back(std::move(record));

                    // Expected Java long addition, independent of GetRecordTimestamp.
                    const i64 timestamp = i == 0 ? baseTimestamp : (baseTimestamp == minTimestamp ? maxTimestamp : minTimestamp);
                    expectedTimestamps.push_back(TInstant::MilliSeconds(static_cast<ui64>(timestamp)));
                    const TString bytes = NKafka::WriteKafkaRecordBatch(batch);
                    auto message = TWriteMessage::CompressedMessage(
                        std::string_view(bytes.data(), bytes.size()), ECodec::CUSTOM, payloads[i].size());
                    message.SeqNo(nextSequence);
                    AssertAck(writer.Write(std::move(message), true), nextSequence, nextSequence - 1);
                    ++nextSequence;
                }
            }
        }
        CloseSession(*writeSession);

        auto session = CreateReadSession();
        const auto messages = ReadMessages(*session, expectedTimestamps.size());
        for (size_t i = 0; i < messages.size(); ++i) {
            UNIT_ASSERT(!messages[i].HasException());
            UNIT_ASSERT_VALUES_EQUAL(messages[i].GetData(), payloads[i % payloads.size()]);
            UNIT_ASSERT_VALUES_EQUAL(messages[i].GetOffset(), i);
            UNIT_ASSERT_VALUES_EQUAL(messages[i].GetSeqNo(), i + 1);
            UNIT_ASSERT_VALUES_EQUAL(messages[i].GetCreateTime(), expectedTimestamps[i]);
        }
        UNIT_ASSERT_VALUES_EQUAL(session->GetCounters()->MessagesRead->Val(), messages.size());
        CloseSession(*session);
    }
};

} // namespace

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

    Y_UNIT_TEST_TWIN_F(CommitMessageAndResumeConsumer, NativeSdk, TLocalTopicClientFixture) {
        WriteTopicMessages({"committed", "remaining-1", "remaining-2"});
        {
            auto session = NativeSdk ? TopicClient->CreateReadSession(ReadSettings()) : CreateReadSession();
            auto messages = ReadMessages(*session, 3);
            messages[0].Commit();
            const auto ack = WaitForReadEvent<TReadSessionEvent::TCommitOffsetAcknowledgementEvent>(*session);
            UNIT_ASSERT_VALUES_EQUAL(ack.GetCommittedOffset(), 1);
            UNIT_ASSERT_VALUES_EQUAL(ack.GetPartitionSession()->GetPartitionId(), 0);
            if constexpr (NativeSdk) {
                UNIT_ASSERT(session->Close(TEST_TIMEOUT));
            } else {
                CloseSession(*session);
            }
        }

        auto session = NativeSdk ? TopicClient->CreateReadSession(ReadSettings()) : CreateReadSession();
        auto start = WaitForReadEvent<TReadSessionEvent::TStartPartitionSessionEvent>(*session);
        UNIT_ASSERT_VALUES_EQUAL(start.GetCommittedOffset(), 1);
        UNIT_ASSERT_VALUES_EQUAL(start.GetEndOffset(), 3);
        start.Confirm();
        const auto messages = ReadMessages(*session, 2);
        UNIT_ASSERT_VALUES_EQUAL(messages[0].GetOffset(), 1);
        UNIT_ASSERT_VALUES_EQUAL(messages[0].GetData(), "remaining-1");
        UNIT_ASSERT_VALUES_EQUAL(messages[1].GetOffset(), 2);
        UNIT_ASSERT_VALUES_EQUAL(messages[1].GetData(), "remaining-2");
        if constexpr (NativeSdk) {
            UNIT_ASSERT(session->Close(TEST_TIMEOUT));
        } else {
            CloseSession(*session);
        }
    }

    Y_UNIT_TEST_TWIN_F(ResumeConsumerInsideKafkaBatch, NativeSdk, TLocalTopicClientFixture) {
        Kikimr->GetTestServer().GetRuntime()->GetAppData().FeatureFlags.SetEnableTopicMessagesBatching(true);
        Kikimr->GetTestServer().GetRuntime()->GetAppData().FeatureFlags.SetEnableTopicWriteOffsetDeltaInKeys(true);
        constexpr char topicPath[] = "/Root/kafka-validation";
        CreateTopic(topicPath);
        const auto altered = TopicClient->AlterTopic(topicPath, TAlterTopicSettings()
            .SetSupportedCodecs({ECodec::KAFKA_BATCH})).GetValue(TEST_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL_C(altered.GetStatus(), EStatus::SUCCESS, altered.GetIssues().ToString());
        NKafka::TKafkaRecordBatch batch;
        batch.Magic = 2;
        batch.ProducerId = 42;
        batch.ProducerEpoch = 0;
        batch.BaseSequence = 1;
        batch.LastOffsetDelta = 1;
        for (i32 i = 0; i < 2; ++i) {
            NKafka::TKafkaRecord record;
            record.OffsetDelta = i;
            record.SetValue(i == 0 ? "committed" : "remaining");
            record.Length = record.Size(2) - NKafka::NPrivate::SizeOfVarint<NKafka::TKafkaRecord::LengthMeta::Type>(0);
            batch.Records.push_back(std::move(record));
        }
        const TString bytes = NKafka::WriteKafkaRecordBatch(batch);
        auto writeSession = TopicClient->CreateWriteSession(WriteSettings(topicPath).Codec(ECodec::KAFKA_BATCH));
        TTestWriter writer(writeSession);
        auto message = TWriteMessage::CompressedMessage(
            std::string_view(bytes.data(), bytes.size()), ECodec::KAFKA_BATCH, 18);
        message.SeqNo(1);
        AssertAck(writer.Write(std::move(message), true), 1, 0);
        UNIT_ASSERT(writeSession->Close(TEST_TIMEOUT));
        AssertTopicEndOffset(2, topicPath);
        const auto committed = TopicClient->CommitOffset(topicPath, 0, CONSUMER, 1).GetValue(TEST_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL_C(committed.GetStatus(), EStatus::SUCCESS, committed.GetIssues().ToString());
        auto session = NativeSdk ? TopicClient->CreateReadSession(ReadSettings(topicPath)) : CreateReadSession(ReadSettings(topicPath));
        auto data = WaitForReadEvent<TReadSessionEvent::TDataReceivedEvent>(*session);
        UNIT_ASSERT_VALUES_EQUAL(data.GetMessagesCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(data.GetMessages()[0].GetOffset(), 1);
        UNIT_ASSERT_VALUES_EQUAL(data.GetMessages()[0].GetData(), "remaining");
        data.Commit();
        const auto ack = WaitForReadEvent<TReadSessionEvent::TCommitOffsetAcknowledgementEvent>(*session);
        UNIT_ASSERT_VALUES_EQUAL(ack.GetCommittedOffset(), 2);
        if constexpr (NativeSdk) {
            UNIT_ASSERT(session->Close(TEST_TIMEOUT));
        } else {
            CloseSession(*session);
        }
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

    Y_UNIT_TEST_TWIN_F(StartFromExplicitOffsetWithoutConsumer, NativeSdk, TLocalTopicClientFixture) {
        WriteTopicMessages({"skip", "read-1", "read-2"});
        auto settings = ReadSettings().WithoutConsumer();
        settings.Topics_[0].AppendPartitionIds(0);
        auto session = NativeSdk ? TopicClient->CreateReadSession(settings) : CreateReadSession(settings);
        auto start = WaitForReadEvent<TReadSessionEvent::TStartPartitionSessionEvent>(*session);
        start.Confirm(/* readOffset */ 1);
        const auto messages = ReadMessages(*session, 2);
        UNIT_ASSERT_VALUES_EQUAL(messages[0].GetOffset(), 1);
        UNIT_ASSERT_VALUES_EQUAL(messages[0].GetData(), "read-1");
        UNIT_ASSERT_VALUES_EQUAL(messages[1].GetOffset(), 2);
        UNIT_ASSERT_VALUES_EQUAL(messages[1].GetData(), "read-2");
        if constexpr (NativeSdk) {
            UNIT_ASSERT(session->Close(TEST_TIMEOUT));
        } else {
            CloseSession(*session);
        }
    }

    Y_UNIT_TEST_OCTET_F(CommitAfterExplicitReadOffset, LargeMessages, Gzip, NativeSdk, TLocalTopicClientFixture) {
        const std::string payload(LargeMessages ? 600 * 1024 : 16, 'x');
        WriteTopicMessages({"skip", payload, "last"}, WriteSettings().Codec(Gzip ? ECodec::GZIP : ECodec::RAW));
        auto session = NativeSdk ? TopicClient->CreateReadSession(ReadSettings()) : CreateReadSession();
        auto start = WaitForReadEvent<TReadSessionEvent::TStartPartitionSessionEvent>(*session);
        start.Confirm(/* readOffset */ 1);

        auto messages = ReadMessages(*session, 2);
        UNIT_ASSERT_VALUES_EQUAL(messages[0].GetOffset(), 1);
        UNIT_ASSERT_VALUES_EQUAL(messages[0].GetData(), payload);
        UNIT_ASSERT_VALUES_EQUAL(messages[1].GetOffset(), 2);

        // Reading alone must not commit the skipped prefix.
        messages[0].GetPartitionSession()->RequestStatus();
        const auto initialStatus = WaitForReadEvent<TReadSessionEvent::TPartitionSessionStatusEvent>(*session);
        UNIT_ASSERT_VALUES_EQUAL(initialStatus.GetCommittedOffset(), 0);

        // Skipped offsets must not cause a delivered, uncommitted message to be committed.
        messages[1].Commit();
        const auto gapAck = WaitForReadEvent<TReadSessionEvent::TCommitOffsetAcknowledgementEvent>(*session);
        UNIT_ASSERT_VALUES_EQUAL(gapAck.GetCommittedOffset(), 1);
        messages[1].GetPartitionSession()->RequestStatus();
        const auto status = WaitForReadEvent<TReadSessionEvent::TPartitionSessionStatusEvent>(*session);
        UNIT_ASSERT_VALUES_EQUAL(status.GetCommittedOffset(), 1);
        messages[0].Commit();
        const auto ack = WaitForReadEvent<TReadSessionEvent::TCommitOffsetAcknowledgementEvent>(*session);
        UNIT_ASSERT_VALUES_EQUAL(ack.GetCommittedOffset(), 3);
        if constexpr (NativeSdk) {
            UNIT_ASSERT(session->Close(TEST_TIMEOUT));
        } else {
            CloseSession(*session);
        }
    }

    Y_UNIT_TEST_QUAD_F(CommitAfterExplicitReadAndCommitOffsets, NativeSdk, Deferred, TLocalTopicClientFixture) {
        WriteTopicMessages({"committed", "skipped", "remaining"});
        auto session = NativeSdk ? TopicClient->CreateReadSession(ReadSettings()) : CreateReadSession();
        auto start = WaitForReadEvent<TReadSessionEvent::TStartPartitionSessionEvent>(*session);
        start.Confirm(/* readOffset */ 2, /* commitOffset */ 1);

        auto messages = ReadMessages(*session, 1);
        UNIT_ASSERT_VALUES_EQUAL(messages[0].GetOffset(), 2);
        UNIT_ASSERT_VALUES_EQUAL(messages[0].GetData(), "remaining");
        const auto deadline = TInstant::Now() + TEST_TIMEOUT;
        ui64 committedOffset = 0;
        do {
            messages[0].GetPartitionSession()->RequestStatus();
            const auto status = WaitForReadEvent<TReadSessionEvent::TPartitionSessionStatusEvent>(*session, deadline);
            committedOffset = status.GetCommittedOffset();
            UNIT_ASSERT_LE(committedOffset, 1);
            if (committedOffset == 1) {
                break;
            }
            Sleep(TDuration::MilliSeconds(20));
        } while (TInstant::Now() < deadline);
        UNIT_ASSERT_VALUES_EQUAL(committedOffset, 1);
        if constexpr (Deferred) {
            NYdb::NTopic::TDeferredCommit deferred;
            deferred.Add(messages[0]);
            deferred.Commit();
        } else {
            messages[0].Commit();
        }
        const auto ack = WaitForReadEvent<TReadSessionEvent::TCommitOffsetAcknowledgementEvent>(*session);
        UNIT_ASSERT_VALUES_EQUAL(ack.GetCommittedOffset(), 3);
        if constexpr (NativeSdk) {
            UNIT_ASSERT(session->Close(TEST_TIMEOUT));
        } else {
            CloseSession(*session);
        }
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

    Y_UNIT_TEST_F(ReadKafkaBatchesWithWrappingTimestamps, TTimestampReadSessionFixture) {
        CheckKafkaTimestamps();
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
