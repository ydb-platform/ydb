#include <ydb/core/persqueue/ut/common/autoscaling_ut_common.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/ut/common/pq_ut_common.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/library/kafka/kafka_messages_int.h>
#include <ydb/library/kafka/kafka_records.h>

#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/write_session.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/persqueue/public/partition_key_range/partition_key_range.h>
#include <ydb/core/persqueue/pqrb/partition_scale_manager.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_env.h>

#include <util/stream/output.h>

namespace NKikimr {

using namespace NYdb::NTopic;
using namespace NYdb::NTopic::NTests;
using namespace NSchemeShardUT_Private;
using namespace NKikimr::NPQ::NTest;

using TDataEvent = NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent;
using TStartPartitionEvent = NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent;
using TStopPartitionEvent = NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent;
using TCommitOffsetAcknowledgementEvent = NYdb::NTopic::TReadSessionEvent::TCommitOffsetAcknowledgementEvent;
using TMessage = TDataEvent::TMessage;

#define UNIT_ASSERT_TIME_EQUAL(A, B, D)                                                               \
  do {                                                                                                \
    if (!(((A - B) >= TDuration::Zero()) && ((A - B) <= D))                                           \
            && !(((B - A) >= TDuration::Zero()) && ((B - A) <= D))) {                                 \
        auto&& failMsg = Sprintf("%s and %s diferent more then %s", (::TStringBuilder() << A).data(), \
            (::TStringBuilder() << B).data(), (::TStringBuilder() << D).data());                      \
        UNIT_FAIL_IMPL("assertion failure", failMsg);                                                 \
    }                                                                                                 \
  } while (false)


namespace {

TTopicSdkTestSetup CreateBatchingSetup() {
    NKikimrConfig::TFeatureFlags ff;
    ff.SetEnableTopicMessagesBatching(true);
    ff.SetEnableTopicWriteOffsetDeltaInKeys(true);

    auto settings = TTopicSdkTestSetup::MakeServerSettings();
    settings.SetFeatureFlags(ff);

    TTopicSdkTestSetup setup("KafkaBatchMessagesWriteRead", settings, false);
    setup.CreateTopic();
    return setup;
}

// Writes groups of messages as kafka batches: one tuple is {first seqno, message count, payload fill char}.
void WriteKafkaBatchMessages(
        TTopicClient& client,
        const std::string& topicPath,
        const std::string& producerId,
        size_t dataSize,
        ui64 maxBatchMessageCount,
        const TVector<std::tuple<ui64, ui32, char>>& writes)
{
    TWriteSessionSettings writeSettings;
    writeSettings.Path(topicPath)
        .ProducerId(producerId)
        .MessageGroupId(producerId)
        .PartitionId(0)
        .Codec(ECodec::RAW)
        .BatchFlushMessageCount(maxBatchMessageCount)
        .MessageFormat(EMessageFormat::KAFKA_BATCH)
        // Groups smaller than BatchFlushMessageCount are flushed by this interval;
        // writes within a group are fast enough to never split a group.
        .BatchFlushInterval(TDuration::Seconds(1))
        .BatchFlushSizeBytes(10_MB);

    auto writeSession = client.CreateWriteSession(writeSettings);

    std::optional<TContinuationToken> token;

    for (const auto& [firstSeqNo, messageCount, fill] : writes) {
        const ui64 lastSeqNo = firstSeqNo + messageCount - 1;
        ui64 nextSeqNo = firstSeqNo;
        THashSet<ui64> ackedSeqNos;

        while (ackedSeqNos.size() < messageCount) {
            if (token.has_value() && nextSeqNo <= lastSeqNo) {
                TWriteMessage message(TString(dataSize, fill));
                message.SeqNo(nextSeqNo++);
                writeSession->Write(std::move(*token), std::move(message));
                token.reset();
                continue;
            }

            auto event = writeSession->GetEvent(true);
            UNIT_ASSERT(event.has_value());
            if (auto* ready = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&*event)) {
                token = std::move(ready->ContinuationToken);
            } else if (auto* ack = std::get_if<TWriteSessionEvent::TAcksEvent>(&*event)) {
                for (const auto& item : ack->Acks) {
                    UNIT_ASSERT_C(
                        item.State == TWriteSessionEvent::TWriteAck::EES_WRITTEN,
                        TStringBuilder() << "Unexpected ack state: " << item.State);
                    UNIT_ASSERT_C(
                        item.SeqNo >= firstSeqNo && item.SeqNo <= lastSeqNo,
                        TStringBuilder() << "Unexpected ack seqNo: " << item.SeqNo);
                    ackedSeqNos.insert(item.SeqNo);
                }
            } else if (auto* closed = std::get_if<TSessionClosedEvent>(&*event)) {
                UNIT_FAIL(TStringBuilder() << "Unexpected session close: " << closed->DebugString());
            }
        }
    }

    UNIT_ASSERT(writeSession->Close(TDuration::Seconds(5)));
}

} // namespace

Y_UNIT_TEST_SUITE(WithSDK) {

    TString MakeKafkaBatchPayload(const TVector<TString>& values, ui64 baseSequence) {
        NKafka::TKafkaRecordBatch batch;
        batch.BaseOffset = 0;
        batch.Magic = 2;
        batch.Attributes = static_cast<NKafka::TKafkaRecordBatch::AttributesMeta::Type>(NKafka::ECompressionType::NONE);
        batch.LastOffsetDelta = values.size() - 1;
        batch.BaseTimestamp = 1000;
        batch.MaxTimestamp = 1000 + values.size() - 1;
        batch.ProducerId = 42;
        batch.ProducerEpoch = 0;
        batch.BaseSequence = baseSequence;

        batch.Records.reserve(values.size());
        for (size_t i = 0; i < values.size(); ++i) {
            NKafka::TKafkaRecord record;
            record.TimestampDelta = i;
            record.OffsetDelta = i;
            record.SetValue(TString{values[i]});
            record.Length = record.Size(2)
                - NKafka::NPrivate::SizeOfVarint<NKafka::TKafkaRecord::LengthMeta::Type>(0);
            batch.Records.push_back(std::move(record));
        }

        batch.BatchLength = batch.Size(2)
            - sizeof(NKafka::TKafkaRecordBatch::BaseOffsetMeta::Type)
            - sizeof(NKafka::TKafkaRecordBatch::BatchLengthMeta::Type);
        return NKafka::WriteKafkaRecordBatch(batch);
    }

    ui64 GetPQTabletId(TTopicSdkTestSetup& setup, const TString& topicPath, ui32 partitionId) {
        auto pathDescr = setup.GetServer().AnnoyingClient->Describe(&setup.GetRuntime(), topicPath, Tests::SchemeRoot, true);
        const auto& partitions = pathDescr.GetPathDescription().GetPersQueueGroup().GetPartitions();
        for (const auto& partition : partitions) {
            if (partition.GetPartitionId() == partitionId) {
                UNIT_ASSERT(partition.GetTabletId());
                return partition.GetTabletId();
            }
        }
        UNIT_FAIL(TStringBuilder() << "Partition " << partitionId << " not found in " << pathDescr.DebugString());
        return 0;
    }

    void WriteKafkaBatch(
        TTopicSdkTestSetup& setup,
        ui64 tabletId,
        const TActorId& edge,
        const ui32 partitionId,
        const TString& ownerCookie,
        ui32 messageNo,
        const TString& sourceId,
        ui64 seqNo,
        const TVector<TString>& values,
        i64 offset)
    {
        TAutoPtr<IEventHandle> handle;
        THolder<TEvPersQueue::TEvRequest> request(new TEvPersQueue::TEvRequest);
        auto* req = request->Record.MutablePartitionRequest();
        req->SetPartition(partitionId);
        req->SetOwnerCookie(ownerCookie);
        req->SetMessageNo(messageNo);
        req->SetCmdWriteOffset(offset);

        auto* write = req->AddCmdWrite();
        write->SetSourceId(sourceId);
        write->SetSeqNo(seqNo);

        NKikimrPQClient::TDataChunk dataChunk;
        dataChunk.SetChunkType(NKikimrPQClient::TDataChunk::REGULAR);
        dataChunk.SetCodec(NPersQueueCommon::RAW);
        dataChunk.SetData(MakeKafkaBatchPayload(values, seqNo));

        TString serializedDataChunk;
        Y_ENSURE(dataChunk.SerializeToString(&serializedDataChunk));
        write->SetData(std::move(serializedDataChunk));
        write->SetMessageCount(values.size());
        write->SetMessageFormat(NKikimrClient::KAFKA_BATCH);
        write->SetMaxSeqNo(seqNo + values.size() - 1);

        setup.GetRuntime().SendToPipe(tabletId, edge, request.Release(), 0, GetPipeConfigWithRetries());
        auto* result = setup.GetRuntime().GrabEdgeEventIf<TEvPersQueue::TEvResponse>(handle,
            [](const TEvPersQueue::TEvResponse& ev) {
                return ev.Record.HasPartitionResponse()
                    && ev.Record.GetPartitionResponse().CmdWriteResultSize() > 0
                    || ev.Record.GetErrorCode() != NPersQueue::NErrorCode::OK;
            });

        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL_C(
            static_cast<ui32>(result->Record.GetErrorCode()),
            static_cast<ui32>(NPersQueue::NErrorCode::OK),
            result->Record.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetPartitionResponse().CmdWriteResultSize(), 1u);
    }

    Y_UNIT_TEST(DescribeConsumer) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 1);

        auto describe = [&]() {
            return setup.DescribeConsumer(TEST_TOPIC, TEST_CONSUMER);
        };

        std::deque<TString> messagesTextQueue;
        auto write = [&](size_t seqNo) {
            TTopicClient client(setup.MakeDriver());

            TWriteSessionSettings settings;
            settings.Path(TEST_TOPIC);
            settings.PartitionId(0);
            settings.DeduplicationEnabled(false);
            auto session = client.CreateSimpleBlockingWriteSession(settings);

            TString msgTxt = TStringBuilder() << "message_" << seqNo;
            TWriteMessage msg(msgTxt);
            constexpr size_t maxSeqNo = 10;
            Y_ASSERT(seqNo <= maxSeqNo);
            msg.CreateTimestamp(TInstant::Now() - TDuration::Seconds(maxSeqNo - seqNo));
            UNIT_ASSERT(session->Write(std::move(msg)));
            messagesTextQueue.push_back(msgTxt);
            session->Close(TDuration::Seconds(5));
        };

        Cerr << ">>>>> Check describe for empty topic\n";
        {
            auto d = describe();
            UNIT_ASSERT_STRINGS_EQUAL(TEST_CONSUMER, d.GetConsumer().GetConsumerName());
            UNIT_ASSERT_VALUES_EQUAL(1, d.GetPartitions().size());
            auto& p = d.GetPartitions()[0];
            UNIT_ASSERT_VALUES_EQUAL(0, p.GetPartitionId());
            UNIT_ASSERT_VALUES_EQUAL(true, p.GetActive());
            UNIT_ASSERT_VALUES_EQUAL(0, p.GetPartitionStats()->GetEndOffset());
            auto& c = p.GetPartitionConsumerStats();
            UNIT_ASSERT_VALUES_EQUAL(true, c.has_value());
            UNIT_ASSERT_VALUES_EQUAL(0, c->GetCommittedOffset());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(0), c->GetMaxWriteTimeLag());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(0), c->GetMaxReadTimeLag());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(0), c->GetMaxCommittedTimeLag());
            UNIT_ASSERT_TIME_EQUAL(TInstant::Now(), c->GetLastReadTime(), TDuration::Seconds(3)); // why not zero?
            UNIT_ASSERT_VALUES_EQUAL(0, c->GetLastReadOffset());
        }

        write(3);
        Sleep(TDuration::Seconds(2));
        write(7);
        Sleep(TDuration::Seconds(2));
        write(10);

        Cerr << ">>>>> Check describe for topic which contains messages, but consumer hasn`t read\n";
        {
            auto d = describe();
            UNIT_ASSERT_STRINGS_EQUAL(TEST_CONSUMER, d.GetConsumer().GetConsumerName());
            UNIT_ASSERT_VALUES_EQUAL(1, d.GetPartitions().size());
            auto& p = d.GetPartitions()[0];
            UNIT_ASSERT_VALUES_EQUAL(0, p.GetPartitionId());
            UNIT_ASSERT_VALUES_EQUAL(true, p.GetActive());
            UNIT_ASSERT_VALUES_EQUAL(3, p.GetPartitionStats()->GetEndOffset());
            auto& c = p.GetPartitionConsumerStats();
            UNIT_ASSERT_VALUES_EQUAL(true, c.has_value());
            UNIT_ASSERT_VALUES_EQUAL(0, c->GetCommittedOffset());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(7), c->GetMaxWriteTimeLag()); //
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(0), c->GetMaxReadTimeLag());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(4), c->GetMaxCommittedTimeLag());
            UNIT_ASSERT_TIME_EQUAL(TInstant::Now(), c->GetLastReadTime(), TDuration::Seconds(3)); // why not zero?
            UNIT_ASSERT_VALUES_EQUAL(1, c->GetLastReadOffset());
        }

        UNIT_ASSERT(setup.Commit(TEST_TOPIC, TEST_CONSUMER, 0, 1).IsSuccess());
        messagesTextQueue.pop_front();

        Cerr << ">>>>> Check describe for topic whis contains messages, has commited offset but hasn`t read (restart tablet for example)\n";
        {
            auto d = describe();
            UNIT_ASSERT_STRINGS_EQUAL(TEST_CONSUMER, d.GetConsumer().GetConsumerName());
            UNIT_ASSERT_VALUES_EQUAL(1, d.GetPartitions().size());
            auto& p = d.GetPartitions()[0];
            UNIT_ASSERT_VALUES_EQUAL(0, p.GetPartitionId());
            UNIT_ASSERT_VALUES_EQUAL(true, p.GetActive());
            UNIT_ASSERT_VALUES_EQUAL(3, p.GetPartitionStats()->GetEndOffset());
            auto& c = p.GetPartitionConsumerStats();
            UNIT_ASSERT_VALUES_EQUAL(true, c.has_value());
            UNIT_ASSERT_VALUES_EQUAL(1, c->GetCommittedOffset());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(7), c->GetMaxWriteTimeLag());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(0), c->GetMaxReadTimeLag());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(2), c->GetMaxCommittedTimeLag());
            UNIT_ASSERT_TIME_EQUAL(TInstant::Now(), c->GetLastReadTime(), TDuration::Seconds(3)); // why not zero?
            UNIT_ASSERT_VALUES_EQUAL(1, c->GetLastReadOffset());
        }

        {
            TTopicClient client(setup.MakeDriver());
            TReadSessionSettings settings;
            settings.ConsumerName(TEST_CONSUMER);
            settings.AppendTopics(TTopicReadSettings().Path(TEST_TOPIC));

            auto session = client.CreateReadSession(settings);

            TInstant endTime = TInstant::Now() + TDuration::Seconds(5);
            while (true) {
                auto e = session->GetEvent();
                if (e) {
                    Cerr << ">>>>> Event = " << e->index() << Endl << Flush;
                }
                if (e && std::holds_alternative<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(e.value())) {
                    for (const auto& message : std::get<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(e.value()).GetMessages()) {
                        UNIT_ASSERT(!messagesTextQueue.empty());
                        UNIT_ASSERT_VALUES_EQUAL(message.GetData(), messagesTextQueue.front());
                        messagesTextQueue.pop_front();
                    }
                    if (messagesTextQueue.empty()) {
                        // we must receive data events for all messages except the first one
                        break;
                    }
                } else if (e && std::holds_alternative<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(e.value())) {
                    std::get<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(e.value()).Confirm();
                }
                UNIT_ASSERT_C(endTime > TInstant::Now(), "Unable wait");
            }

            session->Close(TDuration::Seconds(1));
        }

        Cerr << ">>>>> Check describe for topic wich contains messages, has commited offset of first message and read second message\n";
        {
            auto d = describe();
            UNIT_ASSERT_STRINGS_EQUAL(TEST_CONSUMER, d.GetConsumer().GetConsumerName());
            UNIT_ASSERT_VALUES_EQUAL(1, d.GetPartitions().size());
            auto& p = d.GetPartitions()[0];
            UNIT_ASSERT_VALUES_EQUAL(0, p.GetPartitionId());
            UNIT_ASSERT_VALUES_EQUAL(true, p.GetActive());
            UNIT_ASSERT_VALUES_EQUAL(3, p.GetPartitionStats()->GetEndOffset());
            auto& c = p.GetPartitionConsumerStats();
            UNIT_ASSERT_VALUES_EQUAL(true, c.has_value());
            UNIT_ASSERT_VALUES_EQUAL(1, c->GetCommittedOffset());
            //UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(7), c->GetMaxWriteTimeLag());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(0), c->GetMaxReadTimeLag());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(2), c->GetMaxCommittedTimeLag());
            UNIT_ASSERT_TIME_EQUAL(TInstant::Now(), c->GetLastReadTime(), TDuration::Seconds(3));
            UNIT_ASSERT_VALUES_EQUAL(3, c->GetLastReadOffset());
        }
    }

    Y_UNIT_TEST(ReadWithBadTopic) {
        TTopicSdkTestSetup setup = CreateSetup();

        TTopicClient client(setup.MakeDriver());

        std::vector<std::string> topics = { "//", "==", "--", "**", "@@", "!!", "##", "$$", "\%\%", "^^", "::", "&&", "??",
             "\\\\", "||", "++", "--", ",,", "..", "``", "~~", ";;", "::", "((", "))", "[[", "]]", "{{", "}}", ""};
        for (auto& topic : topics) {
            Cerr << "Checking topic: '" << topic << "'" << Endl;

            TReadSessionSettings settings;
            settings.AppendTopics(TTopicReadSettings().Path(topic));
            settings.ConsumerName(TEST_CONSUMER);
            auto session = client.CreateReadSession(settings);
            auto event = session->GetEvent(true);
            // check that verify didn`t happened
            UNIT_ASSERT(event.has_value());
        }
    }

    Y_UNIT_TEST(ReadWithBadConsumer) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC);

        TTopicClient client(setup.MakeDriver());

        std::vector<std::string> consumers = { "//", "==", "--", "**", "@@", "!!", "##", "$$", "\%\%", "^^", "::", "&&", "??",
             "\\\\", "||", "++", "--", ",,", "..", "``", "~~", ";;", "::", "((", "))", "[[", "]]", "{{", "}}", ""};
        for (auto& consumer : consumers) {
            Cerr << "Checking consumer: '" << consumer << "'" << Endl;

            TReadSessionSettings settings;
            settings.AppendTopics(TTopicReadSettings().Path(TEST_TOPIC));
            settings.ConsumerName(consumer);
            auto session = client.CreateReadSession(settings);
            auto event = session->GetEvent(true);
            // check that verify didn`t happened
            UNIT_ASSERT(event.has_value());
        }
    }

    Y_UNIT_TEST(Read_WithConsumer_WithBadPartitions) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC);

        TTopicClient client(setup.MakeDriver());

        TReadSessionSettings settings;
        settings.AppendTopics(TTopicReadSettings().Path(TEST_TOPIC)
            .AppendPartitionIds(0)
            .AppendPartitionIds(101)
            .AppendPartitionIds(113));
        settings.ConsumerName(TEST_CONSUMER);
        auto session = client.CreateReadSession(settings);
        auto event = session->GetEvent(true);
        // check that verify didn`t happened
        UNIT_ASSERT(event.has_value());
    }

    Y_UNIT_TEST(Read_WithoutConsumer_WithBadPartitions) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC);

        TTopicClient client(setup.MakeDriver());

        TReadSessionSettings settings;
        settings.AppendTopics(TTopicReadSettings().Path(TEST_TOPIC)
            .AppendPartitionIds(0)
            .AppendPartitionIds(101)
            .AppendPartitionIds(113));
        settings.WithoutConsumer();
        auto session = client.CreateReadSession(settings);
        auto event = session->GetEvent(true);
        // check that verify didn`t happened
        UNIT_ASSERT(event.has_value());
    }

    Y_UNIT_TEST(WriteBigMessage) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.GetRuntime().GetAppData().PQConfig.SetMaxMessageSizeBytes(1_KB);
        setup.CreateTopic(TEST_TOPIC);

        TTopicClient client(setup.MakeDriver());
        TWriteSessionSettings settings;
        settings.Path(TEST_TOPIC)
            .ProducerId("producer-1")
            .MessageGroupId("producer-1")
            .Codec(ECodec::RAW);

        auto session = client.CreateWriteSession(settings);

        auto event = session->GetEvent(true);
        UNIT_ASSERT(event.has_value());
        auto* readyEvent = std::get_if<NYdb::NTopic::TWriteSessionEvent::TReadyToAcceptEvent>(&*event);
        UNIT_ASSERT(readyEvent);
        session->Write(std::move(readyEvent->ContinuationToken), TString(2_KB, 'a'));

        for (size_t i = 0; i < 10; ++i) {
            event = session->GetEvent(false);
            if (event.has_value()) {
                auto closeEvent = std::get_if<NYdb::NTopic::TSessionClosedEvent>(&*event);
                if (closeEvent) {
                    UNIT_ASSERT_C(closeEvent->GetIssues().ToOneLineString().contains("Too big message"), closeEvent->GetIssues().ToOneLineString());
                    return;
                }
            }

            Sleep(TDuration::Seconds(1));
        }

        UNIT_ASSERT_C(false, "Session closed event not received");
    }

    Y_UNIT_TEST(WriteSessionClosedWithSessionExpiredWhenOwnershipLost) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 1);

        TTopicClient client(setup.MakeDriver());

        auto createSession = [&]() {
            TWriteSessionSettings settings;
            settings.Path(TEST_TOPIC)
                .ProducerId("producer-1")
                .MessageGroupId("producer-1")
                .PartitionId(0)
                .Codec(ECodec::RAW)
                .BatchFlushInterval(TDuration::Zero())
                .BatchFlushSizeBytes(0)
                .RetryPolicy(NYdb::NTopic::IRetryPolicy::GetNoRetryPolicy());
            return client.CreateWriteSession(settings);
        };

        auto waitReady = [](const std::shared_ptr<IWriteSession>& session) {
            for (;;) {
                auto event = session->GetEvent(true);
                UNIT_ASSERT(event.has_value());
                if (auto* ready = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&*event)) {
                    return std::move(ready->ContinuationToken);
                }
                if (auto* closed = std::get_if<TSessionClosedEvent>(&*event)) {
                    UNIT_FAIL(TStringBuilder() << "Unexpected session close while waiting for ready: "
                                               << closed->DebugString());
                }
            }
        };

        auto waitAck = [](const std::shared_ptr<IWriteSession>& session) {
            for (;;) {
                auto event = session->GetEvent(true);
                UNIT_ASSERT(event.has_value());
                if (auto* ack = std::get_if<TWriteSessionEvent::TAcksEvent>(&*event)) {
                    UNIT_ASSERT(!ack->Acks.empty());
                    for (const auto& item : ack->Acks) {
                        UNIT_ASSERT_C(
                            item.State == TWriteSessionEvent::TWriteAck::EES_WRITTEN
                                || item.State == TWriteSessionEvent::TWriteAck::EES_ALREADY_WRITTEN,
                            TStringBuilder() << "Unexpected ack state: " << item.State);
                    }
                    return;
                }
                if (auto* closed = std::get_if<TSessionClosedEvent>(&*event)) {
                    UNIT_FAIL(TStringBuilder() << "Unexpected session close while waiting for ack: "
                                               << closed->DebugString());
                }
            }
        };

        auto waitClosed = [](const std::shared_ptr<IWriteSession>& session) {
            for (;;) {
                auto event = session->GetEvent(true);
                UNIT_ASSERT(event.has_value());
                if (auto* closed = std::get_if<TSessionClosedEvent>(&*event)) {
                    return *closed;
                }
            }
        };

        auto first = createSession();
        first->Write(waitReady(first), "first", 1);
        waitAck(first);

        auto second = createSession();
        second->Write(waitReady(second), "second", 2);
        waitAck(second);

        const auto closed = waitClosed(first);
        UNIT_ASSERT_VALUES_EQUAL_C(closed.GetStatus(), NYdb::EStatus::SESSION_EXPIRED, closed.DebugString());

        UNIT_ASSERT(second->Close(TDuration::Seconds(5)));
    }

    Y_UNIT_TEST(WithPartitionMaxInFlightBytesSetting) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC);

        TTopicClient client(setup.MakeDriver());

        TWriteSessionSettings writeSettings;
        writeSettings.Path(TEST_TOPIC)
            .ProducerId("producer-1")
            .MessageGroupId("producer-1")
            .Codec(ECodec::RAW);
        auto writeSession = client.CreateSimpleBlockingWriteSession(writeSettings);

        auto writeMessages = [&](size_t count) {
            for (size_t i = 0; i < count; ++i) {
                writeSession->Write(TString(1000, 'a'));
            }
        };

        writeMessages(10);
        Sleep(TDuration::Seconds(5));

        TReadSessionSettings settings;
        settings.AppendTopics(TTopicReadSettings().Path(TEST_TOPIC)
            .AppendPartitionIds(0));
        settings.ConsumerName(TEST_CONSUMER);
        settings.PartitionMaxInFlightBytes(10000);
        auto session = client.CreateReadSession(settings);

        TVector<TMessage> first10;
        first10.reserve(10);

        // 1) Read first 10 messages, without committing them
        while (first10.size() < 10) {
            UNIT_ASSERT_C(session->WaitEvent().Wait(TDuration::Seconds(30)), TStringBuilder() << "No event received at iteration, first10 size: " << first10.size());
            auto eventOpt = session->GetEvent(false);
            auto& event = *eventOpt;

            if (auto* start = std::get_if<TStartPartitionEvent>(&event)) {
                start->Confirm();
                continue;
            }
            if (auto* stop = std::get_if<TStopPartitionEvent>(&event)) {
                stop->Confirm();
                continue;
            }

            if (auto* data = std::get_if<TDataEvent>(&event)) {
                for (auto& msg : data->GetMessages()) {
                    if (first10.size() >= 10) {
                        break;
                    }
                    first10.push_back(msg);
                }
            }
        }

        writeMessages(10);
        Sleep(TDuration::Seconds(5));

        // 2) Check that no new events appear due to the inflight bytes limit
        {
            auto future = session->WaitEvent();
            UNIT_ASSERT(!future.Wait(TDuration::Seconds(3)));
        }

        // 3) Commit these 10 messages
        for (auto& msg : first10) {
            msg.Commit();
        }

        first10.clear();

        // 4) Check that after commit new events start appearing again
        while (first10.size() < 10) {
            UNIT_ASSERT_C(session->WaitEvent().Wait(TDuration::Seconds(30)), TStringBuilder() << "No event received at iteration, first10 size: " << first10.size());
            auto eventOpt = session->GetEvent(false);
            auto& event = *eventOpt;

            if (auto* stop = std::get_if<TStopPartitionEvent>(&event)) {
                stop->Confirm();
                break;
            }

            if (auto* data = std::get_if<TDataEvent>(&event)) {
                for (auto& msg : data->GetMessages()) {
                    first10.push_back(msg);
                }
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(10, first10.size());
        UNIT_ASSERT(session->Close(TDuration::Seconds(5)));
    }

    Y_UNIT_TEST(WithPartitionMaxInFlightBytesSetting_LimitOneMessage) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC);

        TTopicClient client(setup.MakeDriver());

        TWriteSessionSettings writeSettings;
        writeSettings.Path(TEST_TOPIC)
            .ProducerId("producer-1")
            .MessageGroupId("producer-1")
            .Codec(ECodec::RAW);
        auto writeSession = client.CreateSimpleBlockingWriteSession(writeSettings);

        auto writeMessages = [&](size_t count) {
            for (size_t i = 0; i < count; ++i) {
                writeSession->Write(TString(1_KB, 'a'));
            }
        };

        writeMessages(1);

        TReadSessionSettings settings;
        settings.AppendTopics(TTopicReadSettings().Path(TEST_TOPIC)
            .AppendPartitionIds(0));
        settings.ConsumerName(TEST_CONSUMER);
        settings.PartitionMaxInFlightBytes(1_KB);
        auto session = client.CreateReadSession(settings);

        auto readMessages = [&](size_t count) {
            TVector<TMessage> result;
            while (result.size() < count) {
                UNIT_ASSERT_C(session->WaitEvent().Wait(TDuration::Seconds(30)), TStringBuilder()
                    << "No event received, messages size: " << result.size());
                auto eventOpt = session->GetEvent(false);
                UNIT_ASSERT(eventOpt);
                auto& event = *eventOpt;

                if (auto* start = std::get_if<TStartPartitionEvent>(&event)) {
                    start->Confirm();
                    continue;
                }
                if (auto* stop = std::get_if<TStopPartitionEvent>(&event)) {
                    stop->Confirm();
                    continue;
                }
                if (std::get_if<TCommitOffsetAcknowledgementEvent>(&event)) {
                    continue;
                }

                if (auto* data = std::get_if<TDataEvent>(&event)) {
                    for (auto& msg : data->GetMessages()) {
                        result.push_back(msg);
                    }
                }
            }
            return result;
        };

        auto messages = readMessages(1);
        UNIT_ASSERT_VALUES_EQUAL(1, messages.size());

        writeMessages(1);
        Sleep(TDuration::Seconds(5));

        // The first message is still in-flight, so the second one should not be delivered yet.
        {
            auto future = session->WaitEvent();
            UNIT_ASSERT(!future.Wait(TDuration::Seconds(3)));
        }

        messages[0].Commit();
        messages.clear();

        // Once the first message is committed, partition in-flight bytes are released
        // and the second message can be read.
        messages = readMessages(1);
        UNIT_ASSERT_VALUES_EQUAL(1, messages.size());

        UNIT_ASSERT(session->Close(TDuration::Seconds(5)));
    }

    Y_UNIT_TEST(WithPartitionMaxInFlightBytesSetting_UnorderedCommitGap) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC);

        TTopicClient client(setup.MakeDriver());

        TWriteSessionSettings writeSettings;
        writeSettings.Path(TEST_TOPIC)
            .ProducerId("producer-1")
            .MessageGroupId("producer-1")
            .Codec(ECodec::RAW);
        auto writeSession = client.CreateSimpleBlockingWriteSession(writeSettings);

        auto writeMessages = [&](size_t count) {
            for (size_t i = 0; i < count; ++i) {
                writeSession->Write(TString(1000, 'a'));
            }
        };

        writeMessages(20);
        Sleep(TDuration::Seconds(5));

        auto writeOneMoreMessage = [&] {
            writeMessages(1);
            Sleep(TDuration::Seconds(5));
        };

        TReadSessionSettings settings;
        settings.AppendTopics(TTopicReadSettings().Path(TEST_TOPIC)
            .AppendPartitionIds(0));
        settings.ConsumerName(TEST_CONSUMER);
        settings.PartitionMaxInFlightBytes(10000);
        auto session = client.CreateReadSession(settings);

        auto readUntil = [&](size_t count) {
            TVector<TMessage> result;
            while (result.size() < count) {
                UNIT_ASSERT_C(session->WaitEvent().Wait(TDuration::Seconds(30)), TStringBuilder()
                    << "No event received, messages size: " << result.size());
                auto eventOpt = session->GetEvent(false);
                UNIT_ASSERT(eventOpt);
                auto& event = *eventOpt;

                if (auto* start = std::get_if<TStartPartitionEvent>(&event)) {
                    start->Confirm();
                    continue;
                }
                if (auto* stop = std::get_if<TStopPartitionEvent>(&event)) {
                    stop->Confirm();
                    continue;
                }
                if (std::get_if<TCommitOffsetAcknowledgementEvent>(&event)) {
                    continue;
                }

                if (auto* data = std::get_if<TDataEvent>(&event)) {
                    for (auto& msg : data->GetMessages()) {
                        result.push_back(msg);
                    }
                }
            }
            return result;
        };

        auto firstBatch = readUntil(10);
        UNIT_ASSERT_C(firstBatch.size() >= 10, TStringBuilder()
            << "Expected at least 10 messages, got " << firstBatch.size());

        writeOneMoreMessage();

        // Simulate unordered processing: later messages finish and call Commit(),
        // but the first offset is still a gap. The server cannot advance the
        // committed offset through this gap, so partition in-flight bytes remain held.
        for (size_t i = 1; i < firstBatch.size(); ++i) {
            firstBatch[i].Commit();
        }

        {
            auto future = session->WaitEvent();
            UNIT_ASSERT(!future.Wait(TDuration::Seconds(3)));
        }

        firstBatch[0].Commit();

        auto nextMessages = readUntil(1);
        UNIT_ASSERT_VALUES_EQUAL(1, nextMessages.size());

        UNIT_ASSERT(session->Close(TDuration::Seconds(5)));
    }

    Y_UNIT_TEST(WithPartitionMaxInFlightBytesSetting_DirectReadUnorderedCommitGap) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC);

        TTopicClient client(setup.MakeDriver());

        TWriteSessionSettings writeSettings;
        writeSettings.Path(TEST_TOPIC)
            .ProducerId("producer-1")
            .MessageGroupId("producer-1")
            .Codec(ECodec::RAW);
        auto writeSession = client.CreateSimpleBlockingWriteSession(writeSettings);

        auto writeMessages = [&](size_t count) {
            for (size_t i = 0; i < count; ++i) {
                writeSession->Write(TString(1000, 'a'));
            }
        };

        writeMessages(20);
        Sleep(TDuration::Seconds(5));

        auto writeOneMoreMessage = [&] {
            writeMessages(1);
            Sleep(TDuration::Seconds(5));
        };

        TReadSessionSettings settings;
        settings.AppendTopics(TTopicReadSettings().Path(TEST_TOPIC)
            .AppendPartitionIds(0));
        settings.ConsumerName(TEST_CONSUMER);
        settings.PartitionMaxInFlightBytes(10000);
        settings.DirectRead(true);
        auto session = client.CreateReadSession(settings);

        auto readUntil = [&](size_t count) {
            TVector<TMessage> result;
            while (result.size() < count) {
                UNIT_ASSERT_C(session->WaitEvent().Wait(TDuration::Seconds(30)), TStringBuilder()
                    << "No event received, messages size: " << result.size());
                auto eventOpt = session->GetEvent(false);
                UNIT_ASSERT(eventOpt);
                auto& event = *eventOpt;

                if (auto* start = std::get_if<TStartPartitionEvent>(&event)) {
                    start->Confirm();
                    continue;
                }
                if (auto* stop = std::get_if<TStopPartitionEvent>(&event)) {
                    stop->Confirm();
                    continue;
                }
                if (std::get_if<TCommitOffsetAcknowledgementEvent>(&event)) {
                    continue;
                }

                if (auto* data = std::get_if<TDataEvent>(&event)) {
                    for (auto& msg : data->GetMessages()) {
                        result.push_back(msg);
                    }
                }
            }
            return result;
        };

        auto firstBatch = readUntil(10);
        UNIT_ASSERT_C(firstBatch.size() >= 10, TStringBuilder()
            << "Expected at least 10 messages, got " << firstBatch.size());

        writeOneMoreMessage();

        for (size_t i = 1; i < firstBatch.size(); ++i) {
            firstBatch[i].Commit();
        }

        {
            auto future = session->WaitEvent();
            UNIT_ASSERT(!future.Wait(TDuration::Seconds(3)));
        }

        firstBatch[0].Commit();

        auto nextMessages = readUntil(1);
        UNIT_ASSERT_VALUES_EQUAL(1, nextMessages.size());

        UNIT_ASSERT(session->Close(TDuration::Seconds(5)));
    }

    Y_UNIT_TEST(KafkaBatchesReadThroughSdkAreCut) {
        auto serverSettings = TTopicSdkTestSetup::MakeServerSettings();
        serverSettings.FeatureFlags.SetEnableTopicMessagesBatching(true);
        serverSettings.FeatureFlags.SetEnableTopicWriteOffsetDeltaInKeys(true);

        TTopicSdkTestSetup setup{TEST_CASE_NAME, serverSettings, false};
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 1);

        constexpr ui32 partitionId = 0;
        constexpr size_t batchCount = 3;
        constexpr size_t messagesPerBatch = 10;

        const ui64 tabletId = GetPQTabletId(setup, setup.GetFullTopicPath(), partitionId);
        const auto edge = setup.GetRuntime().AllocateEdgeActor();
        const TString ownerCookie = NPQ::CmdSetOwner(&setup.GetRuntime(), tabletId, edge, partitionId).first;

        TVector<TString> expected;
        expected.reserve(batchCount * messagesPerBatch);
        for (size_t batchIdx = 0; batchIdx < batchCount; ++batchIdx) {
            TVector<TString> values;
            values.reserve(messagesPerBatch);
            for (size_t messageIdx = 0; messageIdx < messagesPerBatch; ++messageIdx) {
                values.push_back(TStringBuilder() << "value-" << batchIdx << "-" << messageIdx);
                expected.push_back(values.back());
            }

            WriteKafkaBatch(
                setup,
                tabletId,
                edge,
                partitionId,
                ownerCookie,
                batchIdx,
                "sourceid_kafka_batch_sdk",
                batchIdx * messagesPerBatch + 1,
                values,
                batchIdx * messagesPerBatch);
        }

        TTopicClient client(setup.MakeDriver());
        TReadSessionSettings settings;
        settings.ConsumerName(TEST_CONSUMER);
        settings.AppendTopics(TTopicReadSettings().Path(TEST_TOPIC));

        auto session = client.CreateReadSession(settings);
        TVector<TString> actual;
        actual.reserve(expected.size());

        const TInstant deadline = TInstant::Now() + TDuration::Seconds(30);
        while (actual.size() < expected.size()) {
            UNIT_ASSERT_C(deadline > TInstant::Now(), "Unable to read all messages");
            auto event = session->GetEvent();
            if (!event) {
                continue;
            }

            if (auto* start = std::get_if<TStartPartitionEvent>(&*event)) {
                start->Confirm();
            } else if (auto* data = std::get_if<TDataEvent>(&*event)) {
                for (const auto& message : data->GetMessages()) {
                    UNIT_ASSERT_VALUES_EQUAL(message.GetOffset(), actual.size());
                    actual.push_back(TString{message.GetData()});
                }
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(actual.size(), expected.size());
        for (size_t i = 0; i < expected.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(actual[i], expected[i]);
        }

        UNIT_ASSERT(session->Close(TDuration::Seconds(5)));
    }

    Y_UNIT_TEST(WithPartitionMaxInFlightBytesSetting_ManyPartitions) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 3, 3);

        TTopicClient client(setup.MakeDriver());

        auto describeResult = client.DescribeTopic(TEST_TOPIC).GetValueSync();
        UNIT_ASSERT(describeResult.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(3, describeResult.GetTopicDescription().GetPartitions().size());

        const auto& partitions = describeResult.GetTopicDescription().GetPartitions();

        TWriteSessionSettings writeSettings1;
        writeSettings1.Path(TEST_TOPIC)
            .ProducerId("producer-1")
            .MessageGroupId("producer-1")
            .Codec(ECodec::RAW)
            .PartitionId(partitions[0].GetPartitionId())
            .DirectWriteToPartition(true);

        TWriteSessionSettings writeSettings2 = writeSettings1;
        writeSettings1.ProducerId("producer-2");
        writeSettings1.MessageGroupId("producer-2");
        writeSettings2.PartitionId(partitions[1].GetPartitionId());

        TWriteSessionSettings writeSettings3 = writeSettings1;
        writeSettings3.ProducerId("producer-3");
        writeSettings3.MessageGroupId("producer-3");
        writeSettings3.PartitionId(partitions[2].GetPartitionId());

        std::unordered_map<ui32, std::shared_ptr<ISimpleBlockingWriteSession>> writeSessions;
        writeSessions[partitions[0].GetPartitionId()] = client.CreateSimpleBlockingWriteSession(writeSettings1);
        writeSessions[partitions[1].GetPartitionId()] = client.CreateSimpleBlockingWriteSession(writeSettings2);
        writeSessions[partitions[2].GetPartitionId()] = client.CreateSimpleBlockingWriteSession(writeSettings3);

        std::vector<TMessage> receivedMessages;
        std::vector<ui64> committedOffsets;

        auto writeMessages = [&](size_t count, ui32 partitionId) {
            for (size_t i = 0; i < count; ++i) {
                writeSessions[partitionId]->Write(TString(1000, 'a'));
            }
        };

        auto handleEvents = [&](std::shared_ptr<IReadSession> session) {
            while (true) {
                auto eventOpt = session->GetEvent(false);
                if (!eventOpt) {
                    break;
                }
                auto& event = *eventOpt;

                if (auto* start = std::get_if<TStartPartitionEvent>(&event)) {
                    start->Confirm();
                    continue;
                }
                if (auto* stop = std::get_if<TStopPartitionEvent>(&event)) {
                    stop->Confirm();
                    continue;
                }
                if (auto* data = std::get_if<TDataEvent>(&event)) {
                    for (auto& msg : data->GetMessages()) {
                        receivedMessages.push_back(msg);
                    }
                    continue;
                }
                if (auto* commit = std::get_if<TCommitOffsetAcknowledgementEvent>(&event)) {
                    committedOffsets.push_back(commit->GetCommittedOffset());
                    continue;
                }
                if (std::get_if<TSessionClosedEvent>(&event)) {
                    return;
                }
            }
        };

        TReadSessionSettings settings;
        settings.AppendTopics(
            TTopicReadSettings()
            .Path(TEST_TOPIC)
            .AppendPartitionIds(0)
            .AppendPartitionIds(1)
            .AppendPartitionIds(2));
        settings.ConsumerName(TEST_CONSUMER);
        settings.PartitionMaxInFlightBytes(2000);
        auto session = client.CreateReadSession(settings);

        auto readMessages = [&](size_t count, TDuration timeout) {
            auto deadline = TInstant::Now() + timeout;
            while (receivedMessages.size() < count && TInstant::Now() < deadline) {
                UNIT_ASSERT_C(session->WaitEvent().Wait(TDuration::Seconds(30)), TStringBuilder() << "No event received in 30 seconds");
                handleEvents(session);
            }
            UNIT_ASSERT_VALUES_EQUAL(count, receivedMessages.size());
        };

        auto waitCommitMessages = [&](size_t count, TDuration timeout) {
            auto deadline = TInstant::Now() + timeout;
            while (committedOffsets.size() < count && TInstant::Now() < deadline) {
                UNIT_ASSERT_C(session->WaitEvent().Wait(TDuration::Seconds(30)), TStringBuilder() << "No event received in 30 seconds");
                Sleep(TDuration::Seconds(1));
                handleEvents(session);
            }
            UNIT_ASSERT_VALUES_EQUAL(count, committedOffsets.size());
        };

        writeMessages(2, partitions[0].GetPartitionId());
        Sleep(TDuration::Seconds(5));
        readMessages(2, TDuration::Seconds(30));

        writeMessages(1, partitions[0].GetPartitionId());
        writeMessages(2, partitions[1].GetPartitionId());
        writeMessages(2, partitions[2].GetPartitionId());
        Sleep(TDuration::Seconds(10));
        readMessages(6, TDuration::Seconds(30));

        std::unordered_set<ui64> expectedPartitions = { partitions[1].GetPartitionId(), partitions[2].GetPartitionId() };
        for (size_t i = 0; i < receivedMessages.size(); ++i) {
            if (i >= 2) {
                UNIT_ASSERT(expectedPartitions.contains(receivedMessages[i].GetPartitionSession()->GetPartitionId()));
            } else {
                UNIT_ASSERT_VALUES_EQUAL(partitions[0].GetPartitionId(), receivedMessages[i].GetPartitionSession()->GetPartitionId());
            }

            receivedMessages[i].Commit();
        }

        receivedMessages.clear();
        waitCommitMessages(6, TDuration::Seconds(30));
        readMessages(1, TDuration::Seconds(30));
        receivedMessages[0].Commit();

        UNIT_ASSERT(session->Close(TDuration::Seconds(5)));
        for (const auto& [_, writeSession] : writeSessions) {
            UNIT_ASSERT(writeSession->Close(TDuration::Seconds(5)));
        }
    }

    Y_UNIT_TEST(KafkaBatchMessagesWriteRead) {
        TTopicSdkTestSetup setup = CreateBatchingSetup();
        setup.GetRuntime().SetScheduledLimit(5000);

        TTopicClient client(setup.MakeDriver());

        const std::string producerId = "sourceid_batch_read";
        constexpr size_t dataSize = 16;
        constexpr ui64 maxBatchMessageCount = 5;

        const TVector<std::tuple<ui64, ui32, char>> writes = {
            {1, 5, 'a'},
            {6, 3, 'b'},
            {9, 1, 'c'},
        };

        WriteKafkaBatchMessages(client, setup.GetFullTopicPath(), producerId, dataSize, maxBatchMessageCount, writes);

        TReadSessionSettings readSettings;
        readSettings.ConsumerName(TEST_CONSUMER);
        readSettings.AppendTopics(TTopicReadSettings().Path(setup.GetFullTopicPath()));
        readSettings.Decompress(true);

        auto readSession = client.CreateReadSession(readSettings);

        TVector<TMessage> receivedMessages;
        const TInstant deadline = TInstant::Now() + TDuration::Seconds(30);
        while (receivedMessages.size() < 9 && TInstant::Now() < deadline) {
            UNIT_ASSERT_C(readSession->WaitEvent().Wait(TDuration::Seconds(5)),
                "Read session event timeout");
            auto event = readSession->GetEvent(false);
            UNIT_ASSERT(event.has_value());

            if (auto* start = std::get_if<TStartPartitionEvent>(&*event)) {
                start->Confirm();
                continue;
            }
            if (auto* stop = std::get_if<TStopPartitionEvent>(&*event)) {
                stop->Confirm();
                continue;
            }
            if (auto* data = std::get_if<TDataEvent>(&*event)) {
                for (auto& message : data->GetMessages()) {
                    receivedMessages.push_back(message);
                }
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(receivedMessages.size(), 9u);

        for (ui64 offset = 0; offset < 5; ++offset) {
            UNIT_ASSERT_VALUES_EQUAL(receivedMessages[offset].GetOffset(), offset);
            UNIT_ASSERT_VALUES_EQUAL(receivedMessages[offset].GetSeqNo(), offset + 1);
            UNIT_ASSERT_VALUES_EQUAL(receivedMessages[offset].GetData(), TString(dataSize, 'a'));
        }
        for (ui64 offset = 5; offset < 8; ++offset) {
            UNIT_ASSERT_VALUES_EQUAL(receivedMessages[offset].GetOffset(), offset);
            UNIT_ASSERT_VALUES_EQUAL(receivedMessages[offset].GetSeqNo(), offset + 1);
            UNIT_ASSERT_VALUES_EQUAL(receivedMessages[offset].GetData(), TString(dataSize, 'b'));
        }
        UNIT_ASSERT_VALUES_EQUAL(receivedMessages[8].GetOffset(), 8u);
        UNIT_ASSERT_VALUES_EQUAL(receivedMessages[8].GetSeqNo(), 9u);
        UNIT_ASSERT_VALUES_EQUAL(receivedMessages[8].GetData(), TString(dataSize, 'c'));

        UNIT_ASSERT(readSession->Close(TDuration::Seconds(5)));
    }

    Y_UNIT_TEST(KafkaBatchCommitToMiddleOfBatch) {
        TTopicSdkTestSetup setup = CreateBatchingSetup();
        setup.GetRuntime().SetScheduledLimit(5000);

        TTopicClient client(setup.MakeDriver());

        const std::string producerId = "sourceid_batch_commit";
        constexpr size_t dataSize = 16;
        constexpr ui64 maxBatchMessageCount = 5;
        constexpr ui64 totalMessages = 8;

        // One full batch [0..4] and one smaller batch [5..7].
        const TVector<std::tuple<ui64, ui32, char>> writes = {
            {1, 5, 'a'},
            {6, 3, 'b'},
        };

        WriteKafkaBatchMessages(client, setup.GetFullTopicPath(), producerId, dataSize, maxBatchMessageCount, writes);

        // Commit to the middle of the first batch.
        constexpr ui64 commitToOffset = 2;

        TReadSessionSettings readSettings;
        readSettings.ConsumerName(TEST_CONSUMER);
        readSettings.AppendTopics(TTopicReadSettings().Path(setup.GetFullTopicPath()));
        readSettings.Decompress(true);

        // First session: read everything, commit only offsets [0, commitToOffset).
        {
            auto readSession = client.CreateReadSession(readSettings);

            size_t messagesReceived = 0;
            bool committed = false;
            const TInstant deadline = TInstant::Now() + TDuration::Seconds(30);
            while ((messagesReceived < totalMessages || !committed) && TInstant::Now() < deadline) {
                UNIT_ASSERT_C(readSession->WaitEvent().Wait(TDuration::Seconds(5)),
                    "Read session event timeout");
                auto event = readSession->GetEvent(false);
                UNIT_ASSERT(event.has_value());

                if (auto* start = std::get_if<TStartPartitionEvent>(&*event)) {
                    start->Confirm();
                    continue;
                }
                if (auto* stop = std::get_if<TStopPartitionEvent>(&*event)) {
                    stop->Confirm();
                    continue;
                }
                if (auto* commit = std::get_if<TCommitOffsetAcknowledgementEvent>(&*event)) {
                    if (commit->GetCommittedOffset() >= commitToOffset) {
                        committed = true;
                    }
                    continue;
                }
                if (auto* data = std::get_if<TDataEvent>(&*event)) {
                    for (auto& message : data->GetMessages()) {
                        UNIT_ASSERT_VALUES_EQUAL(message.GetOffset(), messagesReceived);
                        if (message.GetOffset() < commitToOffset) {
                            message.Commit();
                        }
                        ++messagesReceived;
                    }
                }
            }

            UNIT_ASSERT_VALUES_EQUAL(messagesReceived, totalMessages);
            UNIT_ASSERT_C(committed, "Commit acknowledgement was not received");
            UNIT_ASSERT(readSession->Close(TDuration::Seconds(5)));
        }

        // Second session: reading must resume from the middle of the first batch.
        {
            auto readSession = client.CreateReadSession(readSettings);

            TVector<TMessage> receivedMessages;
            const TInstant deadline = TInstant::Now() + TDuration::Seconds(30);
            while (receivedMessages.size() < totalMessages - commitToOffset && TInstant::Now() < deadline) {
                UNIT_ASSERT_C(readSession->WaitEvent().Wait(TDuration::Seconds(5)),
                    "Read session event timeout");
                auto event = readSession->GetEvent(false);
                UNIT_ASSERT(event.has_value());

                if (auto* start = std::get_if<TStartPartitionEvent>(&*event)) {
                    UNIT_ASSERT_VALUES_EQUAL(start->GetCommittedOffset(), commitToOffset);
                    start->Confirm();
                    continue;
                }
                if (auto* stop = std::get_if<TStopPartitionEvent>(&*event)) {
                    stop->Confirm();
                    continue;
                }
                if (auto* data = std::get_if<TDataEvent>(&*event)) {
                    for (auto& message : data->GetMessages()) {
                        receivedMessages.push_back(message);
                    }
                }
            }

            UNIT_ASSERT_VALUES_EQUAL(receivedMessages.size(), totalMessages - commitToOffset);

            for (size_t i = 0; i < receivedMessages.size(); ++i) {
                const ui64 offset = commitToOffset + i;
                UNIT_ASSERT_VALUES_EQUAL(receivedMessages[i].GetOffset(), offset);
                UNIT_ASSERT_VALUES_EQUAL(receivedMessages[i].GetSeqNo(), offset + 1);
                UNIT_ASSERT_VALUES_EQUAL(receivedMessages[i].GetData(), TString(dataSize, offset < 5 ? 'a' : 'b'));
            }

            UNIT_ASSERT(readSession->Close(TDuration::Seconds(5)));
        }
    }
}
} // namespace NKikimr
