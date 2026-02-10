

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/retry/retry.h>

#include "kafka_test_client.h"
#include "test_server.h"

#include <ydb/core/kafka_proxy/kafka_transactional_producers_initializers.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/public/constants.h>

#include <ydb/public/sdk/cpp/src/client/persqueue_public/persqueue.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/codecs.h>
#include <ydb/public/api/grpc/draft/ydb_datastreams_v1.grpc.pb.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/digest/md5/md5.h>
#include <library/cpp/string_utils/base64/base64.h>

using namespace NKafka;
using namespace NYdb;
using namespace NYdb::NTable;

static constexpr const ui64 FirstTopicOffset = -2;
static constexpr const ui64 LastTopicOffset = -1;

TString GetMessageMetaKey(const NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage& msg, const TString& key) {
    if (msg.GetMessageMeta()) {
        for (auto& [k, v] : msg.GetMessageMeta()->Fields) {
            Cerr << ">>>>> key=" << k << ", value=" << v << Endl;
            if (k == key) {
                return TString{v};
            }
        }
    }
    return TString{};
}

void AssertMessageMeta(const NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage& msg, const TString& field,
                       const TString& expectedValue) {
    UNIT_ASSERT_VALUES_EQUAL_C(GetMessageMetaKey(msg, field), expectedValue, "Field " << field << " not found in message meta");
}

void AssertPartitionsIsUniqueAndCountIsExpected(std::vector<TReadInfo> readInfos, ui32 expectedPartitionsCount, TString topic) {
    std::unordered_set<int> partitions;
    ui32 partitionsCount = 0;
    for (TReadInfo readInfo: readInfos) {
        for (auto topicPartitions: readInfo.Partitions) {
            if (topicPartitions.Topic == topic) {
                for (auto partition: topicPartitions.Partitions) {
                    partitions.emplace(partition);
                    partitionsCount++;
                }
            }
        }
    }
    UNIT_ASSERT_VALUES_EQUAL(partitionsCount, expectedPartitionsCount);
    UNIT_ASSERT_VALUES_EQUAL(partitions.size(), expectedPartitionsCount);
}

std::vector<NTopic::TReadSessionEvent::TDataReceivedEvent> Read(std::shared_ptr<NYdb::NTopic::IReadSession> reader, bool lock = true) {
    std::vector<NTopic::TReadSessionEvent::TDataReceivedEvent> result;
    while (true) {
        auto event = reader->GetEvent(lock);
        if (!event)
            break;
        if (auto dataEvent = std::get_if<NTopic::TReadSessionEvent::TDataReceivedEvent>(&*event)) {
            result.push_back(*dataEvent);
            break;
        } else if (auto* lockEv = std::get_if<NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&*event)) {
            lockEv->Confirm();
        } else if (auto* releaseEv = std::get_if<NTopic::TReadSessionEvent::TStopPartitionSessionEvent>(&*event)) {
            releaseEv->Confirm();
        } else if (std::get_if<NTopic::TSessionClosedEvent>(&*event)) {
            break;
        }
    }
    Cerr << ">>>>> Received messages " << result.size() << Endl;
    return result;
}


std::vector<NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage> ReadCommitWaitAcks(std::shared_ptr<NTopic::IReadSession> topicReader, int messageCount = 1) {
    int gotMessages = 0;
    std::vector<NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage> result;
    bool gotAllMessages = false;
    int lastOffset = -1;
    while (!gotAllMessages) {
        auto event = topicReader->GetEvent(true);
        if (!event) {
            break;
        }
        if (auto dataEvent = std::get_if<NTopic::TReadSessionEvent::TDataReceivedEvent>(&*event)) {
            Cerr << ">>>>> Got TDataReceivedEvent " << dataEvent->DebugString() << Endl;
            lastOffset = dataEvent->GetMessages().back().GetOffset();
            gotMessages += dataEvent->GetMessages().size();
            for (auto& message : dataEvent->GetMessages()) {
                result.push_back(std::move(message));
            }
            if (gotMessages >= messageCount) {
                gotAllMessages = true;
            }
            dataEvent->Commit();
        } else if (auto* lockEv = std::get_if<NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&*event)) {
            lockEv->Confirm();
        } else if (auto* releaseEv = std::get_if<NTopic::TReadSessionEvent::TStopPartitionSessionEvent>(&*event)) {
            releaseEv->Confirm();
        } else if (auto* commitAck = std::get_if<NTopic::TReadSessionEvent::TCommitOffsetAcknowledgementEvent>(&*event)) {
            if (gotAllMessages && commitAck->GetCommittedOffset() == static_cast<ui64>(lastOffset + 1)) {
                break;
            }
        } else if (std::get_if<NTopic::TSessionClosedEvent>(&*event)) {
            break;
        }
    }
    return result;
}

void AssertMessageAvaialbleThroughLogbrokerApiAndCommit(std::shared_ptr<NTopic::IReadSession> topicReader) {
    auto responseFromLogbrokerApi = Read(topicReader);
    UNIT_ASSERT_VALUES_EQUAL(responseFromLogbrokerApi.size(), 1);

    UNIT_ASSERT_VALUES_EQUAL(responseFromLogbrokerApi[0].GetMessages().size(), 1);
    responseFromLogbrokerApi[0].GetMessages()[0].Commit();
}

void CreateTopic(NYdb::NTopic::TTopicClient& pqClient, TString& topicName, ui32 minActivePartitions,
                std::vector<TString> consumers,
                std::optional<ui64> retentionSeconds = std::nullopt,
                std::optional<TString> timestampType = std::nullopt,
                EStatus expectedStatus = EStatus::SUCCESS) {
    auto topicSettings = NYdb::NTopic::TCreateTopicSettings()
                            .PartitioningSettings(minActivePartitions, 100);

    if(retentionSeconds) {
        Cerr << "===Create topic with retention = " << *retentionSeconds << " seconds" << Endl;
        topicSettings.RetentionPeriod(TDuration::Seconds(*retentionSeconds));
    }
    if (timestampType) {
        Cerr << "===Create topic with timestampType = " << *timestampType << Endl;
        topicSettings.AddAttribute("_timestamp_type", *timestampType);
    }
    for (auto& consumer : consumers) {
        topicSettings.BeginAddConsumer(consumer).EndAddConsumer();
    }

    auto result = pqClient
                                .CreateTopic(topicName, topicSettings)
                                .ExtractValueSync();

    UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), expectedStatus, result.GetIssues().ToOneLineString());
}

void AlterTopic(NYdb::NTopic::TTopicClient& pqClient, TString& topicName, std::vector<TString> consumers) {
    auto topicSettings = NYdb::NTopic::TAlterTopicSettings();

    for (auto& consumer : consumers) {
        topicSettings.BeginAddConsumer(consumer).EndAddConsumer();
    }

    auto result = pqClient
                                .AlterTopic(topicName, topicSettings)
                                .ExtractValueSync();

    UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
    UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

}


Y_UNIT_TEST_SUITE(KafkaProtocol) {
    // this test imitates kafka producer behaviour:
    // 1. get api version,
    // 2. authenticate via sasl,
    // 3. acquire producer id,
    // 4. produce to topic several messages, read them and assert correct contents and metadata
    Y_UNIT_TEST(ProduceScenario) {
        TInsecureTestServer testServer("2");

        TString topicName = "/Root/topic-0-test";
        ui64 minActivePartitions = 10;

        NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
        CreateTopic(pqClient, topicName, minActivePartitions, {"consumer-0"});

        auto settings = NTopic::TReadSessionSettings()
                            .AppendTopics(NTopic::TTopicReadSettings(topicName))
                            .ConsumerName("consumer-0");
        auto topicReader = pqClient.CreateReadSession(settings);

        TKafkaTestClient client(testServer.Port);

        // authenticate
        {
            client.ScramAuthenticateToKafka("ouruser", "ourUserPassword");
        }

        // acquire producer id and epoch
        {
            auto msg = client.InitProducerId();

            UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        }

        // send test message
        {
            TString key = "record-key";
            TString value = "record-value";
            TString headerKey = "header-key";
            TString headerValue = "header-value";

            TKafkaRecordBatch batch;
            batch.BaseOffset = 3;
            batch.BaseSequence = 5;
            batch.Magic = 2; // Current supported
            batch.Records.resize(1);
            batch.Records[0].Key = TKafkaRawBytes(key.data(), key.size());
            batch.Records[0].Value = TKafkaRawBytes(value.data(), value.size());
            batch.Records[0].Headers.resize(1);
            batch.Records[0].Headers[0].Key = TKafkaRawBytes(headerKey.data(), headerKey.size());
            batch.Records[0].Headers[0].Value = TKafkaRawBytes(headerValue.data(), headerValue.size());

            auto msg = client.Produce(topicName, 0, batch);

            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Name, topicName);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].PartitionResponses[0].Index, 0);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].PartitionResponses[0].ErrorCode,
                                     static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

            // read message from topic to assert delivery
            {
                std::vector<std::pair<TString, std::vector<i32>>> topics {{topicName, {0}}};
                auto msg = client.Fetch(topics);

                UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
                auto record = msg->Responses[0].Partitions[0].Records->Records[0];

                auto recordValue = record.Value.value();
                auto recordValuesAsStr = TString(recordValue.data(), recordValue.size());
                UNIT_ASSERT_VALUES_EQUAL(recordValuesAsStr, value);

                auto readRecordKey = record.Key.value();
                auto readRecordKeysAsStr = TString(readRecordKey.data(), readRecordKey.size());
                UNIT_ASSERT_VALUES_EQUAL(readRecordKeysAsStr, key);

                auto readHeaderKey = record.Headers[0].Key.value();
                auto readHeaderKeyStr = TString(readHeaderKey.data(), readHeaderKey.size());
                UNIT_ASSERT_VALUES_EQUAL(readHeaderKeyStr, headerKey);

                auto readHeaderValue = record.Headers[0].Value.value();
                auto readHeaderValueStr = TString(readHeaderValue.data(), readHeaderValue.size());
                UNIT_ASSERT_VALUES_EQUAL(readHeaderValueStr, headerValue);
            }

            auto msg2 = client.Produce(topicName, 0, batch);

            // read by logbroker protocol
            auto readMessages = ReadCommitWaitAcks(topicReader, 2);

            auto& readMessage = readMessages[0];
            UNIT_ASSERT_STRINGS_EQUAL(readMessage.GetData(), value);
            AssertMessageMeta(readMessage, "__key", key);
            AssertMessageMeta(readMessage, headerKey, headerValue);
        }

        // send empty produce message
        {
            TKafkaRecordBatch batch;
            batch.BaseOffset = 3;
            batch.BaseSequence = 5;
            batch.Magic = 2; // Current supported
            batch.Records.resize(1);
            batch.Records[0].Key = TKafkaBytes{};
            batch.Records[0].Value = TKafkaBytes{};

            auto msg = client.Produce(topicName, 0, batch);

            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Name, topicName);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].PartitionResponses[0].Index, 0);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].PartitionResponses[0].ErrorCode,
                                     static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        }

        {
            // Check short topic name

            TKafkaRecordBatch batch;
            batch.BaseOffset = 7;
            batch.BaseSequence = 6;
            batch.Magic = 2; // Current supported
            batch.Records.resize(1);
            batch.Records[0].Key = "record-key-1";
            batch.Records[0].Value = "record-value-1";

            auto msg = client.Produce("topic-0-test", 0, batch);

            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Name, "topic-0-test");
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].PartitionResponses[0].Index, 0);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].PartitionResponses[0].ErrorCode,
                                     static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

            ReadCommitWaitAcks(topicReader, 1);
        }

        {
            // Check for few records

            TKafkaRecordBatch batch;
            batch.BaseOffset = 13;
            batch.BaseSequence = 7;
            batch.Magic = 2; // Current supported
            batch.Records.resize(1);
            batch.Records[0].Key = "record-key-0";
            batch.Records[0].Value = "record-value-0";

            std::vector<std::pair<ui32, TKafkaRecordBatch>> msgs;
            msgs.emplace_back(0, batch);
            batch.BaseSequence = 8;
            msgs.emplace_back(1, batch);

            auto msg = client.Produce("topic-0-test", msgs);

            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Name, "topic-0-test");
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].PartitionResponses[0].Index, 0);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].PartitionResponses[0].ErrorCode,
                                     static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].PartitionResponses[1].Index, 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].PartitionResponses[1].ErrorCode,
                                     static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

            ReadCommitWaitAcks(topicReader, 2);
        }

        {
            // Unknown topic

            TKafkaRecordBatch batch;
            batch.BaseOffset = 7;
            batch.BaseSequence = 9;
            batch.Magic = 2; // Current supported
            batch.Records.resize(1);
            batch.Records[0].Key = "record-key-1";
            batch.Records[0].Value = "record-value-1";

            auto msg = client.Produce("topic-0-test-not-exists", 0, batch);

            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Name, "topic-0-test-not-exists");
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].PartitionResponses[0].Index, 0);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].PartitionResponses[0].ErrorCode,
                                     static_cast<TKafkaInt16>(EKafkaErrors::UNKNOWN_TOPIC_OR_PARTITION));
        }

        {
            // Unknown partition

            TKafkaRecordBatch batch;
            batch.BaseOffset = 7;
            batch.BaseSequence = 10;
            batch.Magic = 2; // Current supported
            batch.Records.resize(1);
            batch.Records[0].Key = "record-key-1";
            batch.Records[0].Value = "record-value-1";

            auto msg = client.Produce("topic-0-test", 10000, batch);

            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Name, "topic-0-test");
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].PartitionResponses[0].Index, 10000);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].PartitionResponses[0].ErrorCode,
                                     static_cast<TKafkaInt16>(EKafkaErrors::UNKNOWN_TOPIC_OR_PARTITION));
        }

        {
            // Check unknown ApiKey (must be last. close the session)
            // expect no exception
            client.UnknownApiKey();
        }
    } // Y_UNIT_TEST(ProduceScenario)

    Y_UNIT_TEST(IdempotentProducerScenario) {
        using TProducerId = TKafkaRecordBatch::ProducerIdMeta::Type;
        using TProducerEpoch = TKafkaRecordBatch::ProducerEpochMeta::Type;
        using TBaseSequence = TKafkaRecordBatch::BaseSequenceMeta::Type;
        TInsecureTestServer testServer("2");
        testServer.KikimrServer->GetRuntime()->SetLogPriority(NKikimrServices::PQ_WRITE_PROXY, NActors::NLog::PRI_TRACE);
        testServer.KikimrServer->GetRuntime()->SetLogPriority(NKikimrServices::PERSQUEUE, NActors::NLog::PRI_TRACE);

        TString topicNamePrefix = "/Root/topic";

        TKafkaTestClient client(testServer.Port);

        auto createTopic = [&](const TString& topicName) -> TMessagePtr<TMetadataResponseData> {
            for (size_t i = 0; i < 10; ++i) {
                auto res = client.CreateTopics(std::vector<TTopicConfig>{TTopicConfig(topicName, 1)});
                if (res->Topics[0].ErrorCode == EKafkaErrors::NONE_ERROR) {
                    break;
                }
                Sleep(TDuration::Seconds(1));
            }
            for (size_t i = 0; i < 10; ++i) {
                auto res = client.Metadata({topicName}, false);
                if (res->Topics[0].ErrorCode == EKafkaErrors::NONE_ERROR) {
                    return res;
                }
                Sleep(TDuration::Seconds(1));
            }
            Y_ABORT_S("Could not create topic " << topicName);
        };

        auto withNewTopic = [&](std::function<void(TProducerId, TProducerEpoch, NKafka::TMetadataResponseData::TMetadataResponseTopic)> fn) {
            static ui64 index = 0;
            auto name = TStringBuilder() << topicNamePrefix << "-" << index++;
            Cerr << "XXXXX WithNewTopic " << name << Endl;
            auto res = createTopic(name);
            auto msg = client.InitProducerId();
            UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            fn(msg->ProducerId, msg->ProducerEpoch, res->Topics[0]);
        };

        TString recordKey = "record-key";
        TString recordValue = "record-value";
        TString headerKey = "header-key";
        TString headerValue = "header-value";

        struct TBatchParams {
            TBaseSequence BaseSequence = 0;
            ui64 RecordCount = 1;
        };
        auto makeBatch = [&](TProducerId id, TProducerEpoch epoch, TBatchParams batchParams) -> TKafkaRecordBatch {
            TKafkaRecordBatch batch;
            batch.ProducerId = id;
            batch.ProducerEpoch = epoch;
            batch.BaseSequence = batchParams.BaseSequence;
            batch.Magic = 2; // Current supported
            batch.Records.resize(batchParams.RecordCount);
            for (ui64 i = 0; i < batchParams.RecordCount; ++i) {
                batch.Records[i].Key = TKafkaRawBytes(recordKey.data(), recordKey.size());
                batch.Records[i].Value = TKafkaRawBytes(recordValue.data(), recordValue.size());
                batch.Records[i].Headers.resize(1);
                batch.Records[i].Headers[0].Key = TKafkaRawBytes(headerKey.data(), headerKey.size());
                batch.Records[i].Headers[0].Value = TKafkaRawBytes(headerValue.data(), headerValue.size());
            }
            return batch;
        };

        struct TExpectedResponse {
            using T = NKafka::TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse;
            TMaybe<T::BaseOffsetMeta::Type> BaseOffset = Nothing();
            TMaybe<EKafkaErrors> ErrorCode = Nothing();
        };
        auto checkResponse = [](TMessagePtr<TProduceResponseData> res, TExpectedResponse expected) {
            if (expected.BaseOffset) {
                UNIT_ASSERT_VALUES_EQUAL(res->Responses[0].PartitionResponses[0].BaseOffset, *expected.BaseOffset);
            } else {
                UNIT_ASSERT_GE(res->Responses[0].PartitionResponses[0].BaseOffset, 0);
            }
            if (expected.ErrorCode) {
                using TError = NKafka::TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::ErrorCodeMeta::Type;
                UNIT_ASSERT_VALUES_EQUAL(res->Responses[0].PartitionResponses[0].ErrorCode, static_cast<TError>(*expected.ErrorCode));
            }
        };
        auto listOffsets = [&](auto topic, i64 partition = 0) {
            std::vector<std::pair<i32, i64>> partitions{{{partition, -1}}};
            auto res = client.ListOffsets(partitions, topic);
            return res;
        };
        auto assertOffset = [&](TString topicName, i64 expectedOffset, i64 partition = 0) {
            auto offsets = listOffsets(topicName, partition);
            UNIT_ASSERT_VALUES_EQUAL(offsets->Topics[0].Partitions[0].Offset, expectedOffset);
        };

        withNewTopic([&](TProducerId id, TProducerEpoch epoch, NKafka::TMetadataResponseData::TMetadataResponseTopic topicMetadata) {
            // Write correct seqnos:

            auto topic = *topicMetadata.Name;

            auto res1 = client.Produce(topic, 0, makeBatch(id, epoch, { .BaseSequence = 0 }));
            checkResponse(res1, { .BaseOffset = 0, .ErrorCode = EKafkaErrors::NONE_ERROR });

            auto res2 = client.Produce(topic, 0, makeBatch(id, epoch, { .BaseSequence = 1 }));
            checkResponse(res2, { .BaseOffset = 1, .ErrorCode = EKafkaErrors::NONE_ERROR });

            auto res3 = client.Produce(topic, 0, makeBatch(id, epoch, { .BaseSequence = 2, .RecordCount = 3 }));
            checkResponse(res3, { .BaseOffset = 2, .ErrorCode = EKafkaErrors::NONE_ERROR });

            auto res4 = client.Produce(topic, 0, makeBatch(id, epoch, { .BaseSequence = 5 }));
            checkResponse(res4, { .BaseOffset = 5, .ErrorCode = EKafkaErrors::NONE_ERROR });

            assertOffset(topic, 6);
        });

        withNewTopic([&](TProducerId id, TProducerEpoch epoch, NKafka::TMetadataResponseData::TMetadataResponseTopic topicMetadata) {
            // TODO(qyryq) Kafka does not respond with DUPLICATE_SEQUENCE_NUMBER at all.
            // If you send several ProduceRequests to a partition, then resend any of the last 5 requests,
            // you'll just get the same response. But if you resend an older request, then you'll get an OUT_OF_ORDER_SEQUENCE_NUMBER error.
            // You will get the same error if you send any other seqnos, even if they cover the seqnos within the last 5 requests.
            // E.g after sending ProduceRequests with seqnos (3) (4) (5) (6 7 8) (9) (10),
            // if you repeat any of the (4) - (10) requests, the same response will be returned as the first one for that particular request.
            // Any other request will result in an OUT_OF_ORDER_SEQUENCE_NUMBER error: (1), (3), even (4 5) or (6 7).

            // Send the same message twice, it should be written only once:

            auto topic = *topicMetadata.Name;

            checkResponse(
                client.Produce(topic, 0, makeBatch(id, -1, { .BaseSequence = 0 })),
                { .BaseOffset = 0, .ErrorCode = EKafkaErrors::NONE_ERROR });

            // Kafka allows any seqno if the producer ID is unknown, so we can send seqno=5 here.

            checkResponse(
                client.Produce(topic, 0, makeBatch(id, epoch, { .BaseSequence = 5 })),
                { .BaseOffset = 1, .ErrorCode = EKafkaErrors::NONE_ERROR });

            assertOffset(topic, 2);

            // Duplicate message
            checkResponse(
                client.Produce(topic, 0, makeBatch(id, epoch, { .BaseSequence = 5 })),
                { .BaseOffset = 1, .ErrorCode = EKafkaErrors::NONE_ERROR });

            checkResponse(
                client.Produce(topic, 0, makeBatch(id, epoch, { .BaseSequence = 6 })),
                { .BaseOffset = 2, .ErrorCode = EKafkaErrors::NONE_ERROR });

            checkResponse(
                client.Produce(topic, 0, makeBatch(id, epoch, { .BaseSequence = 7 })),
                { .BaseOffset = 3, .ErrorCode = EKafkaErrors::NONE_ERROR });

            assertOffset(topic, 4);

            // We should return OUT_OF_ORDER_SEQUENCE_NUMBER, but it will be done later.
            checkResponse(
                client.Produce(topic, 0, makeBatch(id, epoch, { .BaseSequence = 4 })),
                { .BaseOffset = 0, .ErrorCode = EKafkaErrors::NONE_ERROR });

            // We simply guess the base offset, as we store in memory only offsets of the last message.
            checkResponse(
                client.Produce(topic, 0, makeBatch(id, epoch, { .BaseSequence = 5 })),
                { .BaseOffset = 1, .ErrorCode = EKafkaErrors::NONE_ERROR });

            // This is an incorrect request (we didn't send seqno=1). We try to guess the offset of the message
            // with seqno=1 (maxOffset - (maxSeqNo - seqno)), but if the result is negative, we return 0.
            checkResponse(
                client.Produce(topic, 0, makeBatch(id, epoch, { .BaseSequence = 1 })),
                { .BaseOffset = 0, .ErrorCode = EKafkaErrors::NONE_ERROR });

            assertOffset(topic, 4);
        });

        withNewTopic([&](TProducerId id, TProducerEpoch epoch, NKafka::TMetadataResponseData::TMetadataResponseTopic topicMetadata) {
            // Write a message with seqno greater than expected:
            auto topic = *topicMetadata.Name;
            auto res1 = client.Produce(topic, 0, makeBatch(id, epoch, { .BaseSequence = 5 }));
            auto res2 = client.Produce(topic, 0, makeBatch(id, epoch, { .BaseSequence = 7 }));
            checkResponse(res2, {
                .BaseOffset = 0,
                .ErrorCode = EKafkaErrors::OUT_OF_ORDER_SEQUENCE_NUMBER,
            });
            assertOffset(topic, 1);
        });

        withNewTopic([&](TProducerId id, TProducerEpoch epoch, NKafka::TMetadataResponseData::TMetadataResponseTopic topicMetadata) {
            // Write a message with seqno = max<int32>. The next seqno should be 0 and we should accept it.
            auto topic = *topicMetadata.Name;
            auto res1 = client.Produce(topic, 0, makeBatch(id, epoch, { .BaseSequence = std::numeric_limits<int32_t>::max() }));
            checkResponse(res1, { .BaseOffset = 0, .ErrorCode = EKafkaErrors::NONE_ERROR });
            assertOffset(topic, 1);

            auto res2 = client.Produce(topic, 0, makeBatch(id, epoch, { .BaseSequence = 0 }));
            checkResponse(res2, { .BaseOffset = 1, .ErrorCode = EKafkaErrors::NONE_ERROR });
            assertOffset(topic, 2);
        });

        withNewTopic([&](TProducerId id, TProducerEpoch epoch, NKafka::TMetadataResponseData::TMetadataResponseTopic topicMetadata) {
            // Write a batch of messages with seqnos (max<int32> - 1, max<int32>, 0, 1).
            auto topic = *topicMetadata.Name;
            auto res1 = client.Produce(topic, 0, makeBatch(id, epoch, { .BaseSequence = std::numeric_limits<int32_t>::max() - 1, .RecordCount = 4 }));
            checkResponse(res1, { .BaseOffset = 0, .ErrorCode = EKafkaErrors::NONE_ERROR });
            assertOffset(topic, 4);
        });

        withNewTopic([&](TProducerId id, TProducerEpoch epoch, NKafka::TMetadataResponseData::TMetadataResponseTopic topicMetadata) {
            // Write a batch with seqnos (max<int32> - 1, max<int32>), then write a batch of messages with seqnos (0, 1).
            auto topic = *topicMetadata.Name;

            auto res1 = client.Produce(topic, 0, makeBatch(id, epoch, { .BaseSequence = std::numeric_limits<int32_t>::max() - 1, .RecordCount = 2 }));
            checkResponse(res1, { .BaseOffset = 0, .ErrorCode = EKafkaErrors::NONE_ERROR });

            auto res2 = client.Produce(topic, 0, makeBatch(id, epoch, { .BaseSequence = 0, .RecordCount = 2 }));
            checkResponse(res2, { .BaseOffset = 2, .ErrorCode = EKafkaErrors::NONE_ERROR });

            assertOffset(topic, 4);
        });

        withNewTopic([&](TProducerId id, TProducerEpoch epoch, NKafka::TMetadataResponseData::TMetadataResponseTopic topicMetadata) {
            // Write seqno=max<int32>, then seqno=1. Expect OUT_OF_ORDER_SEQUENCE_NUMBER.
            // Then write seqno=max<int32> / 2 + 2. Expect "DUPLICATE_SEQUENCE_NUMBER".

            auto topic = *topicMetadata.Name;
            auto res1 = client.Produce(topic, 0, makeBatch(id, epoch, { .BaseSequence = std::numeric_limits<TBaseSequence>::max() }));
            checkResponse(res1, { .BaseOffset = 0, .ErrorCode = EKafkaErrors::NONE_ERROR });

            auto res2 = client.Produce(topic, 0, makeBatch(id, epoch, { .BaseSequence = 1 }));
            checkResponse(res2, { .ErrorCode = EKafkaErrors::OUT_OF_ORDER_SEQUENCE_NUMBER });

            auto res3 = client.Produce(topic, 0, makeBatch(id, epoch, { .BaseSequence = std::numeric_limits<TBaseSequence>::max() / 2 + 2 }));
            checkResponse(res3, { .BaseOffset = 0, .ErrorCode = EKafkaErrors::NONE_ERROR });

            assertOffset(topic, 1);
        });

        withNewTopic([&](TProducerId id, TProducerEpoch epoch, NKafka::TMetadataResponseData::TMetadataResponseTopic topicMetadata) {
            // Send two overlapping batches: seqnos 1, 2, 3, then seqnos 2, 3, 4.
            // TODO(qyryq) Kafka doesn't accept the second batch at all, but we accept seqno = 4.

            auto topic = *topicMetadata.Name;

            checkResponse(
                client.Produce(topic, 0, makeBatch(id, epoch, { .BaseSequence = 1, .RecordCount = 3 })),
                { .BaseOffset = 0, .ErrorCode = EKafkaErrors::NONE_ERROR });

            checkResponse(
                client.Produce(topic, 0, makeBatch(id, epoch, { .BaseSequence = 2, .RecordCount = 3 })),
                { .BaseOffset = 1, .ErrorCode = EKafkaErrors::NONE_ERROR });

            assertOffset(topic, 4);
        });

        withNewTopic([&](TProducerId id, TProducerEpoch epoch, NKafka::TMetadataResponseData::TMetadataResponseTopic topicMetadata) {
            // Write messages in different epochs.

            auto topic = *topicMetadata.Name;

            checkResponse(
                client.Produce(topic, 0, makeBatch(id, epoch, { .BaseSequence = 3, .RecordCount = 5 })),
                { .BaseOffset = 0, .ErrorCode = EKafkaErrors::NONE_ERROR });
            assertOffset(topic, 5);

            checkResponse(
                client.Produce(topic, 0, makeBatch(id, epoch + 1, { .BaseSequence = 0, .RecordCount = 2 })),
                { .BaseOffset = 5, .ErrorCode = EKafkaErrors::NONE_ERROR });
            assertOffset(topic, 7);

            checkResponse(
                client.Produce(topic, 0, makeBatch(id, epoch, { .BaseSequence = 8 })),
                { .ErrorCode = EKafkaErrors::INVALID_PRODUCER_EPOCH });
            assertOffset(topic, 7);

            checkResponse(
                client.Produce(topic, 0, makeBatch(id, epoch + 1, { .BaseSequence = 2 })),
                { .BaseOffset = 7, .ErrorCode = EKafkaErrors::NONE_ERROR });
            assertOffset(topic, 8);
        });

        withNewTopic([&](TProducerId id, TProducerEpoch epoch, NKafka::TMetadataResponseData::TMetadataResponseTopic topicMetadata) {
            // Write a message with an invalid epoch, an old producer should be fenced.

            auto topic = *topicMetadata.Name;

            auto res1 = client.Produce(topic, 0, makeBatch(id, epoch, { .BaseSequence = 0 }));
            checkResponse(res1, { .BaseOffset = 0, .ErrorCode = EKafkaErrors::NONE_ERROR });
            assertOffset(topic, 1);

            auto res2 = client.Produce(topic, 0, makeBatch(id, epoch + 1, { .BaseSequence = 0 }));
            checkResponse(res2, { .BaseOffset = 1, .ErrorCode = EKafkaErrors::NONE_ERROR });
            assertOffset(topic, 2);

            auto res3 = client.Produce(topic, 0, makeBatch(id, epoch, { .BaseSequence = 1 }));
            checkResponse(res3, { .ErrorCode = EKafkaErrors::INVALID_PRODUCER_EPOCH });
            assertOffset(topic, 2);

            auto res4 = client.Produce(topic, 0, makeBatch(id, epoch + 1, { .BaseSequence = 1 }));
            checkResponse(res4, { .BaseOffset = 2, .ErrorCode = EKafkaErrors::NONE_ERROR });
            assertOffset(topic, 3);
        });

        withNewTopic([&](TProducerId id, TProducerEpoch epoch, NKafka::TMetadataResponseData::TMetadataResponseTopic topicMetadata) {
            // Write a message with unknown producer ID: any epoch + seqno pair is allowed.

            auto topic = *topicMetadata.Name;

            auto res1 = client.Produce(topic, 0, makeBatch(id + 1, epoch + 1, { .BaseSequence = 10 }));
            checkResponse(res1, { .BaseOffset = 0, .ErrorCode = EKafkaErrors::NONE_ERROR });

            assertOffset(topic, 1);
        });

        withNewTopic([&](TProducerId id, TProducerEpoch epoch, NKafka::TMetadataResponseData::TMetadataResponseTopic topicMetadata) {
             // Write a message with known producer ID + new epoch: only newEpoch + 0 pair is allowed.

            auto topic = *topicMetadata.Name;

            // Write a message with some seqno.
            auto res1 = client.Produce(topic, 0, makeBatch(id, epoch, { .BaseSequence = 10 }));
            checkResponse(res1, { .BaseOffset = 0, .ErrorCode = EKafkaErrors::NONE_ERROR });

            // Bump the epoch, write a message with non-zero seqno.
            auto res2 = client.Produce(topic, 0, makeBatch(id, epoch + 1, { .BaseSequence = 11 }));
            checkResponse(res2, { .BaseOffset = 0, .ErrorCode = EKafkaErrors::OUT_OF_ORDER_SEQUENCE_NUMBER });

            assertOffset(topic, 1);

            auto res3 = client.Produce(topic, 0, makeBatch(id, epoch + 1, { .BaseSequence = 0 }));
            checkResponse(res3, { .BaseOffset = 1, .ErrorCode = EKafkaErrors::NONE_ERROR });

            assertOffset(topic, 2);
        });

        withNewTopic([&](TProducerId id, TProducerEpoch epoch, NKafka::TMetadataResponseData::TMetadataResponseTopic topicMetadata) {
            // 1. Write messages with producer epoch = -1. Any seqnos are allowed in any order.
            // 2. Then write a message with proper epoch.
            // 3. Then check that epoch -1 is still allowed (NOTE: non-conforming behavior, Kafka does not allow this).

            auto topic = *topicMetadata.Name;

            checkResponse(
                client.Produce(topic, 0, makeBatch(id, -1, { .BaseSequence = 10 })),
                { .BaseOffset = 0, .ErrorCode = EKafkaErrors::NONE_ERROR });

            checkResponse(
                client.Produce(topic, 0, makeBatch(id, -1, { .BaseSequence = 5 })),
                { .BaseOffset = 1, .ErrorCode = EKafkaErrors::NONE_ERROR });

            assertOffset(topic, 2);

            checkResponse(
                client.Produce(topic, 0, makeBatch(id, epoch, { .BaseSequence = 3 })),
                { .BaseOffset = 2, .ErrorCode = EKafkaErrors::NONE_ERROR });

            assertOffset(topic, 3);

            // Non-conforming behavior, Kafka does not accept the next message with producer epoch = -1.
            checkResponse(
                client.Produce(topic, 0, makeBatch(id, -1, { .BaseSequence = 4 })),
                { .BaseOffset = 3, .ErrorCode = EKafkaErrors::NONE_ERROR });

            assertOffset(topic, 4);
        });

        // TODO(qyryq) The same tests but with the tablet restarting in between the producer requests.

        withNewTopic([&](TProducerId id, TProducerEpoch epoch, NKafka::TMetadataResponseData::TMetadataResponseTopic topicMetadata) {
            // Send a message, kill the tablet, send the same message, it should be written only once:

            auto topic = *topicMetadata.Name;

            auto batch1 = makeBatch(id, epoch, { .BaseSequence = 5 });
            auto res1 = client.Produce(topic, 0, batch1);
            checkResponse(res1, {
                .BaseOffset = 0,
                .ErrorCode = EKafkaErrors::NONE_ERROR,
            });

            // Kill topic tablet:
            NKikimr::NFlatTests::TFlatMsgBusClient kikimrClient(*(testServer.KikimrServer->ServerSettings));
            auto pathDescr = kikimrClient.Ls(topic)->Record.GetPathDescription().GetPersQueueGroup();
            auto tabletId = pathDescr.GetPartitions(0).GetTabletId();
            kikimrClient.KillTablet(testServer.KikimrServer->GetServer(), tabletId);

            while (true) {
                auto res2 = client.Produce(topic, 0, batch1);  // Duplicate message
                if (res2->Responses[0].PartitionResponses[0].ErrorCode != EKafkaErrors::NOT_LEADER_OR_FOLLOWER) {
                    checkResponse(res2, { .BaseOffset = 0, .ErrorCode = EKafkaErrors::NONE_ERROR });
                    break;
                }
            }

            assertOffset(topic, 1);
        });

    } // Y_UNIT_TEST(IdempotentProducerScenario)

    Y_UNIT_TEST(FetchScenario) {
        TInsecureTestServer testServer("2");

        TString topicName = "/Root/topic-0-test";
        TString shortTopicName = "topic-0-test";
        TString notExistsTopicName = "/Root/not-exists";
        TString consumer1 = "consumer1";

        TString tableName = "/Root/table-0-test";
        TString feedName = "feed";
        TString feedPath = tableName + "/" + feedName;

        ui64 minActivePartitions = 10;

        TString key = "record-key";
        TString value = "record-value";
        TString headerKey = "header-key";
        TString headerValue = "header-value";

        NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
        CreateTopic(pqClient, topicName, minActivePartitions, {});

        TKafkaTestClient client(testServer.Port);

        client.PlainAuthenticateToKafka();

        {
            // Check list offsets for empty topic
            std::vector<std::pair<i32,i64>> partitions {{0, FirstTopicOffset}, {0, LastTopicOffset}};
            auto msg = client.ListOffsets(partitions, topicName);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].Partitions.size(), 2);

            for (auto& topic: msg->Topics) {
                for (auto& partition: topic.Partitions) {
                    UNIT_ASSERT_VALUES_EQUAL(partition.ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
                    UNIT_ASSERT_VALUES_EQUAL(partition.Offset, 0);
                }
            }
        }

        {
            // Check empty topic (no records)
            std::vector<std::pair<TString, std::vector<i32>>> topics {{topicName, {0}}};
            auto msg = client.Fetch(topics);

            UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Partitions.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Partitions[0].Records.has_value(), false);
        }

        {
            auto msg = client.InitProducerId();
            UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        }

        {
            // Produce
            TKafkaRecordBatch batch;
            batch.BaseOffset = 3;
            batch.BaseSequence = 5;
            batch.Magic = 2; // Current supported
            batch.Records.resize(1);
            batch.Records[0].Key = TKafkaRawBytes(key.data(), key.size());
            batch.Records[0].Value = TKafkaRawBytes(value.data(), value.size());
            batch.Records[0].Headers.resize(1);
            batch.Records[0].Headers[0].Key = TKafkaRawBytes(headerKey.data(), headerKey.size());
            batch.Records[0].Headers[0].Value = TKafkaRawBytes(headerValue.data(), headerValue.size());

            auto msg = client.Produce(topicName, 0, batch);

            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Name, topicName);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].PartitionResponses[0].Index, 0);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].PartitionResponses[0].ErrorCode,
                                        static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        }

        {
            // Check list offsets after produce
            std::vector<std::pair<i32,i64>> partitions {{0, FirstTopicOffset}, {0, LastTopicOffset}};
            auto msg = client.ListOffsets(partitions, topicName);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].Partitions.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].Partitions[0].Offset, 0);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].Partitions[1].Offset, 1);
        }

        {
            // Check list offsets short topic name
            std::vector<std::pair<i32,i64>> partitions {{0, FirstTopicOffset}, {0, LastTopicOffset}};
            auto msg = client.ListOffsets(partitions, shortTopicName);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].Partitions.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].Partitions[0].Offset, 0);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].Partitions[1].Offset, 1);
        }

        {
            // Check FETCH
            std::vector<std::pair<TString, std::vector<i32>>> topics {{topicName, {0}}};
            auto msg = client.Fetch(topics);
            UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Partitions.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Partitions[0].Records.has_value(), true);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Partitions[0].Records->Records.size(), 1);
            auto record = msg->Responses[0].Partitions[0].Records->Records[0];

            auto data = record.Value.value();
            auto dataStr = TString(data.data(), data.size());
            UNIT_ASSERT_VALUES_EQUAL(dataStr, value);
            auto key = record.Key.value();
            auto keyStr = TString(key.data(), key.size());
            UNIT_ASSERT_VALUES_EQUAL(keyStr, key);

            auto headerKey = record.Headers[0].Key.value();
            auto headerKeyStr = TString(headerKey.data(), headerKey.size());
            UNIT_ASSERT_VALUES_EQUAL(dataStr, value);

            auto headerValue = record.Headers[0].Value.value();
            auto headerValueStr = TString(headerValue.data(), headerValue.size());
            UNIT_ASSERT_VALUES_EQUAL(dataStr, value);
        }

        {
            // Check big offset
            std::vector<std::pair<TString, std::vector<i32>>> topics {{topicName, {0}}};
            auto msg = client.Fetch(topics, std::numeric_limits<i64>::max());
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Partitions.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Partitions[0].ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::OFFSET_OUT_OF_RANGE));
        }

        {
            // Check short topic name
            std::vector<std::pair<TString, std::vector<i32>>> topics {{shortTopicName, {0}}};
            auto msg = client.Fetch(topics);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Partitions.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Partitions[0].ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        }

        {
            // Check not exists topics and partition
            std::vector<std::pair<TString, std::vector<i32>>> topics {
                {notExistsTopicName, {0}},
                {"", {0}},
                {topicName, {5000}},
                {topicName, {-1}}
                };
            auto msg = client.Fetch(topics);
            UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses.size(), topics.size());
            for (size_t i = 0; i < topics.size(); i++) {
                UNIT_ASSERT_VALUES_EQUAL(msg->Responses[i].Partitions.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Partitions[0].ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::UNKNOWN_TOPIC_OR_PARTITION));
            }
        }

        //broken
        // {
        //     // Check partition double
        //     std::vector<std::pair<TString, std::vector<i32>>> topics {{topicName, {0,0}}};
        //     auto msg = client.Fetch(topics);
        //     UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        //     UNIT_ASSERT_VALUES_EQUAL(msg->Responses.size(), 1);
        //     UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Partitions.size(), 2);

        //     for (size_t i = 0; i < 2; i++) {
        //         auto record = msg->Responses[0].Partitions[i].Records->Records[0];

        //         auto data = record.Value.value();
        //         auto dataStr = TString(data.data(), data.size());
        //         UNIT_ASSERT_VALUES_EQUAL(dataStr, value);
        //     }
        // }

        {
            // Check topic double
            std::vector<std::pair<TString, std::vector<i32>>> topics {{topicName, {0}},{topicName, {0}}};
            auto msg = client.Fetch(topics);
            UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses.size(), 2);

            for (size_t i = 0; i < 2; i++) {
                UNIT_ASSERT_VALUES_EQUAL(msg->Responses[i].Partitions.size(), 1);
                auto record = msg->Responses[i].Partitions[0].Records->Records[0];

                auto data = record.Value.value();
                auto dataStr = TString(data.data(), data.size());
                UNIT_ASSERT_VALUES_EQUAL(dataStr, value);
            }
        }

        // create table and init cdc for it
        {
            NYdb::NTable::TTableClient tableClient(*testServer.Driver);
            tableClient.RetryOperationSync([&](TSession session)
                {
                    NYdb::NTable::TTableBuilder builder;
                    builder.AddNonNullableColumn("key", NYdb::EPrimitiveType::Int64).SetPrimaryKeyColumn("key");
                    builder.AddNonNullableColumn("value", NYdb::EPrimitiveType::Int64);

                    auto createResult = session.CreateTable(tableName, builder.Build()).ExtractValueSync();
                    UNIT_ASSERT_VALUES_EQUAL(createResult.IsTransportError(), false);
                    Cerr << createResult.GetIssues().ToString() << "\n";
                    UNIT_ASSERT_VALUES_EQUAL(createResult.GetStatus(), EStatus::SUCCESS);

                    auto alterResult = session.AlterTable(tableName, NYdb::NTable::TAlterTableSettings()
                                    .AppendAddChangefeeds(NYdb::NTable::TChangefeedDescription(feedName,
                                                                                            NYdb::NTable::EChangefeedMode::Updates,
                                                                                            NYdb::NTable::EChangefeedFormat::Json))
                                                        ).ExtractValueSync();
                    Cerr << alterResult.GetIssues().ToString() << "\n";
                    UNIT_ASSERT_VALUES_EQUAL(alterResult.IsTransportError(), false);
                    UNIT_ASSERT_VALUES_EQUAL(alterResult.GetStatus(), EStatus::SUCCESS);
                    return alterResult;
                }
            );

            TValueBuilder rows;
            rows.BeginList();
            rows.AddListItem()
                .BeginStruct()
                    .AddMember("key").Int64(1)
                    .AddMember("value").Int64(2)
                .EndStruct();
            rows.EndList();

            auto upsertResult = tableClient.BulkUpsert(tableName, rows.Build()).GetValueSync();
            UNIT_ASSERT_EQUAL(upsertResult.GetStatus(), EStatus::SUCCESS);
        }

        for (size_t i = 10; i--;){
            // Check CDC
            std::vector<std::pair<TString, std::vector<i32>>> topics {{feedPath, {0}}};
            auto msg = client.Fetch(topics);

            if (msg->Responses.empty() || msg->Responses[0].Partitions.empty() || !msg->Responses[0].Partitions[0].Records.has_value()) {
                UNIT_ASSERT_C(i, "Timeout");
                Sleep(TDuration::Seconds(1));
                continue;
            }

            UNIT_ASSERT_VALUES_EQUAL(msg->Responses.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Partitions.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Partitions[0].ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Partitions[0].Records.has_value(), true);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Partitions[0].Records->Records.size(), 1);
            auto record = msg->Responses[0].Partitions[0].Records->Records[0];

            auto data = record.Value.value();
            auto dataStr = TString(data.data(), data.size());
            UNIT_ASSERT_VALUES_EQUAL(dataStr, "{\"update\":{\"value\":2},\"key\":[1]}");

            break;
        }
    } // Y_UNIT_TEST(FetchScenario)

    Y_UNIT_TEST(FetchScenarioWithJoinGroup) {
        TInsecureTestServer testServer("2");

        TString protocolName = "roundrobin";

        TString topicName = "/Root/topic-0-test";
        TString shortTopicName = "topic-0-test";
        TString group = "group-0-test";

        TString key = "";
        TString value = "value";

        ui64 minActivePartitions = 10;

        NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
        CreateTopic(pqClient, topicName, minActivePartitions, { group });

        TKafkaTestClient client(testServer.Port);

        client.PlainAuthenticateToKafka();

        {
            auto msg = client.InitProducerId();
            UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        }

        {
            // Produce
            TKafkaRecordBatch batch;
            batch.BaseOffset = 3;
            batch.BaseSequence = 5;
            batch.Magic = 2; // Current supported
            batch.Records.resize(1);
            batch.Records[0].Key = TKafkaRawBytes(key.data(), key.size());
            batch.Records[0].Value = TKafkaRawBytes(value.data(), value.size());

            auto msg = client.Produce(topicName, 0, batch);

            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Name, topicName);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].PartitionResponses[0].Index, 0);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].PartitionResponses[0].ErrorCode,
                                        static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        }

        {
            std::vector<TString> topics = { topicName };
            auto msg = client.JoinGroup(topics, group, protocolName, 10000);
            UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        }

        {
            // Check FETCH
            std::vector<std::pair<TString, std::vector<i32>>> topics {{topicName, {0}}};
            auto msg = client.Fetch(topics);
            UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Partitions.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Partitions[0].Records.has_value(), true);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Partitions[0].Records->Records.size(), 1);
        }
    } // Y_UNIT_TEST(FetchScenarioWithJoinGroup)

    Y_UNIT_TEST(FetchEmptyTopicScenario) {
        TInsecureTestServer testServer("FetchEmptyTopicScenario");

        TString protocolName = "roundrobin";

        TString topicName = "/Root/topic-0-test";
        TString group = "group-0-test";


          NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
        CreateTopic(pqClient, topicName, 1, { group });

        TKafkaTestClient client(testServer.Port);

        client.PlainAuthenticateToKafka();

        {
            // Check FETCH
            std::vector<std::pair<TString, std::vector<i32>>> topics {{topicName, {0}}};
            auto msg = client.Fetch(topics);
            UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Partitions.size(), 1);
            // To protect the clients from failing due to null records,
            // Java SDK always convert null records to MemoryRecords.EMPTY
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Partitions[0].Records.has_value(), false);
        }
    } // Y_UNIT_TEST(FetchEmptyTopicScenario)

    Y_UNIT_TEST(SwitchToServerBalancingScenario) {
        TInsecureTestServer testServer("SwitchToServerBalancingScenario");

        TString topicName = "/Root/topic-0-test";
        TString shortTopicName = "topic-0-test";
        TString group = "group-0-test";

        TString key = "";
        TString value = "value";

        ui64 minActivePartitions = 10;

        NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
        CreateTopic(pqClient, topicName, minActivePartitions, { group });

        TKafkaTestClient client0(testServer.Port);
        TKafkaTestClient client1(testServer.Port);

        client0.PlainAuthenticateToKafka();
        client1.PlainAuthenticateToKafka();

        TString memberId0;
        i32 generationId0;
        {
            std::vector<TString> topics = { topicName };
            auto msg = client0.JoinGroup(topics, group, "roundrobin", 5000);
            UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            memberId0 = *msg->MemberId;
            generationId0 = msg->GenerationId;
        }

        //TString memberId1;
        //i32 generationId1;
        {
            std::vector<TString> topics = { topicName };
            auto msg = client1.JoinGroup(topics, group, "server", 5000);
            UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

           // memberId1 = *msg->MemberId;
            //generationId1 = msg->GenerationId;
        }

        Sleep(TDuration::MilliSeconds(100));

        {
            // Check FETCH
            std::vector<std::pair<TString, std::vector<i32>>> topics {{topicName, {0}}};
            auto msg = client0.Fetch(topics);
            UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR)); // From version 7
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Partitions.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Partitions[0].ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::REBALANCE_IN_PROGRESS));
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Partitions[0].Records.has_value(), false);
        }

        {
            // Double check FETCH
            std::vector<std::pair<TString, std::vector<i32>>> topics {{topicName, {0}}};
            auto msg = client0.Fetch(topics);
            UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR)); // From version 7
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Partitions.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Partitions[0].ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::REBALANCE_IN_PROGRESS));
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Partitions[0].Records.has_value(), false);
        }

        {
            auto msg = client0.Heartbeat(memberId0, generationId0, group);
            UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::REBALANCE_IN_PROGRESS));
        }

        {
            TRequestHeaderData syncHeader = client0.Header(NKafka::EApiKey::SYNC_GROUP, 5);

            TSyncGroupRequestData syncReq;
            syncReq.GroupId = group;
            syncReq.ProtocolType = "consumer";
            syncReq.ProtocolName = "roundrobin";
            syncReq.GenerationId = generationId0;
            syncReq.MemberId = memberId0;

            client0.WriteToSocket(syncHeader, syncReq);
            auto msg = client0.ReadResponse<TSyncGroupResponseData>(syncHeader);

            UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::ILLEGAL_GENERATION));
        }

    } // Y_UNIT_TEST(SwitchToServerBalancingScenario)

    void RunBalanceScenarionTest(bool forFederation) {
        TString protocolName = "roundrobin";
        TInsecureTestServer testServer("2", false, false);

        TString topicName = "/Root/topic-0-test";
        TString shortTopicName = "topic-0-test";

        TString secondTopicName = "/Root/topic-1-test";

        TString notExistsTopicName = "/Root/not-exists";

        ui64 minActivePartitions = 12;

        TString group = "consumer-0";
        TString notExistsGroup = "consumer-not-exists";

        NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
        CreateTopic(pqClient, topicName, minActivePartitions, {group});
        CreateTopic(pqClient, secondTopicName, minActivePartitions, {group});

        if (forFederation) {
            testServer.KikimrServer->GetServer().GetRuntime()->GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(false);
        }
        TKafkaTestClient clientA(testServer.Port);
        TKafkaTestClient clientB(testServer.Port);
        TKafkaTestClient clientC(testServer.Port);
        TKafkaTestClient clientD(testServer.Port);

        {
            clientA.PlainAuthenticateToKafka("ouruser@/Root", "ourUserPassword");
        }

        {
            // Check partitions balance
            std::vector<TString> topics;
            topics.push_back(topicName);

            // clientA join group, and get all partitions
            auto readInfoA = clientA.JoinAndSyncGroupAndWaitPartitions(topics, group, minActivePartitions, protocolName, minActivePartitions);
            UNIT_ASSERT_VALUES_EQUAL(clientA.Heartbeat(readInfoA.MemberId, readInfoA.GenerationId, group)->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT_VALUES_EQUAL(readInfoA.Partitions[0].Topic, topicName);

            // clientB join group, and get 0 partitions, becouse it's all at clientA
            UNIT_ASSERT_VALUES_EQUAL(clientB.SaslHandshake()->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT_VALUES_EQUAL(clientB.SaslPlainAuthenticate("ouruser@/Root", "ourUserPassword")->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            auto readInfoB = clientB.JoinAndSyncGroup(topics, group, protocolName, 1000000, minActivePartitions);
            UNIT_ASSERT_VALUES_EQUAL(readInfoB.Partitions.size(), 0);

            // clientA gets RABALANCE status, because of new reader. We need to release some partitions for new client
            clientA.WaitRebalance(readInfoA.MemberId, readInfoA.GenerationId, group);

            // clientA now gets half of partitions
            readInfoA = clientA.JoinAndSyncGroupAndWaitPartitions(topics, group, minActivePartitions/2, protocolName, minActivePartitions);
            UNIT_ASSERT_VALUES_EQUAL(clientA.Heartbeat(readInfoA.MemberId, readInfoA.GenerationId, group)->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

            // some partitions now released, and we can give them to clientB. clientB now gets half of partitions
            clientB.WaitRebalance(readInfoB.MemberId, readInfoB.GenerationId, group);
            readInfoB = clientB.JoinAndSyncGroupAndWaitPartitions(topics, group, minActivePartitions/2, protocolName, minActivePartitions);
            UNIT_ASSERT_VALUES_EQUAL(clientB.Heartbeat(readInfoB.MemberId, readInfoB.GenerationId, group)->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

            AssertPartitionsIsUniqueAndCountIsExpected({readInfoA, readInfoB}, minActivePartitions, topicName);

            // clientC join group, and get 0 partitions, becouse it's all at clientA and clientB
            UNIT_ASSERT_VALUES_EQUAL(clientC.SaslHandshake()->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT_VALUES_EQUAL(clientC.SaslPlainAuthenticate("ouruser@/Root", "ourUserPassword")->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            auto readInfoC = clientC.JoinAndSyncGroup(topics, group, protocolName, 1000000, minActivePartitions);
            UNIT_ASSERT_VALUES_EQUAL(readInfoC.Partitions.size(), 0);

            // all clients gets RABALANCE status, because of new reader. We need to release some partitions for new client
            clientA.WaitRebalance(readInfoA.MemberId, readInfoA.GenerationId, group);
            clientB.WaitRebalance(readInfoB.MemberId, readInfoB.GenerationId, group);

            // all clients now gets minActivePartitions/3 partitions
            readInfoA = clientA.JoinAndSyncGroupAndWaitPartitions(topics, group, minActivePartitions/3, protocolName, minActivePartitions);
            UNIT_ASSERT_VALUES_EQUAL(clientA.Heartbeat(readInfoA.MemberId, readInfoA.GenerationId, group)->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

            readInfoB = clientB.JoinAndSyncGroupAndWaitPartitions(topics, group, minActivePartitions/3, protocolName, minActivePartitions);
            UNIT_ASSERT_VALUES_EQUAL(clientB.Heartbeat(readInfoB.MemberId, readInfoB.GenerationId, group)->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

            readInfoC = clientC.JoinAndSyncGroupAndWaitPartitions(topics, group, minActivePartitions/3, protocolName, minActivePartitions);
            UNIT_ASSERT_VALUES_EQUAL(clientC.Heartbeat(readInfoC.MemberId, readInfoC.GenerationId, group)->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

            AssertPartitionsIsUniqueAndCountIsExpected({readInfoA, readInfoB, readInfoC}, minActivePartitions, topicName);

            // clientD join group, and get 0 partitions, becouse it's all at clientA, clientB and clientC
            UNIT_ASSERT_VALUES_EQUAL(clientD.SaslHandshake()->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT_VALUES_EQUAL(clientD.SaslPlainAuthenticate("ouruser@/Root", "ourUserPassword")->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            auto readInfoD = clientD.JoinAndSyncGroup(topics, group, protocolName, 1000000, minActivePartitions);
            UNIT_ASSERT_VALUES_EQUAL(readInfoD.Partitions.size(), 0);

            // all clients gets RABALANCE status, because of new reader. We need to release some partitions
            clientA.WaitRebalance(readInfoA.MemberId, readInfoA.GenerationId, group);
            clientB.WaitRebalance(readInfoB.MemberId, readInfoB.GenerationId, group);
            clientC.WaitRebalance(readInfoC.MemberId, readInfoC.GenerationId, group);

            // all clients now gets minActivePartitions/4 partitions
            readInfoA = clientA.JoinAndSyncGroupAndWaitPartitions(topics, group, minActivePartitions/4, protocolName);
            UNIT_ASSERT_VALUES_EQUAL(clientA.Heartbeat(readInfoA.MemberId, readInfoA.GenerationId, group)->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

            readInfoB = clientB.JoinAndSyncGroupAndWaitPartitions(topics, group, minActivePartitions/4, protocolName, minActivePartitions);
            UNIT_ASSERT_VALUES_EQUAL(clientB.Heartbeat(readInfoB.MemberId, readInfoB.GenerationId, group)->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

            readInfoC = clientC.JoinAndSyncGroupAndWaitPartitions(topics, group, minActivePartitions/4, protocolName, minActivePartitions);
            UNIT_ASSERT_VALUES_EQUAL(clientC.Heartbeat(readInfoC.MemberId, readInfoC.GenerationId, group)->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

            readInfoD = clientD.JoinAndSyncGroupAndWaitPartitions(topics, group, minActivePartitions/4, protocolName, minActivePartitions);
            UNIT_ASSERT_VALUES_EQUAL(clientD.Heartbeat(readInfoD.MemberId, readInfoD.GenerationId, group)->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

            AssertPartitionsIsUniqueAndCountIsExpected({readInfoA, readInfoB, readInfoC, readInfoD}, minActivePartitions, topicName);


            // cleintA leave group and all partitions goes to clientB, clientB and clientD
            UNIT_ASSERT_VALUES_EQUAL(clientA.LeaveGroup(readInfoA.MemberId, group)->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

            // all other clients gets RABALANCE status, because one clientA leave group.
            clientB.WaitRebalance(readInfoB.MemberId, readInfoB.GenerationId, group);
            clientC.WaitRebalance(readInfoC.MemberId, readInfoC.GenerationId, group);
            clientD.WaitRebalance(readInfoD.MemberId, readInfoD.GenerationId, group);

            // all other clients now gets minActivePartitions/3 partitions
            readInfoB = clientB.JoinAndSyncGroupAndWaitPartitions(topics, group, minActivePartitions/3, protocolName, minActivePartitions);
            UNIT_ASSERT_VALUES_EQUAL(clientB.Heartbeat(readInfoB.MemberId, readInfoB.GenerationId, group)->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

            readInfoC = clientC.JoinAndSyncGroupAndWaitPartitions(topics, group, minActivePartitions/3, protocolName, minActivePartitions);
            UNIT_ASSERT_VALUES_EQUAL(clientC.Heartbeat(readInfoC.MemberId, readInfoC.GenerationId, group)->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

            readInfoD = clientD.JoinAndSyncGroupAndWaitPartitions(topics, group, minActivePartitions/3, protocolName, minActivePartitions);
            UNIT_ASSERT_VALUES_EQUAL(clientD.Heartbeat(readInfoD.MemberId, readInfoD.GenerationId, group)->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

            AssertPartitionsIsUniqueAndCountIsExpected({readInfoB, readInfoC, readInfoD}, minActivePartitions, topicName);


            // all other clients leaves the group
            UNIT_ASSERT_VALUES_EQUAL(clientB.LeaveGroup(readInfoB.MemberId, group)->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT_VALUES_EQUAL(clientC.LeaveGroup(readInfoC.MemberId, group)->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT_VALUES_EQUAL(clientD.LeaveGroup(readInfoD.MemberId, group)->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        }

        //release partition before lock
        {
            std::vector<TString> topics;
            topics.push_back(topicName);

            auto readInfoA = clientA.JoinGroup(topics, group, protocolName);
            Sleep(TDuration::MilliSeconds(200));
            auto readInfoB = clientB.JoinGroup(topics, group, protocolName);
            Sleep(TDuration::MilliSeconds(200));

            UNIT_ASSERT_VALUES_EQUAL(clientA.LeaveGroup(readInfoA->MemberId.value(), group)->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT_VALUES_EQUAL(clientB.LeaveGroup(readInfoB->MemberId.value(), group)->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        }

        {
            // Check short topic name
            std::vector<TString> topics;
            topics.push_back(shortTopicName);

            auto joinResponse = clientA.JoinGroup(topics, group, protocolName);
            UNIT_ASSERT_VALUES_EQUAL(joinResponse->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT_VALUES_EQUAL(clientA.LeaveGroup(joinResponse->MemberId.value(), group)->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        }

        {
            // Check not exists group/consumer
            std::vector<TString> topics;
            topics.push_back(topicName);

            auto joinResponse = clientA.JoinGroup(topics, notExistsGroup, protocolName);
            UNIT_ASSERT_VALUES_EQUAL(joinResponse->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::GROUP_ID_NOT_FOUND));
        }

        {
            // Check not exists topic
            std::vector<TString> topics;
            topics.push_back(notExistsTopicName);

            auto joinResponse = clientA.JoinGroup(topics, group, protocolName);
            UNIT_ASSERT_VALUES_EQUAL(joinResponse->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::UNKNOWN_TOPIC_OR_PARTITION));
        }

        {
            // Check few topics
            std::vector<TString> topics;
            topics.push_back(topicName);
            topics.push_back(secondTopicName);

            auto readInfo = clientA.JoinAndSyncGroupAndWaitPartitions(topics, group, minActivePartitions * 2, protocolName, minActivePartitions);

            std::unordered_set<TString> topicsSet;
            for (auto partition: readInfo.Partitions) {
                topicsSet.emplace(partition.Topic.value());
            }
            UNIT_ASSERT_VALUES_EQUAL(topicsSet.size(), 2);


            // Check change topics list
            topics.pop_back();
            auto joinResponse = clientA.JoinGroup(topics, group, protocolName);
            UNIT_ASSERT_VALUES_EQUAL(joinResponse->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::REBALANCE_IN_PROGRESS)); // tell client to rejoin
        }

    } // RunBalanceScenarionTest()

    Y_UNIT_TEST(BalanceScenario) {
        RunBalanceScenarionTest(false);
    }

    Y_UNIT_TEST(BalanceScenarioForFederation) {
        RunBalanceScenarionTest(true);
    }

    Y_UNIT_TEST(BalanceScenarioCdc) {

        TString protocolName = "roundrobin";
        TInsecureTestServer testServer("2", false, false);


        TString tableName = "/Root/table-0-test";
        TString feedName = "feed";
        TString feedPath = tableName + "/" + feedName;
        TString tableShortName = "table-0-test";
        TString feedShortPath = tableShortName + "/" + feedName;

        TString group = "consumer-0";
        TString notExistsGroup = "consumer-not-exists";

        // create table and init cdc for it
        {
            NYdb::NTable::TTableClient tableClient(*testServer.Driver);
            tableClient.RetryOperationSync([&](TSession session)
                {
                    NYdb::NTable::TTableBuilder builder;
                    builder.AddNonNullableColumn("key", NYdb::EPrimitiveType::Int64).SetPrimaryKeyColumn("key");
                    builder.AddNonNullableColumn("value", NYdb::EPrimitiveType::Int64);

                    auto createResult = session.CreateTable(tableName, builder.Build()).ExtractValueSync();
                    UNIT_ASSERT_VALUES_EQUAL(createResult.IsTransportError(), false);
                    Cerr << createResult.GetIssues().ToString() << "\n";
                    UNIT_ASSERT_VALUES_EQUAL(createResult.GetStatus(), EStatus::SUCCESS);

                    auto alterResult = session.AlterTable(tableName, NYdb::NTable::TAlterTableSettings()
                                    .AppendAddChangefeeds(NYdb::NTable::TChangefeedDescription(feedName,
                                                                                            NYdb::NTable::EChangefeedMode::Updates,
                                                                                            NYdb::NTable::EChangefeedFormat::Json))
                                                        ).ExtractValueSync();
                    Cerr << alterResult.GetIssues().ToString() << "\n";
                    UNIT_ASSERT_VALUES_EQUAL(alterResult.IsTransportError(), false);
                    UNIT_ASSERT_VALUES_EQUAL(alterResult.GetStatus(), EStatus::SUCCESS);
                    return alterResult;
                }
            );

            TValueBuilder rows;
            rows.BeginList();
            rows.AddListItem()
                .BeginStruct()
                    .AddMember("key").Int64(1)
                    .AddMember("value").Int64(2)
                .EndStruct();
            rows.EndList();

            auto upsertResult = tableClient.BulkUpsert(tableName, rows.Build()).GetValueSync();
            UNIT_ASSERT_EQUAL(upsertResult.GetStatus(), EStatus::SUCCESS);
        }

        NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
        AlterTopic(pqClient, feedPath, {group});

        for(auto name : {feedPath, feedShortPath} ) {
            TKafkaTestClient clientA(testServer.Port);
            {
                clientA.PlainAuthenticateToKafka("ouruser@/Root", "ourUserPassword");
            }

            {
                // Check partitions balance
                std::vector<TString> topics;
                topics.push_back(name);

                // clientA join group, and get all partitions
                auto readInfoA = clientA.JoinAndSyncGroupAndWaitPartitions(topics, group, 1, protocolName, 1);
                UNIT_ASSERT_VALUES_EQUAL(clientA.Heartbeat(readInfoA.MemberId, readInfoA.GenerationId, group)->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

                UNIT_ASSERT_VALUES_EQUAL(readInfoA.Partitions.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(readInfoA.Partitions[0].Topic, name);
            }
        }
    } // Y_UNIT_TEST(BalanceScenarioCdc)

    Y_UNIT_TEST(OffsetCommitConsumerAutocreationScenario) {
        TInsecureTestServer testServer("2", false, true, false, true, true, true);
        testServer.KikimrServer->GetRuntime()->SetLogPriority(NKikimrServices::PQ_WRITE_PROXY, NActors::NLog::PRI_TRACE);
        testServer.KikimrServer->GetRuntime()->SetLogPriority(NKikimrServices::PERSQUEUE, NActors::NLog::PRI_TRACE);

        TString firstTopicName = "/Root/topic-0-test";
        TString secondTopicName = "/Root/topic-1-test";
        TString shortTopicName = "topic-1-test";
        TString notExistsTopicName = "not-exists";
        ui64 minActivePartitions = 3;

        TString firstConsumerName = "consumer-0";
        TString secondConsumerName = "consumer-1";
        TString notExistsConsumerName1 = "notExists1";
        TString notExistsConsumerName2 = "notExists2";
        TString anotherConsumerName = "consumerSingle";

        TString key = "record-key";
        TString value = "record-value";
        TString headerKey = "header-key";
        TString headerValue = "header-value";

        TString commitedMetaData = "additional-info";

        NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
        CreateTopic(pqClient, firstTopicName, minActivePartitions, {firstConsumerName, secondConsumerName, anotherConsumerName});
        CreateTopic(pqClient, secondTopicName, minActivePartitions, {firstConsumerName});
        {
            // authenticating as user with only read rights
            TKafkaTestClient client(testServer.Port);
            TString userName = "useronlyreadrights@/Root";
            TString userPassword = "AbAcAbA";
            client.PlainAuthenticateToKafka(userName, userPassword);
            std::unordered_map<TString, std::vector<NKafka::TEvKafka::PartitionConsumerOffset>> offsets;
            std::vector<NKafka::TEvKafka::PartitionConsumerOffset> partitionsAndOffsets;
            for (ui64 i = 0; i < minActivePartitions; ++i) {
                partitionsAndOffsets.emplace_back(i, 0, commitedMetaData);
            }

            offsets[secondTopicName] = partitionsAndOffsets;
            auto msg = client.OffsetCommit(notExistsConsumerName2, offsets);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics.size(), 1);
            for (const auto& topic : msg->Topics) {
                UNIT_ASSERT_VALUES_EQUAL(topic.Partitions.size(), minActivePartitions);
                for (const auto& partition : topic.Partitions) {
                    UNIT_ASSERT_VALUES_EQUAL(partition.ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::GROUP_ID_NOT_FOUND));
                }
            }
            auto describeTopicSettings = NTopic::TDescribeTopicSettings().IncludeStats(true);
            auto result = pqClient.DescribeTopic(secondTopicName, describeTopicSettings).GetValueSync();
            UNIT_ASSERT(result.IsSuccess());
            const auto& consumers = result.GetTopicDescription().GetConsumers();
            UNIT_ASSERT_EQUAL(consumers.size(), 1);
            std::vector<TString> secondTopicConsumerNames;
            for (const  NYdb::NTopic::TConsumer& consumer : consumers) {
                secondTopicConsumerNames.push_back(TString(consumer.GetConsumerName()));
            }
            UNIT_ASSERT(std::find(secondTopicConsumerNames.begin(), secondTopicConsumerNames.end(), notExistsConsumerName2) == secondTopicConsumerNames.end());
        }

        {
            TKafkaTestClient client(testServer.Port);
            TString userName = "user123@/Root";
            TString userPassword = "UsErPassword";
            client.PlainAuthenticateToKafka(userName, userPassword);

            {
                // checking that if a request contains a topic that is not assigned to this consumer
                // no error is returned and this consumer is added to the topic
                std::unordered_map<TString, std::vector<NKafka::TEvKafka::PartitionConsumerOffset>> offsets;
                std::vector<NKafka::TEvKafka::PartitionConsumerOffset> partitionsAndOffsets;
                for (ui64 i = 0; i < minActivePartitions; ++i) {
                    partitionsAndOffsets.emplace_back(i, 0, commitedMetaData);
                }

                offsets[firstTopicName] = partitionsAndOffsets;
                offsets[secondTopicName] = partitionsAndOffsets;
                auto msg = client.OffsetCommit(anotherConsumerName, offsets);
                UNIT_ASSERT_VALUES_EQUAL(msg->Topics.size(), 2);
                for (const auto& topic : msg->Topics) {
                    UNIT_ASSERT_VALUES_EQUAL(topic.Partitions.size(), minActivePartitions);
                    for (const auto& partition : topic.Partitions) {
                        UNIT_ASSERT_VALUES_EQUAL(partition.ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
                    }
                }

                auto describeTopicSettings = NTopic::TDescribeTopicSettings().IncludeStats(true);
                auto result = pqClient.DescribeTopic(secondTopicName, describeTopicSettings).GetValueSync();
                UNIT_ASSERT(result.IsSuccess());
                const auto& consumers = result.GetTopicDescription().GetConsumers();
                UNIT_ASSERT_EQUAL(consumers.size(), 2);
                std::vector<TString> secondTopicConsumerNames;
                for (const  NYdb::NTopic::TConsumer& consumer : consumers) {
                    secondTopicConsumerNames.push_back(TString(consumer.GetConsumerName()));
                }
                UNIT_ASSERT(std::find(secondTopicConsumerNames.begin(), secondTopicConsumerNames.end(), firstConsumerName) != secondTopicConsumerNames.end());
                UNIT_ASSERT(std::find(secondTopicConsumerNames.begin(), secondTopicConsumerNames.end(), anotherConsumerName) != secondTopicConsumerNames.end());
            }


            auto recordsCount = 5;
            {
                // Produce

                TKafkaRecordBatch batch;
                batch.BaseOffset = 3;
                batch.BaseSequence = 5;
                batch.Magic = 2; // Current supported
                batch.Records.resize(recordsCount);
                batch.ProducerId = -1;
                batch.ProducerEpoch = -1;

                for (auto i = 0; i < recordsCount; i++) {
                    batch.Records[i].Key = TKafkaRawBytes(key.data(), key.size());
                    batch.Records[i].Value = TKafkaRawBytes(value.data(), value.size());
                }

                auto msg = client.Produce(firstTopicName, 0, batch);

                UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Name, firstTopicName);
                UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].PartitionResponses[0].Index, 0);
                UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].PartitionResponses[0].ErrorCode,
                                        static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            }

            {
                // check that errors are processed correctly if a new consumer is added
                std::unordered_map<TString, std::vector<NKafka::TEvKafka::PartitionConsumerOffset>> offsets;
                std::vector<NKafka::TEvKafka::PartitionConsumerOffset> partitionsAndOffsets;
                for (ui64 i = 0; i < minActivePartitions; ++i) {
                    if (i == 0) {
                        partitionsAndOffsets.emplace_back(i, 1, commitedMetaData);
                    } else if (i == 1) {
                        partitionsAndOffsets.emplace_back(i, 1, commitedMetaData);
                    } else {
                        partitionsAndOffsets.emplace_back(i, 0, commitedMetaData);
                    }
                }

                offsets[firstTopicName] = partitionsAndOffsets;
                auto msg = client.OffsetCommit(notExistsConsumerName1, offsets);
                UNIT_ASSERT_VALUES_EQUAL(msg->Topics.size(), 1);
                for (const auto& topic : msg->Topics) {
                    UNIT_ASSERT_VALUES_EQUAL(topic.Partitions.size(), minActivePartitions);
                    for (const auto& partition : topic.Partitions) {
                        if (partition.PartitionIndex != 1) {
                            UNIT_ASSERT_VALUES_EQUAL(partition.ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
                        } else {
                            UNIT_ASSERT_VALUES_EQUAL(partition.ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::OFFSET_OUT_OF_RANGE));
                        }
                    }
                }
                auto describeTopicSettings = NTopic::TDescribeTopicSettings().IncludeStats(true);
                auto result = pqClient.DescribeTopic(firstTopicName, describeTopicSettings).GetValueSync();
                UNIT_ASSERT(result.IsSuccess());
                const auto& consumers = result.GetTopicDescription().GetConsumers();
                UNIT_ASSERT_EQUAL(consumers.size(), 4);
                std::vector<TString> firstTopicConsumerNames;
                for (const  NYdb::NTopic::TConsumer& consumer : consumers) {
                    firstTopicConsumerNames.push_back(TString(consumer.GetConsumerName()));
                }
                UNIT_ASSERT(std::find(firstTopicConsumerNames.begin(), firstTopicConsumerNames.end(), notExistsConsumerName1) != firstTopicConsumerNames.end());
            }
        }

        {
            // check that user with no rights can't create consumer automatically
            TKafkaTestClient client(testServer.Port);
            TString userName = "usernorights@/Root";
            TString userPassword = "dummyPass";
            client.PlainAuthenticateToKafka(userName, userPassword);
            std::unordered_map<TString, std::vector<NKafka::TEvKafka::PartitionConsumerOffset>> offsets;
            std::vector<NKafka::TEvKafka::PartitionConsumerOffset> partitionsAndOffsets;
            for (ui64 i = 0; i < minActivePartitions; ++i) {
                partitionsAndOffsets.emplace_back(i, 0, commitedMetaData);
            }

            offsets[secondTopicName] = partitionsAndOffsets;
            auto msg = client.OffsetCommit(notExistsConsumerName2, offsets);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics.size(), 1);
            for (const auto& topic : msg->Topics) {
                UNIT_ASSERT_VALUES_EQUAL(topic.Partitions.size(), minActivePartitions);
                for (const auto& partition : topic.Partitions) {
                    UNIT_ASSERT_VALUES_EQUAL(partition.ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::TOPIC_AUTHORIZATION_FAILED));
                }
            }
            auto describeTopicSettings = NTopic::TDescribeTopicSettings().IncludeStats(true);
            auto result = pqClient.DescribeTopic(secondTopicName, describeTopicSettings).GetValueSync();
            UNIT_ASSERT(result.IsSuccess());
            const auto& consumers = result.GetTopicDescription().GetConsumers();
            UNIT_ASSERT_EQUAL(consumers.size(), 2);
            std::vector<TString> secondTopicConsumerNames;
            for (const  NYdb::NTopic::TConsumer& consumer : consumers) {
                secondTopicConsumerNames.push_back(TString(consumer.GetConsumerName()));
            }
            UNIT_ASSERT(std::find(secondTopicConsumerNames.begin(), secondTopicConsumerNames.end(), notExistsConsumerName2) == secondTopicConsumerNames.end());
        }

        {
            // check that user with no rights can't commit offset to the topic
            TKafkaTestClient client(testServer.Port);
            TString userName = "usernorights@/Root";
            TString userPassword = "dummyPass";
            client.PlainAuthenticateToKafka(userName, userPassword);
            std::unordered_map<TString, std::vector<NKafka::TEvKafka::PartitionConsumerOffset>> offsets;
            std::vector<NKafka::TEvKafka::PartitionConsumerOffset> partitionsAndOffsets;
            for (ui64 i = 0; i < minActivePartitions; ++i) {
                partitionsAndOffsets.emplace_back(i, 0, commitedMetaData);
            }

            offsets[secondTopicName] = partitionsAndOffsets;
            auto msg = client.OffsetCommit(firstConsumerName, offsets);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics.size(), 1);
            for (const auto& topic : msg->Topics) {
                UNIT_ASSERT_VALUES_EQUAL(topic.Partitions.size(), minActivePartitions);
                for (const auto& partition : topic.Partitions) {
                    UNIT_ASSERT_VALUES_EQUAL(partition.ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::TOPIC_AUTHORIZATION_FAILED));
                }
            }
        }
    }

    Y_UNIT_TEST(OffsetCommitAndFetchScenario) {
        TInsecureTestServer testServer("2", false, true, false, false);
        testServer.KikimrServer->GetRuntime()->SetLogPriority(NKikimrServices::PQ_WRITE_PROXY, NActors::NLog::PRI_TRACE);
        testServer.KikimrServer->GetRuntime()->SetLogPriority(NKikimrServices::PERSQUEUE, NActors::NLog::PRI_TRACE);

        TString firstTopicName = "/Root/topic-0-test";
        TString secondTopicName = "/Root/topic-1-test";
        TString shortTopicName = "topic-1-test";
        TString notExistsTopicName = "not-exists";
        ui64 minActivePartitions = 10;

        TString firstConsumerName = "consumer-0";
        TString secondConsumerName = "consumer-1";
        TString notExistsConsumerName = "notExists";

        TString key = "record-key";
        TString value = "record-value";
        TString headerKey = "header-key";
        TString headerValue = "header-value";

        TString commitedMetaData = "additional-info";

        NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
        CreateTopic(pqClient, firstTopicName, minActivePartitions, {firstConsumerName, secondConsumerName});
        CreateTopic(pqClient, secondTopicName, minActivePartitions, {firstConsumerName, secondConsumerName});

        TKafkaTestClient client(testServer.Port);

        client.PlainAuthenticateToKafka();

        auto recordsCount = 5;
        {
            // Produce

            TKafkaRecordBatch batch;
            batch.BaseOffset = 3;
            batch.BaseSequence = 5;
            batch.Magic = 2; // Current supported
            batch.Records.resize(recordsCount);
            batch.ProducerId = -1;
            batch.ProducerEpoch = -1;

            for (auto i = 0; i < recordsCount; i++) {
                batch.Records[i].Key = TKafkaRawBytes(key.data(), key.size());
                batch.Records[i].Value = TKafkaRawBytes(value.data(), value.size());
            }

            auto msg = client.Produce(firstTopicName, 0, batch);

            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Name, firstTopicName);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].PartitionResponses[0].Index, 0);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].PartitionResponses[0].ErrorCode,
                                     static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        }

        {
            // Fetch offsets
            std::map<TString, std::vector<i32>> topicsToPartions;
            topicsToPartions[firstTopicName] = std::vector<i32>{0, 1, 2, 3 };
            auto msg = client.OffsetFetch(firstConsumerName, topicsToPartions);
            UNIT_ASSERT_VALUES_EQUAL(msg->Groups.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics.size(), 1);
            const auto& partitions = msg->Groups[0].Topics[0].Partitions;
            UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 4);
            auto partition0 = std::find_if(partitions.begin(), partitions.end(), [](const auto& partition) { return partition.PartitionIndex == 0; });
            UNIT_ASSERT_VALUES_UNEQUAL(partition0, partitions.end());
            UNIT_ASSERT_VALUES_EQUAL(partition0->CommittedOffset, 0);
        }
        {
            std::unordered_map<TString, std::vector<NKafka::TEvKafka::PartitionConsumerOffset>> offsets;
            std::vector<NKafka::TEvKafka::PartitionConsumerOffset> partitionsAndOffsets;
        {
            // Check commit

            for (ui64 i = 0; i < minActivePartitions; ++i) {
                // check that if a partition has a non-zero committed offset (that doesn't exceed endoffset) and committed metadata
                // or a zero committed offset and metadata
                // than no error is thrown and metadata is updated

                // check that otherwise, if the committed offset exceeds current endoffset of the partition
                // than an error is returned and passed committed metadata is not saved

                if (i == 0) {
                    partitionsAndOffsets.emplace_back(i, static_cast<ui64>(recordsCount), commitedMetaData);
                } else if (i == 1) {
                    partitionsAndOffsets.emplace_back(i, 0, commitedMetaData);
                } else if (i == 2) {
                    partitionsAndOffsets.emplace_back(i, static_cast<ui64>(recordsCount), commitedMetaData);
                } else {
                    partitionsAndOffsets.emplace_back(i, static_cast<ui64>(recordsCount));
                }
            }
            offsets[firstTopicName] = partitionsAndOffsets;
            offsets[shortTopicName] = partitionsAndOffsets;
            auto msg = client.OffsetCommit(firstConsumerName, offsets);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics.size(), 2);
            for (const auto& topic : msg->Topics) {
                UNIT_ASSERT_VALUES_EQUAL(topic.Partitions.size(), minActivePartitions);
                for (const auto& partition : topic.Partitions) {
                    if (topic.Name.value() == firstTopicName) {
                        // in first topic
                        if (partition.PartitionIndex == 0 || partition.PartitionIndex == 1) {
                            UNIT_ASSERT_VALUES_EQUAL(partition.ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
                        } else {
                            UNIT_ASSERT_VALUES_EQUAL(partition.ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::OFFSET_OUT_OF_RANGE));
                        }
                    } else {
                        if (partition.PartitionIndex == 1) {
                            // nothing was produced in the second topic
                            // check that if a zero offset is committed no error occurs and committed metadata is saved
                            UNIT_ASSERT_VALUES_EQUAL(partition.ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
                        } else {
                            // otherwise, an error occurs, because committed offset exceeds endoffset
                            UNIT_ASSERT_VALUES_EQUAL(partition.ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::OFFSET_OUT_OF_RANGE));
                        }
                    }
                }
            }
        }

        {
            // Fetch offsets after commit
            std::map<TString, std::vector<i32>> topicsToPartions;
            topicsToPartions[firstTopicName] = std::vector<i32>{0, 1, 2 , 3 };
            auto msg = client.OffsetFetch(firstConsumerName, topicsToPartions);
            UNIT_ASSERT_VALUES_EQUAL(msg->Groups.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics.size(), 1);
            const auto& partitions = msg->Groups[0].Topics[0].Partitions;
            UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 4);
            auto partition0 = std::find_if(partitions.begin(), partitions.end(), [](const auto& partition) { return partition.PartitionIndex == 0; });
            UNIT_ASSERT_VALUES_UNEQUAL(partition0, partitions.end());
            UNIT_ASSERT_VALUES_EQUAL(partition0->CommittedOffset, 5);
            UNIT_ASSERT_VALUES_EQUAL(partition0->Metadata, commitedMetaData);
            int i = 0;
            // checking committed metadata for the first topic
            for (auto it = partitions.begin(); it != partitions.end(); it++) {
                if (i != 2) {
                    // for i == 0 and i == 1 check that committed metadata == "additional-info" as committed offset didn't exceed endoffset
                    // for other i != 2 values check that committed metadata is empty as no metadata was committed
                    // that a new value of metadata is saved
                    UNIT_ASSERT_VALUES_EQUAL(it->Metadata, partitionsAndOffsets[i].Metadata);
                } else {
                    // check that in case an error has occurred (because committed offset exceeded endoffset)
                    // committed metadata is not saved
                    UNIT_ASSERT_VALUES_EQUAL(it->Metadata, std::nullopt);
                }
                i += 1;
            }
        }
    }
        {
            // Check fetch offsets with nonexistent topic
            std::map<TString, std::vector<i32>> topicsToPartions;
            topicsToPartions[notExistsTopicName] = std::vector<i32>{0, 1};
            auto msg = client.OffsetFetch(firstConsumerName, topicsToPartions);
            UNIT_ASSERT_VALUES_EQUAL(msg->Groups.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions.size(), 2);
            for (const auto& partition : msg->Groups[0].Topics[0].Partitions) {
                UNIT_ASSERT_VALUES_EQUAL(partition.ErrorCode, UNKNOWN_TOPIC_OR_PARTITION);
            }
        }

        {
            // Check commit with nonexistent topic
            std::unordered_map<TString, std::vector<NKafka::TEvKafka::PartitionConsumerOffset>> offsets;
            std::vector<NKafka::TEvKafka::PartitionConsumerOffset> partitionsAndOffsets;
            for (ui64 i = 0; i < minActivePartitions; ++i) {
                partitionsAndOffsets.emplace_back(i, static_cast<ui64>(recordsCount), commitedMetaData);
            }
            offsets[firstTopicName] = partitionsAndOffsets;
            offsets[notExistsTopicName] = partitionsAndOffsets;

            auto msg = client.OffsetCommit(notExistsConsumerName, offsets);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics.back().Partitions.size(), minActivePartitions);
            for (const auto& topic : msg->Topics) {
                for (const auto& partition : topic.Partitions) {
                UNIT_ASSERT_VALUES_EQUAL(partition.ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::GROUP_ID_NOT_FOUND));
                }
            }
        }

        {
            // Check fetch offsets nonexistent consumer
            std::map<TString, std::vector<i32>> topicsToPartions;
            topicsToPartions[firstTopicName] = std::vector<i32>{0, 1};
            auto msg = client.OffsetFetch(notExistsConsumerName, topicsToPartions);
            UNIT_ASSERT_VALUES_EQUAL(msg->Groups.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions.size(), 2);
            for (const auto& partition : msg->Groups[0].Topics[0].Partitions) {
                UNIT_ASSERT_VALUES_EQUAL(partition.ErrorCode, RESOURCE_NOT_FOUND);
            }
        }

        {
            // Check commit with nonexistent consumer
            std::unordered_map<TString, std::vector<NKafka::TEvKafka::PartitionConsumerOffset>> offsets;
            std::vector<NKafka::TEvKafka::PartitionConsumerOffset> partitionsAndOffsets;
            for (ui64 i = 0; i < minActivePartitions; ++i) {
                partitionsAndOffsets.emplace_back(i, static_cast<ui64>(recordsCount), commitedMetaData);
            }
            offsets[firstTopicName] = partitionsAndOffsets;

            auto msg = client.OffsetCommit(notExistsConsumerName, offsets);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics.back().Partitions.size(), minActivePartitions);
            for (const auto& topic : msg->Topics) {
                for (const auto& partition : topic.Partitions) {
                UNIT_ASSERT_VALUES_EQUAL(partition.ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::GROUP_ID_NOT_FOUND));
                }
            }
        }

        {
            // Check fetch offsets with 2 consumers and topics
            TOffsetFetchRequestData request;

            TOffsetFetchRequestData::TOffsetFetchRequestGroup::TOffsetFetchRequestTopics topic;
            topic.Name = firstTopicName;
            auto partitionIndexes = std::vector<int>{0};
            topic.PartitionIndexes = partitionIndexes;

            TOffsetFetchRequestData::TOffsetFetchRequestGroup::TOffsetFetchRequestTopics shortTopic;
            shortTopic.Name = shortTopicName;
            shortTopic.PartitionIndexes = partitionIndexes;

            TOffsetFetchRequestData::TOffsetFetchRequestGroup group0;
            group0.GroupId = firstConsumerName;
            group0.Topics.push_back(topic);
            request.Groups.push_back(group0);

            TOffsetFetchRequestData::TOffsetFetchRequestGroup group1;
            group1.GroupId = secondConsumerName;
            group1.Topics.push_back(shortTopic);
            request.Groups.push_back(group1);

            auto msg = client.OffsetFetch(request);

            UNIT_ASSERT_VALUES_EQUAL(msg->Groups.size(), 2);
            for (const auto& group: msg->Groups) {
                UNIT_ASSERT_VALUES_EQUAL(group.Topics.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(group.Topics[0].Partitions.size(), 1);
                if (group.GroupId == firstConsumerName) {
                    UNIT_ASSERT_VALUES_EQUAL(group.Topics[0].Partitions[0].CommittedOffset, 5);
                } else if (group.GroupId == secondConsumerName) {
                    UNIT_ASSERT_VALUES_EQUAL(group.Topics[0].Partitions[0].CommittedOffset, 0);
                }
                UNIT_ASSERT_VALUES_EQUAL(group.Topics[0].Partitions[0].ErrorCode, NONE_ERROR);
            }
        }
    } // Y_UNIT_TEST(OffsetFetchScenario)


    void RunCreateTopicsScenario(TInsecureTestServer& testServer, TKafkaTestClient& client) {
        NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);

        auto describeTopicSettings = NTopic::TDescribeTopicSettings().IncludeStats(true);
        {
            // Creation of two topics
            auto msg = client.CreateTopics({
                TTopicConfig("topic-999-test", 12),
                TTopicConfig("topic-998-test", 13)
            });
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].Name.value(), "topic-999-test");
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[1].Name.value(), "topic-998-test");

            auto result999 = pqClient.DescribeTopic("/Root/topic-999-test", describeTopicSettings).GetValueSync();
            UNIT_ASSERT(result999.IsSuccess());
            UNIT_ASSERT_EQUAL(result999.GetTopicDescription().GetPartitions().size(), 12);

            auto result998 = pqClient.DescribeTopic("/Root/topic-998-test", describeTopicSettings).GetValueSync();
            UNIT_ASSERT(result998.IsSuccess());
            UNIT_ASSERT_EQUAL(result998.GetTopicDescription().GetPartitions().size(), 13);
        }

        {
            // Duplicate topics
            auto msg = client.CreateTopics({
                TTopicConfig("topic-997-test", 1),
                TTopicConfig("topic-997-test", 1)
            });

            UNIT_ASSERT_VALUES_EQUAL(msg->Topics.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].Name.value(), "topic-997-test");
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].ErrorCode, INVALID_REQUEST);

            auto describeTopicSettings = NTopic::TDescribeTopicSettings().IncludeStats(true);
            auto result = pqClient.DescribeTopic("/Root/topic-997-test", describeTopicSettings).GetValueSync();
            UNIT_ASSERT(!result.IsSuccess());
        }

        {
            // One OK, two duplicate topics
            auto msg = client.CreateTopics({
                TTopicConfig("topic-996-test", 1),
                TTopicConfig("topic-995-test", 1),
                TTopicConfig("topic-995-test", 1)
            });

            UNIT_ASSERT_VALUES_EQUAL(msg->Topics.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].Name.value(), "topic-996-test");
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].ErrorCode, NONE_ERROR);
            auto result996 = pqClient.DescribeTopic("/Root/topic-996-test", describeTopicSettings).GetValueSync();
            UNIT_ASSERT(result996.IsSuccess());

            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[1].Name.value(), "topic-995-test");
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[1].ErrorCode, INVALID_REQUEST);

            auto result995 = pqClient.DescribeTopic("/Root/topic-995-test", describeTopicSettings).GetValueSync();
            UNIT_ASSERT(!result995.IsSuccess());
        }

        {
            // Existing topic
            client.CreateTopics({ TTopicConfig("topic-994-test", 1) });
            auto result = pqClient.DescribeTopic("/Root/topic-994-test", describeTopicSettings).GetValueSync();
            UNIT_ASSERT(result.IsSuccess());

            auto msg = client.CreateTopics({ TTopicConfig("topic-994-test", 1) });
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].Name.value(), "topic-994-test");
        }

        {
            // Set valid retention
            ui64 retentionMs = 168 * 60 * 60 * 1000;
            ui64 retentionBytes = 51'200 * 1_MB;

            auto msg = client.CreateTopics({ TTopicConfig("topic-993-test", 1, std::to_string(retentionMs), std::to_string(retentionBytes))});
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].Name.value(), "topic-993-test");

            auto result993 = pqClient.DescribeTopic("/Root/topic-993-test", describeTopicSettings).GetValueSync();
            UNIT_ASSERT(result993.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(result993.GetTopicDescription().GetRetentionPeriod().MilliSeconds(), retentionMs);
            UNIT_ASSERT_VALUES_EQUAL(result993.GetTopicDescription().GetRetentionStorageMb().value(), retentionBytes / 1_MB);
        }

        {
            // retention.ms is not number
            auto msg = client.CreateTopics({ TTopicConfig("topic-992-test", 1, "not_a_number", "42")});
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].Name.value(), "topic-992-test");
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].ErrorCode, INVALID_CONFIG);

            auto result992 = pqClient.DescribeTopic("/Root/topic-992-test", describeTopicSettings).GetValueSync();
            UNIT_ASSERT(!result992.IsSuccess());
        }

        {
            // retention.bytes is not number
            auto msg = client.CreateTopics({ TTopicConfig("topic-991-test", 1, "42", "not_a_number")});
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].Name.value(), "topic-991-test");
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].ErrorCode, INVALID_CONFIG);

            auto result992 = pqClient.DescribeTopic("/Root/topic-992-test", describeTopicSettings).GetValueSync();
            UNIT_ASSERT(!result992.IsSuccess());
        }

        {
            // Empty topic name
            auto msg = client.CreateTopics({ TTopicConfig("", 1)});
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].ErrorCode, INVALID_REQUEST);
        }

        {
            // Wrong topic name
            auto msg = client.CreateTopics({ TTopicConfig("//////", 1)});
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].ErrorCode, INVALID_REQUEST);
        }

        {
            // Wrong topic name
            auto msg = client.CreateTopics({ TTopicConfig("/Root/", 1)});
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].ErrorCode, INVALID_REQUEST);
        }

        {
            // Wrong topic name
            auto msg = client.CreateTopics({ TTopicConfig("/Root//", 1)});
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].ErrorCode, INVALID_REQUEST);
        }

        {
            // Set invalid retention
            ui64 retentionMs = 13 * 60 * 60 * 1000;
            ui64 retentionBytes = 11'000'000'000ul;

            auto msg = client.CreateTopics({ TTopicConfig("topic-990-test", 1, std::to_string(retentionMs), std::to_string(retentionBytes))});
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].Name.value(), "topic-990-test");
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].ErrorCode, INVALID_REQUEST);

            auto result992 = pqClient.DescribeTopic("/Root/topic-990-test", describeTopicSettings).GetValueSync();
            UNIT_ASSERT(!result992.IsSuccess());
        }

        {
            // Set only ms retention
            ui64 retentionMs = 168 * 60 * 60 * 1000;
            auto msg = client.CreateTopics({ TTopicConfig("topic-989-test", 1, std::to_string(retentionMs)) });

            UNIT_ASSERT_VALUES_EQUAL(msg->Topics.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].Name.value(), "topic-989-test");
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].ErrorCode, INVALID_REQUEST);

            auto result993 = pqClient.DescribeTopic("/Root/topic-989-test", describeTopicSettings).GetValueSync();
            UNIT_ASSERT(!result993.IsSuccess());
        }

        {
            // Validation only
            auto msg = client.CreateTopics({ TTopicConfig("topic-988-test", 1)}, true);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].Name.value(), "topic-988-test");

            auto result993 = pqClient.DescribeTopic("/Root/topic-988-test", describeTopicSettings).GetValueSync();
            UNIT_ASSERT(!result993.IsSuccess());
        }

        {
            // Legal, but meaningless for Logbroker config
            std::map<TString, TString> configs { std::make_pair("flush.messages", "1") };
            auto msg = client.CreateTopics( { TTopicConfig("topic-987-test", 1, std::nullopt, std::nullopt, configs) });
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].Name.value(), "topic-987-test");
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].ErrorCode, NONE_ERROR);

            auto result = pqClient.DescribeTopic("/Root/topic-987-test", describeTopicSettings).GetValueSync();
            UNIT_ASSERT(result.IsSuccess());
        }

        {
            // Both legal and illegal configs
            std::map<TString, TString> configs { std::make_pair("compression.type", "zstd"), std::make_pair("flush.messages", "1") };
            auto msg = client.CreateTopics( { TTopicConfig("topic-986-test", 1, std::nullopt, std::nullopt, configs) });
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].Name.value(), "topic-986-test");
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].ErrorCode, INVALID_REQUEST);

            auto result = pqClient.DescribeTopic("/Root/topic-986-test", describeTopicSettings).GetValueSync();
            UNIT_ASSERT(!result.IsSuccess());
        }
    }

    Y_UNIT_TEST(CreateTopicsScenarioWithKafkaAuth) {
        TInsecureTestServer testServer("2");
        TKafkaTestClient client(testServer.Port);
        client.PlainAuthenticateToKafka();

        RunCreateTopicsScenario(testServer, client);
    } // Y_UNIT_TEST(CreateTopicsScenarioWithKafkaAuth)

    Y_UNIT_TEST(CreateTopicsScenarioWithoutKafkaAuth) {
        TInsecureTestServer testServer("2");
        TKafkaTestClient client(testServer.Port);

        RunCreateTopicsScenario(testServer, client);
    } // Y_UNIT_TEST(CreateTopicsScenarioWithoutKafkaAuth)

    Y_UNIT_TEST(CreateTopicTimestampTypeScenario) {
        TInsecureTestServer testServer("2");

        TString topic1Name = "/Root/topic-1-test";
        TString topic2Name = "/Root/topic-2-test";
        TString topic3Name = "/Root/topic-3-test";
        TString topic4Name = "/Root/topic-4-test";

        NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);

        CreateTopic(pqClient, topic1Name, 10, {});
        CreateTopic(pqClient, topic2Name, 10, {}, std::nullopt, "CreateTime");
        CreateTopic(pqClient, topic3Name, 10, {}, std::nullopt, "LogAppendTime");
        CreateTopic(pqClient, topic3Name, 10, {}, std::nullopt, "Incorrect", EStatus::BAD_REQUEST);

        TKafkaTestClient client(testServer.Port);
        client.ScramAuthenticateToKafka();

        auto describeTopicSettings = NTopic::TDescribeTopicSettings().IncludeStats(true);

        // check that timestamp_type has "CreateTime" value by default
        auto result1 = pqClient.DescribeTopic(topic1Name, describeTopicSettings).GetValueSync();
        UNIT_ASSERT(result1.IsSuccess());
        UNIT_ASSERT_EQUAL(result1.GetTopicDescription().GetAttributes().at("_timestamp_type"), "CreateTime");

        auto result2 = pqClient.DescribeTopic(topic2Name, describeTopicSettings).GetValueSync();
        UNIT_ASSERT(result2.IsSuccess());
        UNIT_ASSERT_EQUAL(result2.GetTopicDescription().GetAttributes().at("_timestamp_type"), "CreateTime");

        auto result3 = pqClient.DescribeTopic(topic3Name, describeTopicSettings).GetValueSync();
        UNIT_ASSERT(result3.IsSuccess());
        UNIT_ASSERT_EQUAL(result3.GetTopicDescription().GetAttributes().at("_timestamp_type"), "LogAppendTime");

        // checking that it is impossible to create a topic with incorrect timestamp type
        auto result4 = pqClient.DescribeTopic(topic4Name, describeTopicSettings).GetValueSync();
        UNIT_ASSERT(!result4.IsSuccess());
    }

    Y_UNIT_TEST(CreatePartitionsScenario) {

        TInsecureTestServer testServer("2");

        TString topic1Name = "/Root/topic-1-test";
        TString shortTopic1Name = "topic-1-test";

        TString topic2Name = "/Root/topic-2-test";
        TString shortTopic2Name = "topic-2-test";

        TString key = "record-key";
        TString value = "record-value";
        TString headerKey = "header-key";
        TString headerValue = "header-value";

        NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
        CreateTopic(pqClient, topic1Name, 10, {});
        CreateTopic(pqClient, topic2Name, 20, {});

        TKafkaTestClient client(testServer.Port);

        client.PlainAuthenticateToKafka();

        auto describeTopicSettings = NTopic::TDescribeTopicSettings().IncludeStats(true);

        {
            // Validate only
            auto msg = client.CreatePartitions({
                TTopicConfig(topic1Name, 11),
                TTopicConfig(topic2Name, 21)
            }, true);
            UNIT_ASSERT_VALUES_EQUAL(msg->Results.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(msg->Results[0].Name.value(), topic1Name);
            UNIT_ASSERT_VALUES_EQUAL(msg->Results[1].Name.value(), topic2Name);

            auto result0 = pqClient.DescribeTopic(topic1Name, describeTopicSettings).GetValueSync();
            UNIT_ASSERT(result0.IsSuccess());
            UNIT_ASSERT_EQUAL(result0.GetTopicDescription().GetPartitions().size(), 10);

            auto result1 = pqClient.DescribeTopic(topic2Name, describeTopicSettings).GetValueSync();
            UNIT_ASSERT(result1.IsSuccess());
            UNIT_ASSERT_EQUAL(result1.GetTopicDescription().GetPartitions().size(), 20);
        }

        {
            // Increase partitions number
            auto msg = client.CreatePartitions({
                TTopicConfig(shortTopic1Name, 11),
                TTopicConfig(shortTopic2Name, 21)
            });

            UNIT_ASSERT_VALUES_EQUAL(msg->Results.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(msg->Results[0].Name.value(), shortTopic1Name);
            UNIT_ASSERT_VALUES_EQUAL(msg->Results[0].ErrorCode, NONE_ERROR);
            UNIT_ASSERT_VALUES_EQUAL(msg->Results[1].Name.value(), shortTopic2Name);
            UNIT_ASSERT_VALUES_EQUAL(msg->Results[1].ErrorCode, NONE_ERROR);

            auto result1 = pqClient.DescribeTopic(topic1Name, describeTopicSettings).GetValueSync();
            UNIT_ASSERT(result1.IsSuccess());
            UNIT_ASSERT_EQUAL(result1.GetTopicDescription().GetPartitions().size(), 11);

            auto result2 = pqClient.DescribeTopic(topic2Name, describeTopicSettings).GetValueSync();
            UNIT_ASSERT(result2.IsSuccess());
            UNIT_ASSERT_EQUAL(result2.GetTopicDescription().GetPartitions().size(), 21);
        }

        {
            // Check with two same topic names
            auto msg = client.CreatePartitions({
                TTopicConfig(shortTopic1Name, 12),
                TTopicConfig(shortTopic1Name, 12)
            });

            UNIT_ASSERT_VALUES_EQUAL(msg->Results.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Results[0].Name.value(), shortTopic1Name);
            UNIT_ASSERT_VALUES_EQUAL(msg->Results[0].ErrorCode, INVALID_REQUEST);

            auto result = pqClient.DescribeTopic(topic1Name, describeTopicSettings).GetValueSync();
            UNIT_ASSERT(result.IsSuccess());
            UNIT_ASSERT_EQUAL(result.GetTopicDescription().GetPartitions().size(), 11);
        }

        {
            // Check with lesser partitions number
            auto msg = client.CreatePartitions({ TTopicConfig(shortTopic1Name, 1) });

            UNIT_ASSERT_VALUES_EQUAL(msg->Results.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Results[0].Name.value(), shortTopic1Name);
            UNIT_ASSERT_VALUES_EQUAL(msg->Results[0].ErrorCode, INVALID_REQUEST);

            auto result1 = pqClient.DescribeTopic(topic1Name, describeTopicSettings).GetValueSync();
            UNIT_ASSERT(result1.IsSuccess());
            UNIT_ASSERT_EQUAL(result1.GetTopicDescription().GetPartitions().size(), 11);
        }

        {
            // Check with nonexistent topic name
            auto topicName = "NonExTopicName";
            auto msg = client.CreatePartitions({ TTopicConfig(topicName, 1) });

            UNIT_ASSERT_VALUES_EQUAL(msg->Results.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Results[0].Name.value(), topicName);
            UNIT_ASSERT_VALUES_EQUAL(msg->Results[0].ErrorCode, UNKNOWN_TOPIC_OR_PARTITION);

            auto result1 = pqClient.DescribeTopic(topic1Name, describeTopicSettings).GetValueSync();
            UNIT_ASSERT(result1.IsSuccess());
            UNIT_ASSERT_EQUAL(result1.GetTopicDescription().GetPartitions().size(), 11);
        }
    } // Y_UNIT_TEST(CreatePartitionsScenario)

    static std::unordered_map<ui64, TString> dummyMessages;

    ui64 WriteMessagesWithKeys(auto& writeSession, const TVector<std::pair<TString, ui64>>& keyToSize , ui64 cyclesCount) {
        ui64 totalWritten = 0;
        for (auto i = 0u; i < cyclesCount; i++) {
            for (const auto& [key, size] : keyToSize) {
                auto& msgBody = dummyMessages[size];
                if (msgBody.empty()) {
                    msgBody = TString{size, 'a'};
                }
                NYdb::NTopic::TWriteMessage message{msgBody};
                NYdb::NTopic::TWriteMessage::TMessageMeta meta1;
                meta1.push_back(std::make_pair("__key", key));
                message.MessageMeta(meta1);
                writeSession->Write(std::move(message));
                totalWritten++;
            }
        }
        return totalWritten;
    }

    void RunTestTopicsWithCleanupPolicy(TInsecureTestServer& testServer, TKafkaTestClient& client) {
        NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);

        TString topic1 = "topic-999-test", topic2 = "topic-998-test";
        TStringBuilder topic1FullPath;
        topic1FullPath << "/Root/" << topic1;

        CreateTopic(pqClient, topic1FullPath, 1, {"consumer1"});
        {
            // Creation of two topics
            auto msg = client.CreateTopics({
                TTopicConfig(topic1, 1, std::nullopt, std::nullopt, {{"cleanup.policy", "compact"}}),
                TTopicConfig(topic2, 13, std::nullopt, std::nullopt, {{"cleanup.policy", "delete"}}),
                TTopicConfig("topic_bad", 1, std::nullopt, std::nullopt, {{"cleanup.policy", "bad"}})
            });
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].Name.value(), topic1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[1].Name.value(), topic2);

            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].ErrorCode, NONE_ERROR);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[1].ErrorCode, NONE_ERROR);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[2].ErrorCode, INVALID_REQUEST);
        }

        auto getConfigsMap = [&](const auto& describeResult) {
            THashMap<TString, TDescribeConfigsResponseData::TDescribeConfigsResult::TDescribeConfigsResourceResult> configs;
            for (const auto& config : describeResult.Configs) {
                configs[TString(config.Name->data())] = config;
            }
            return configs;
        };

        struct TDescribeTopicResult {
            TString name;
            TString policy;
        };


        auto checkDescribeTopic = [&](const std::vector<TDescribeTopicResult>& topics) {
            std::vector<TString> topicNames;

            for (const auto& topic : topics) {
                bool hasCompactionConsumer = false;
                auto result0 = pqClient.DescribeTopic(topic.name, NTopic::TDescribeTopicSettings{}).GetValueSync();
                UNIT_ASSERT(result0.IsSuccess());
                const auto& consumers = result0.GetTopicDescription().GetConsumers();
                Cerr << "Check consumers for topic: " << topic.name << " with policy: " << topic.policy << Endl;
                for (const auto& consumer : consumers) {
                    Cerr << "Got consumer with name: " << consumer.GetConsumerName() << Endl;
                    if (consumer.GetConsumerName() == NKikimr::NPQ::CLIENTID_COMPACTION_CONSUMER) {
                        hasCompactionConsumer = true;
                        break;
                    }
                }
                if (topic.policy == "compact") {
                    UNIT_ASSERT_C(hasCompactionConsumer, topic.name);
                } else {
                    UNIT_ASSERT_C(!hasCompactionConsumer, topic.name);
                }
                topicNames.push_back(topic.name);
            }
            auto msg = client.DescribeConfigs(topicNames);
            UNIT_ASSERT_VALUES_EQUAL(msg->Results.size(), topics.size());
            for (auto i = 0u; i < topics.size(); ++i) {
                const auto& res = msg->Results[i];
                UNIT_ASSERT_VALUES_EQUAL(res.ResourceName.value(), topics[i].name);
                UNIT_ASSERT_VALUES_EQUAL(res.ErrorCode, NONE_ERROR);
                UNIT_ASSERT_VALUES_EQUAL_C(getConfigsMap(res).find("cleanup.policy")->second.Value->data(),
                                           topics[i].policy, res.ResourceName.value());

                auto topicDescribe = pqClient.DescribeTopic(topics[i].name).ExtractValueSync();
                UNIT_ASSERT_C(topicDescribe.IsSuccess(), topicDescribe.GetIssues().ToString());
                bool hasCompConsumer = false;
                for (const auto& consumer : topicDescribe.GetTopicDescription().GetConsumers()) {
                    Cerr << "Got consumer = " << consumer.GetConsumerName() << " for topic " << topics[i].name << Endl;
                    if (consumer.GetConsumerName() == NPQ::CLIENTID_COMPACTION_CONSUMER) {
                        hasCompConsumer = true;
                        break;
                    }
                }
                if (topics[i].policy == "compact") {
                    UNIT_ASSERT_C(hasCompConsumer, topics[i].name);
                } else {
                    UNIT_ASSERT_C(!hasCompConsumer, topics[i].name);
                }
            }
        };

        {
            auto msg = client.AlterConfigs({
                TTopicConfig(topic1, 1, std::nullopt, std::nullopt, {{"cleanup.policy", "compact"}}),
            });
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].ErrorCode, NONE_ERROR);
            checkDescribeTopic({{topic1, "compact"}, {topic2, "delete"}});
        }
        NYdb::NTopic::TWriteSessionSettings wSSettings{topic1FullPath, "producer1", ""};
        wSSettings.Codec(NTopic::ECodec::RAW);

        auto writeSession = pqClient.CreateSimpleBlockingWriteSession(wSSettings);
        ui64 totalWritten = 0;

        {
            auto msg = client.AlterConfigs({
                TTopicConfig(topic1, 1, std::nullopt, std::nullopt, {{"cleanup.policy", "delete"}}),
                TTopicConfig(topic2, 13, std::nullopt, std::nullopt, {{"cleanup.policy", "delete"}})
            });
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[1].ErrorCode, NONE_ERROR);
            checkDescribeTopic({{topic1, "delete"}, {topic2, "delete"}});
        }

        {
            auto msg = client.AlterConfigs({
                TTopicConfig(topic1, 1, std::nullopt, std::nullopt, {{"cleanup.policy", "bad"}}),
            });
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].ErrorCode, INVALID_REQUEST);
            checkDescribeTopic({{topic1, "delete"}, {topic2, "delete"}});

        }
        {
            auto msg = client.AlterConfigs({
                TTopicConfig(topic1, 1, std::nullopt, std::nullopt, {{"cleanup.policy", "compact"}}),
                TTopicConfig(topic2, 13, std::nullopt, std::nullopt, {{"cleanup.policy", ""}})
            });
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[1].ErrorCode, INVALID_REQUEST);
            checkDescribeTopic({{topic1, "compact"}, {topic2, "delete"}});

        }
        ui64 xtraKeySize = 3_MB;
        std::vector<ui64> msgSize = {100_KB, 500_KB, 8_MB, /*9_MB, 20_MB*/};
        // ToDo: return back after fix of big messages;
        // LOGBROKER-9700
        std::unordered_map<size_t, TString> messages;
        for (auto size : msgSize) {
            messages[size] = TString{size, 'a'};
        }
        messages[xtraKeySize] = TString{xtraKeySize, 'a'};
        THashSet<TString> keysWritten;
        auto writeMessage = [&] (const TString& key, ui64 size) {
            NYdb::NTopic::TWriteMessage message{messages[size]};
            NYdb::NTopic::TWriteMessage::TMessageMeta meta1;
            meta1.push_back(std::make_pair("__key", key));
            keysWritten.insert(key);
            message.MessageMeta(meta1);
            writeSession->Write(std::move(message));
            totalWritten++;
        };

        Cerr << ">>>>> BEGIN WRITE" << Endl;

        ui32 totalWriteCycles = 20;
        for (auto i = 0u; i < totalWriteCycles; i++) {
            for (auto size: msgSize) {
                writeMessage(ToString(size), size);
            }
            TStringBuilder xtraKey;
            xtraKey << "extra-key-" << i;
            writeMessage(xtraKey, xtraKeySize);
            Cerr << "Wrote message " << i << Endl;
        }
        Cerr << ">>>>> END WRITE" << Endl;

        writeSession->Close(TDuration::Seconds(10));
        Cerr << ">>>>> SESSION CLOSED" << Endl;
        Sleep(TDuration::Seconds(20));

        NYdb::NTopic::TReadSessionSettings rSSettings{.ConsumerName_ = "consumer1"};
        rSSettings.AppendTopics({topic1FullPath});
        auto readSession = pqClient.CreateReadSession(rSSettings);
        auto getMessagesFromTopic = [&](auto& reader) {
            TMaybe<NTopic::TReadSessionEvent::TDataReceivedEvent> result;
            while (true) {
                auto event = reader->GetEvent(false);
                if (!event)
                    return result;
                if (auto dataEvent = std::get_if<NTopic::TReadSessionEvent::TDataReceivedEvent>(&*event)) {
                    dataEvent->Commit();
                    result = *dataEvent;

                    break;
                } else if (auto *lockEv = std::get_if<NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&*event)) {
                    lockEv->Confirm();
                } else if (auto *releaseEv = std::get_if<NTopic::TReadSessionEvent::TStopPartitionSessionEvent>(&*event)) {
                    releaseEv->Confirm();
                } else if (auto *closeSessionEvent = std::get_if<NTopic::TSessionClosedEvent>(&*event)) {
                    Cerr << "Session closed event\n";
                    return result;
                }
            }
            return result;
        };
        ui64 totalTries = 5;
        ui64 totalMessages = 0;
        THashSet<TString> keysFound;
        while(totalTries) {
            auto result = getMessagesFromTopic(readSession);
            if (!result) {
                --totalTries;
                Sleep(TDuration::MilliSeconds(500));
                continue;
            }
            if (result) {
                totalTries = 5;
                for (const auto& message : result->GetMessages()) {
                    TStringBuilder msg;
                    msg << "Got message from offset " << message.GetOffset() << " and size: " << message.GetData().size() << Endl;
                    keysFound.emplace(message.GetMessageMeta()->Fields[0].second);
                    totalMessages++;
                    msg << " with meta: " << message.GetMessageMeta()->Fields[0].first << ":" << message.GetMessageMeta()->Fields[0].second << Endl;
                    Cerr << msg;
                    UNIT_ASSERT(!message.GetMessageMeta()->Fields.empty());
                    UNIT_ASSERT(message.GetData().size() > 0);
                }
            }
        }
        Cerr << "Total messages: " << totalMessages << Endl;
        UNIT_ASSERT(keysFound == keysWritten);
        UNIT_ASSERT_VALUES_EQUAL(keysFound.size(), 3 + totalWriteCycles); //4 + 15
        UNIT_ASSERT(totalMessages < totalWritten);
    }

    Y_UNIT_TEST(TopicsWithCleanupPolicyScenario) {
        // TODO(abcdef): temporarily deleted
        return;

        // TTestServer(const TString& kafkaApiMode = "1", bool serverless = false, bool enableNativeKafkaBalancing = true,
        // bool enableAutoTopicCreation = true, bool enableAutoConsumerCreation = true, bool enableQuoting = true) {
        TInsecureTestServer testServer(TTestServerSettings{.KafkaApiMode = "2", .EnableQuoting = false});
        TKafkaTestClient client(testServer.Port);

        RunTestTopicsWithCleanupPolicy(testServer, client);
    }

    Y_UNIT_TEST(TopicsCompactionSwitchOnAndOff) {
        TInsecureTestServer testServer(TTestServerSettings{.KafkaApiMode = "2", .EnableQuoting = false});
        TKafkaTestClient client(testServer.Port);
        TString topic = "topic-comp-test";
        TStringBuilder topicFullPath;
        topicFullPath << "/Root/" << topic;

        NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
        CreateTopic(pqClient, topicFullPath, 1, {"consumer1"}, 3);

        auto alterTopic = [&](bool compact) {
            auto msg = client.AlterConfigs({
                TTopicConfig(topic, 1, std::nullopt, std::nullopt, {{"cleanup.policy", compact ? "compact" : "delete"}})
            });
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].ErrorCode, NONE_ERROR);
        };

        alterTopic(true);

        NYdb::NTopic::TWriteSessionSettings wSSettings{topicFullPath, "producer1", ""};
        wSSettings.Codec(NTopic::ECodec::RAW);

        auto writeSession = pqClient.CreateSimpleBlockingWriteSession(wSSettings);
        ui64 totalWritten = 0;

        totalWritten += WriteMessagesWithKeys(writeSession,
            {{"key-small1", 1_KB}, {"key-large", 7_MB}, {"key-small2", 2_KB}}, 10);
        Sleep(TDuration::Seconds(3));

        alterTopic(false);
        totalWritten += WriteMessagesWithKeys(writeSession,
            {{"key-small1", 1_KB}, {"key-large", 7_MB}, {"key-small2", 2_KB}}, 10);
        Sleep(TDuration::Seconds(3));

        alterTopic(true);
        totalWritten += WriteMessagesWithKeys(writeSession,
            {{"key-small1", 1_KB}, {"key-large", 7_MB}, {"key-small2", 2_KB}}, 10);
        Sleep(TDuration::Seconds(3));

        alterTopic(false);
        totalWritten += WriteMessagesWithKeys(
            writeSession, {{"key-small1", 1_KB}, {"key-large", 7_MB},
                           {"key-small2", 2_KB}}, 10);

        NYdb::NTopic::TReadSessionSettings rSSettings{.ConsumerName_ = "consumer1"};
        rSSettings.AppendTopics({topicFullPath});
        auto readSession = pqClient.CreateReadSession(rSSettings);
        ui32 triesCount = 15;
        TMaybe<ui64> lastSeqNo;
        TMaybe<TString> lastKey;

        while(triesCount && lastSeqNo.GetOrElse(0) < totalWritten) {
            auto results = Read(readSession, false);
            if (results.empty()) {
                triesCount--;
                Sleep(TDuration::MilliSeconds(250));
                continue;
            }
            for (auto& dataEvent : results) {
                dataEvent.Commit();

                for (const auto& msg : dataEvent.GetMessages()) {
                    if (lastSeqNo) {
                        UNIT_ASSERT_VALUES_EQUAL(*lastSeqNo + 1, msg.GetSeqNo());
                    }
                    lastSeqNo = msg.GetSeqNo();
                    const auto& key = GetMessageMetaKey(msg, "__key");
                    Cerr << "===Got message from SeqNo: " << msg.GetSeqNo() << " with key : " << key << Endl;

                    UNIT_ASSERT(key);
                    if (lastKey) {
                        if (*lastKey == "key-small1") {
                            UNIT_ASSERT_VALUES_EQUAL(key, "key-large");
                            UNIT_ASSERT_VALUES_EQUAL(msg.GetData().size(), 7_MB);
                        } else if (*lastKey == "key-small2") {
                            UNIT_ASSERT_VALUES_EQUAL(key, "key-small1");
                            UNIT_ASSERT_VALUES_EQUAL(msg.GetData().size(), 1_KB);
                        } else if (*lastKey == "key-large") {
                           UNIT_ASSERT_VALUES_EQUAL(key, "key-small2");
                           UNIT_ASSERT_VALUES_EQUAL(msg.GetData().size(), 2_KB);
                        } else {
                            Cerr << "Bad key: " << *lastKey << Endl;
                            UNIT_FAIL("");
                        }
                    }
                    lastKey = key;
                }
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(*lastKey, "key-small2");
        UNIT_ASSERT_VALUES_EQUAL(*lastSeqNo, totalWritten);
    }

    Y_UNIT_TEST(TopicsCompactionGapFromRetention) {
        TInsecureTestServer testServer(TTestServerSettings{.KafkaApiMode = "2", .EnableQuoting = false});
        TKafkaTestClient client(testServer.Port);
        TString topic = "topic-comp-test";
        TStringBuilder topicFullPath;
        topicFullPath << "/Root/" << topic;

        NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
            CreateTopic(pqClient, topicFullPath, 1, {"consumer1"}, /*retention_sec = */ 1);

            NYdb::NTopic::TWriteSessionSettings wSSettings{topicFullPath, "producer1", ""};
            wSSettings.Codec(NTopic::ECodec::RAW);
            auto writeSession = pqClient.CreateSimpleBlockingWriteSession(wSSettings);

            WriteMessagesWithKeys(writeSession, {{"key-1", 7_MB}}, 3);
            WriteMessagesWithKeys(writeSession, {{"key-new", 100}}, 20);
            Sleep(TDuration::Seconds(15));

            NYdb::NTopic::TReadSessionSettings rSSettings{.ConsumerName_ = "consumer1"};
            rSSettings.AppendTopics({topicFullPath});
            auto readSession = pqClient.CreateReadSession(rSSettings);
            for (ui32 triesCount = 3; triesCount != 0;) {
                auto results = Read(readSession, false);
                if (results.empty()) {
                    triesCount--;
                    Sleep(TDuration::MilliSeconds(250));
                    continue;
                }
                for (auto& dataEvent : results) {
                    for (const auto& msg : dataEvent.GetMessages()) {
                        UNIT_ASSERT_GE(msg.GetOffset(), 1);
                        break;
                    }
                }
            }
            auto msg = client.AlterConfigs(
                {TTopicConfig(topic, 1, std::nullopt, std::nullopt, {{"cleanup.policy", "compact"}})});
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].ErrorCode, NONE_ERROR);
            WriteMessagesWithKeys(writeSession, {{"key-1", 6_MB}}, 10);
            Sleep(TDuration::Seconds(10));
            for (ui32 triesCount = 3; triesCount != 0;) {
                auto results = Read(readSession, false);
                if (results.empty()) {
                    triesCount--;
                    Sleep(TDuration::MilliSeconds(250));
                    continue;
                }
                for (auto& dataEvent : results) {
                    for (const auto& msg : dataEvent.GetMessages()) {
                        UNIT_ASSERT_GT(msg.GetOffset(), 1);
                        break;
                    }
                }
            }
        }

    Y_UNIT_TEST(DescribeConfigsScenario) {
        TInsecureTestServer testServer("2");

        TString topic0Name = "/Root/topic-0-test";
        TString shortTopic0Name = "topic-0-test";
        TString topic1Name = "/Root/topic-1-test";
        TString shortTopic1Name = "topic-1-test";
        TString notExistsTopicName = "/Root/not-exists";
        //ui64 minActivePartitions = 10;

        NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
        {
            auto result0 = pqClient.CreateTopic(
                topic0Name,
                NYdb::NTopic::TCreateTopicSettings().PartitioningSettings(5, 5).RetentionPeriod(TDuration::Hours(10))
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result0.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result0.GetStatus(), EStatus::SUCCESS, result0.GetIssues().ToString());

            auto result1 = pqClient.CreateTopic(
                topic1Name,
                NYdb::NTopic::TCreateTopicSettings().PartitioningSettings(10, 10).RetentionStorageMb(51200)
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result1.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result1.GetStatus(), EStatus::SUCCESS, result1.GetIssues().ToString());
        }

        TKafkaTestClient client(testServer.Port);

        client.PlainAuthenticateToKafka();

        auto getConfigsMap = [&](const auto& describeResult) {
            THashMap<TString, TDescribeConfigsResponseData::TDescribeConfigsResult::TDescribeConfigsResourceResult> configs;
            for (const auto& config : describeResult.Configs) {
                configs[TString(config.Name->data())] = config;
            }
            return configs;
        };
        {
            auto msg = client.DescribeConfigs({ shortTopic0Name, notExistsTopicName, shortTopic1Name});
            const auto& res0 = msg->Results[0];
            UNIT_ASSERT_VALUES_EQUAL(res0.ResourceName.value(), shortTopic0Name);
            UNIT_ASSERT_VALUES_EQUAL(res0.ErrorCode, NONE_ERROR);
            auto configs0 = getConfigsMap(res0);
            UNIT_ASSERT_VALUES_EQUAL(configs0.size(), 33);
            UNIT_ASSERT_VALUES_EQUAL(FromString<ui64>(configs0.find("retention.ms")->second.Value->data()), TDuration::Hours(10).MilliSeconds());
            UNIT_ASSERT_VALUES_EQUAL(configs0.find("cleanup.policy")->second.Value->data(), "delete");

            UNIT_ASSERT_VALUES_EQUAL(msg->Results[1].ResourceName.value(), notExistsTopicName);
            UNIT_ASSERT_VALUES_EQUAL(msg->Results[1].ErrorCode, UNKNOWN_TOPIC_OR_PARTITION);

            UNIT_ASSERT_VALUES_EQUAL(msg->Results[2].ResourceName.value(), shortTopic1Name);
            UNIT_ASSERT_VALUES_EQUAL(msg->Results[2].ErrorCode, NONE_ERROR);
            auto configs1 = getConfigsMap(msg->Results[2]);
            UNIT_ASSERT_VALUES_EQUAL(FromString<ui64>(configs1.find("retention.bytes")->second.Value->data()), 51200 * 1_MB);
            UNIT_ASSERT_VALUES_EQUAL(FromString<ui64>(configs1.find("max.message.bytes")->second.Value->data()), 1_KB);
        }
        {
            auto msg = client.DescribeConfigs({ shortTopic0Name, shortTopic0Name});
            UNIT_ASSERT_VALUES_EQUAL(msg->Results.size(), 1);
            const auto& res0 = msg->Results[0];
            UNIT_ASSERT_VALUES_EQUAL(res0.ResourceName.value(), shortTopic0Name);
            UNIT_ASSERT_VALUES_EQUAL(res0.ErrorCode, NONE_ERROR);
        }
    }

    Y_UNIT_TEST(AlterConfigsScenario) {
        TInsecureTestServer testServer("2");

        TString topic0Name = "/Root/topic-0-test";
        TString shortTopic0Name = "topic-0-test";
        TString topic1Name = "/Root/topic-1-test";
        TString shortTopic1Name = "topic-1-test";
        TString notExistsTopicName = "/Root/not-exists";
        ui64 minActivePartitions = 10;

        NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
        {
            auto result0 = pqClient.CreateTopic(
                topic0Name,
                NYdb::NTopic::TCreateTopicSettings().PartitioningSettings(minActivePartitions, minActivePartitions)
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result0.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result0.GetStatus(), EStatus::SUCCESS);

            auto result1 = pqClient.CreateTopic(
                topic1Name,
                NYdb::NTopic::TCreateTopicSettings().PartitioningSettings(minActivePartitions, minActivePartitions)
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result1.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result1.GetStatus(), EStatus::SUCCESS);
        }

        TKafkaTestClient client(testServer.Port);

        client.PlainAuthenticateToKafka();

        auto describeTopicSettings = NTopic::TDescribeTopicSettings().IncludeStats(true);

        {
            // Check validate only
            auto msg = client.AlterConfigs({ TTopicConfig(shortTopic0Name, 1), TTopicConfig(notExistsTopicName, 1) }, true);

            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].ResourceName.value(), shortTopic0Name);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].ErrorCode, NONE_ERROR);

            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[1].ResourceName.value(), notExistsTopicName);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[1].ErrorCode, NONE_ERROR);
        }

        {
            // Set valid retention
            ui64 retentionMs = 168 * 60 * 60 * 1000;
            ui64 retentionBytes = 51'200 * 1_MB;

            auto msg = client.AlterConfigs({
                    TTopicConfig(shortTopic0Name, 1, std::to_string(retentionMs), std::to_string(retentionBytes)),
                    TTopicConfig(shortTopic1Name, 1, std::to_string(retentionMs), std::to_string(retentionBytes)),
            });

            UNIT_ASSERT_VALUES_EQUAL(msg->Responses.size(), 2);

            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].ResourceName.value(), shortTopic0Name);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].ErrorCode, NONE_ERROR);

            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[1].ResourceName.value(), shortTopic1Name);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[1].ErrorCode, NONE_ERROR);

            auto result0 = pqClient.DescribeTopic(shortTopic0Name, describeTopicSettings).GetValueSync();
            UNIT_ASSERT(result0.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(result0.GetTopicDescription().GetRetentionPeriod().MilliSeconds(), retentionMs);
            UNIT_ASSERT_VALUES_EQUAL(result0.GetTopicDescription().GetRetentionStorageMb().value(), retentionBytes / (1024 * 1024));

            auto result1 = pqClient.DescribeTopic(shortTopic0Name, describeTopicSettings).GetValueSync();
            UNIT_ASSERT(result1.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(result1.GetTopicDescription().GetRetentionPeriod().MilliSeconds(), retentionMs);
            UNIT_ASSERT_VALUES_EQUAL(result1.GetTopicDescription().GetRetentionStorageMb().value(), retentionBytes / (1024 * 1024));
        }

        {
            // Set valid timestamp type
            ui64 retentionMs = 168 * 60 * 60 * 1000;
            ui64 retentionBytes = 51'200 * 1_MB;

            TString timestampType = "LogAppendTime";

            auto msg = client.AlterConfigs({
                    TTopicConfig(shortTopic0Name, 1, std::to_string(retentionMs), std::to_string(retentionBytes), {}, 1, timestampType)
            });

            UNIT_ASSERT_VALUES_EQUAL(msg->Responses.size(), 1);

            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].ResourceName.value(), shortTopic0Name);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].ErrorCode, NONE_ERROR);

            auto result0 = pqClient.DescribeTopic(shortTopic0Name, describeTopicSettings).GetValueSync();
            UNIT_ASSERT(result0.IsSuccess());
            auto d = result0.GetTopicDescription();
            UNIT_ASSERT_VALUES_EQUAL(result0.GetTopicDescription().GetRetentionPeriod().MilliSeconds(), retentionMs);
            UNIT_ASSERT_VALUES_EQUAL(result0.GetTopicDescription().GetRetentionStorageMb().value(), retentionBytes / (1024 * 1024));
            UNIT_ASSERT_VALUES_EQUAL(d.GetAttributes().at("_timestamp_type"), timestampType);
        }

        {
            // Set invalid timestamp type
            ui64 oldRetentionMs = 168 * 60 * 60 * 1000;
            ui64 oldRetentionBytes = 51'200 * 1_MB;

            ui64 newRetentionMs = 154 * 60 * 60 * 1000;
            ui64 newRetentionBytes = 40'000 * 1_MB;

            TString oldTimestampType = "LogAppendTime";
            TString newTimestampType = "Invalid";

            auto msg = client.AlterConfigs({
                    TTopicConfig(shortTopic0Name, 1, std::to_string(newRetentionMs), std::to_string(newRetentionBytes), {}, 1, newTimestampType)
            });

            UNIT_ASSERT_VALUES_EQUAL(msg->Responses.size(), 1);

            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].ResourceName.value(), shortTopic0Name);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].ErrorCode, INVALID_REQUEST);

            auto result0 = pqClient.DescribeTopic(shortTopic0Name, describeTopicSettings).GetValueSync();
            UNIT_ASSERT(result0.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(result0.GetTopicDescription().GetRetentionPeriod().MilliSeconds(), oldRetentionMs);
            UNIT_ASSERT_VALUES_EQUAL(result0.GetTopicDescription().GetRetentionStorageMb().value(), oldRetentionBytes / (1024 * 1024));
            UNIT_ASSERT_VALUES_EQUAL(result0.GetTopicDescription().GetAttributes().at("_timestamp_type"), oldTimestampType);
        }

        {
            // Wrong config value(retention.ms) isn't applied
            auto initialTopicDescription = pqClient.DescribeTopic(shortTopic0Name, describeTopicSettings)
                    .GetValueSync()
                    .GetTopicDescription();

            auto msg = client.AlterConfigs({ TTopicConfig(shortTopic0Name, 1, "not_a_number", "42")});

            UNIT_ASSERT_VALUES_EQUAL(msg->Responses.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].ResourceName.value(), shortTopic0Name);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].ErrorCode, INVALID_CONFIG);

            auto resultingTopicDescription = pqClient.DescribeTopic(shortTopic0Name, describeTopicSettings)
                    .GetValueSync()
                    .GetTopicDescription();

            UNIT_ASSERT_VALUES_EQUAL(
                    initialTopicDescription.GetRetentionPeriod().MilliSeconds(),
                    resultingTopicDescription.GetRetentionPeriod().MilliSeconds()
            );
            UNIT_ASSERT(
                initialTopicDescription.GetRetentionStorageMb() == resultingTopicDescription.GetRetentionStorageMb()
            );
        }

        {
            // Nonnumber retention.bytes
            auto alteredTopic = TTopicConfig(
                    shortTopic0Name,
                    1,
                    std::nullopt,
                    "notNumber"
            );
            auto msg = client.AlterConfigs({alteredTopic});
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].ResourceName.value(), shortTopic0Name);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].ErrorCode, INVALID_CONFIG);
        }

        {
            // Too big retention.ms
            auto alteredTopic = TTopicConfig(
                    shortTopic0Name,
                    1,
                    std::to_string(TDuration::Days(365).MilliSeconds()),
                    std::nullopt
            );
            auto msg = client.AlterConfigs({alteredTopic});
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].ResourceName.value(), shortTopic0Name);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].ErrorCode, INVALID_CONFIG);
        }

        {
            // Duplicate topics
            ui64 retentionMs = 168 * 60 * 60 * 1000;
            ui64 retentionBytes = 51'200 * 1_MB;

            auto msg = client.AlterConfigs({
                    TTopicConfig(shortTopic0Name, 1, std::to_string(retentionMs), std::to_string(retentionBytes)),
                    TTopicConfig(shortTopic0Name, 1, std::to_string(retentionMs), std::to_string(retentionBytes)),
            });

            UNIT_ASSERT_VALUES_EQUAL(msg->Responses.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].ResourceName.value(), shortTopic0Name);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].ErrorCode, INVALID_REQUEST);
        }

        {
            // Legal, but meaningless for Logbroker config
            std::map<TString, TString> configs { std::make_pair("flush.messages", "1") };
            auto msg = client.AlterConfigs({ TTopicConfig(shortTopic0Name, 1, std::nullopt, std::nullopt, configs) });

            UNIT_ASSERT_VALUES_EQUAL(msg->Responses.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].ResourceName.value(), shortTopic0Name);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].ErrorCode, NONE_ERROR);
        }

        {
            // Both legal and illegal configs
            std::map<TString, TString> configs { std::make_pair("compression.type", "zstd"), std::make_pair("flush.messages", "1") };
            auto msg = client.AlterConfigs({ TTopicConfig(shortTopic0Name, 1, std::nullopt, std::nullopt, configs) });

            UNIT_ASSERT_VALUES_EQUAL(msg->Responses.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].ResourceName.value(), shortTopic0Name);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].ErrorCode, INVALID_REQUEST);
        }

    }

    Y_UNIT_TEST(LoginWithApiKey) {
        TInsecureTestServer testServer;

        TString topicName = "/Root/topic-0-test";

        NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
        CreateTopic(pqClient, topicName, 10, {"consumer-0"});

        auto settings = NTopic::TReadSessionSettings()
                            .AppendTopics(NTopic::TTopicReadSettings(topicName))
                            .ConsumerName("consumer-0");
        auto topicReader = pqClient.CreateReadSession(settings);

        TKafkaTestClient client(testServer.Port);

        {
            client.PlainAuthenticateToKafka("@/Root", "ApiKey-value-valid");
        }

        Sleep(TDuration::Seconds(1));
    }

    Y_UNIT_TEST(LoginWithApiKeyWithoutAt) {
        TInsecureTestServer testServer;

        TString topicName = "/Root/topic-0-test";

        NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
        CreateTopic(pqClient, topicName, 10, {"consumer-0"});

        auto settings = NTopic::TReadSessionSettings()
                            .AppendTopics(NTopic::TTopicReadSettings(topicName))
                            .ConsumerName("consumer-0");
        auto topicReader = pqClient.CreateReadSession(settings);

        TKafkaTestClient client(testServer.Port);

        {
            client.PlainAuthenticateToKafka("/Root", "ApiKey-value-valid");
        }

        Sleep(TDuration::Seconds(1));
    } // LoginWithApiKeyWithoutAt

    Y_UNIT_TEST(MetadataScenario) {
        TInsecureTestServer testServer;
        TKafkaTestClient client(testServer.Port);

        auto metadataResponse = client.Metadata({});

        UNIT_ASSERT_VALUES_EQUAL(metadataResponse->ClusterId, "ydb-cluster");
        UNIT_ASSERT_VALUES_EQUAL(metadataResponse->ControllerId, testServer.KikimrServer->GetRuntime()->GetFirstNodeId());
        UNIT_ASSERT_VALUES_EQUAL(metadataResponse->Topics.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(metadataResponse->Brokers.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(metadataResponse->Brokers[0].NodeId, testServer.KikimrServer->GetRuntime()->GetFirstNodeId());
        UNIT_ASSERT_VALUES_EQUAL(metadataResponse->Brokers[0].Host, "::1");
        UNIT_ASSERT_VALUES_EQUAL(metadataResponse->Brokers[0].Port, testServer.Port);
    }

    Y_UNIT_TEST(MetadataInServerlessScenario) {
        TInsecureTestServer testServer("1", true);
        TKafkaTestClient client(testServer.Port);

        auto metadataResponse = client.Metadata({});

        UNIT_ASSERT_VALUES_EQUAL(metadataResponse->ClusterId, "ydb-cluster");
        UNIT_ASSERT_VALUES_EQUAL(metadataResponse->ControllerId, NKafka::ProxyNodeId);
        UNIT_ASSERT_VALUES_EQUAL(metadataResponse->Topics.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(metadataResponse->Brokers.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(metadataResponse->Brokers[0].NodeId, NKafka::ProxyNodeId);
        UNIT_ASSERT_VALUES_EQUAL(metadataResponse->Brokers[0].Host, "localhost");
        UNIT_ASSERT_VALUES_EQUAL(metadataResponse->Brokers[0].Port, FAKE_SERVERLESS_KAFKA_PROXY_PORT);
    }

    Y_UNIT_TEST(FetchCodecVisibilityInHeadersScenario) {
        TInsecureTestServer testServer("1");

        TString topicName = "test-topic";
        TString consumerName = "consumer1";

        NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
        CreateTopic(pqClient, topicName, 1, {consumerName});

        NYdb::NTopic::TWriteSessionSettings wsSettings;
        wsSettings.Path(topicName).ProducerId("12345").PartitionId(0);
        auto writer = pqClient.CreateSimpleBlockingWriteSession(wsSettings);

        std::vector<NTopic::ECodec> codecs = {NTopic::ECodec::RAW, NTopic::ECodec::GZIP,  NTopic::ECodec::LZOP, NTopic::ECodec::ZSTD, NTopic::ECodec::CUSTOM, static_cast<NTopic::ECodec>(888)};
        std::vector<TString> expectedCodecNames = {"RAW", "GZIP", "LZOP", "ZSTD", std::to_string(static_cast<TKafkaUint32>(NTopic::ECodec::CUSTOM)), "888"};
        for (size_t i = 0; i < codecs.size(); i++) {
            TString messageData = "Data" + std::to_string(i);
            NYdb::NTopic::TWriteMessage msg = NYdb::NTopic::TWriteMessage::CompressedMessage(messageData, codecs[i], messageData.size());
            writer->Write(std::move(msg));
        }
        writer->Close();

        TKafkaTestClient kafkaClient(testServer.Port);
        auto fetchResponse = kafkaClient.Fetch({{topicName, {0}}});
        UNIT_ASSERT_VALUES_EQUAL(fetchResponse->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        UNIT_ASSERT_VALUES_EQUAL(fetchResponse->Responses.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fetchResponse->Responses[0].Partitions[0].Records->Records.size(), codecs.size());
        for (size_t i = 0; i < fetchResponse->Responses[0].Partitions[0].Records->Records.size(); i++) {
            auto& record = fetchResponse->Responses[0].Partitions[0].Records->Records[i];
            UNIT_ASSERT_VALUES_EQUAL(record.Headers.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(TString(record.Headers[0].Key.value().data(), record.Headers[0].Key.value().size()), "__codec");
            UNIT_ASSERT_VALUES_EQUAL(TString(record.Headers[0].Value.value().data(), record.Headers[0].Value.value().size()), expectedCodecNames[i]);
        }
    }

    Y_UNIT_TEST(OffsetFetchConsumerAutocreationScenario) {
        TInsecureTestServer testServer("2", false, true, false, true, true, true);

        TString nonExistedTopicName = "non-existent-topic";
        TString existedTopicName = "existent-topic";
        TString consumerName = "my-consumer";
        TString newConsumer1 = "new-consumer-1";
        TString newConsumer2 = "new-consumer-2";

        {
            NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
            CreateTopic(pqClient, existedTopicName, 3, {consumerName});
        }

        {
            TKafkaTestClient client(testServer.Port);
            // authenticating as user with read and write rights
            TString userName = "user123@/Root";
            TString userPassword = "UsErPassword";
            client.PlainAuthenticateToKafka(userName, userPassword);
            {
                // checking consumer autocreation for existing topic
                std::map<TString, std::vector<i32>> topicsToPartions;
                topicsToPartions[existedTopicName] = std::vector<i32>{0, 1};
                auto msg = client.OffsetFetch(newConsumer1, topicsToPartions);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].ErrorCode, 0);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Name, existedTopicName);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions.size(), 2);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions[0].PartitionIndex, 0);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions[0].ErrorCode, EKafkaErrors::NONE_ERROR);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions[1].PartitionIndex, 1);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions[1].ErrorCode, EKafkaErrors::NONE_ERROR);
            }

            {
                // non-existent topic with non-existent consumer should return an error for unexistent topic
                std::map<TString, std::vector<i32>> topicsToPartions;
                topicsToPartions[nonExistedTopicName] = std::vector<i32>{0};
                auto msg = client.OffsetFetch(newConsumer1, topicsToPartions);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].ErrorCode, 0);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Name, nonExistedTopicName);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions[0].PartitionIndex, 0);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions[0].ErrorCode, EKafkaErrors::UNKNOWN_TOPIC_OR_PARTITION);
            }
        }
        {
            // authenticating as user with read and write rights
            TKafkaTestClient client(testServer.Port);
            TString userName = "usernorights@/Root";
            TString userPassword = "dummyPass";
            client.PlainAuthenticateToKafka(userName, userPassword);
            {
                // checking that consumer autocreation for existing topic for user with no rights
                // returns authorisation error
                std::map<TString, std::vector<i32>> topicsToPartions;
                topicsToPartions[existedTopicName] = std::vector<i32>{0, 1};
                auto msg = client.OffsetFetch(newConsumer2, topicsToPartions);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].ErrorCode, 0);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Name, existedTopicName);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions.size(), 2);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions[0].PartitionIndex, 0);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions[0].ErrorCode, EKafkaErrors::TOPIC_AUTHORIZATION_FAILED);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions[1].PartitionIndex, 1);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions[1].ErrorCode, EKafkaErrors::TOPIC_AUTHORIZATION_FAILED);
            }

            {
                // checking that offset fetch for user with no rights returns error
                std::map<TString, std::vector<i32>> topicsToPartions;
                topicsToPartions[existedTopicName] = std::vector<i32>{0, 1};
                auto msg = client.OffsetFetch(newConsumer1, topicsToPartions);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].ErrorCode, 0);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Name, existedTopicName);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions.size(), 2);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions[0].PartitionIndex, 0);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions[0].ErrorCode, EKafkaErrors::TOPIC_AUTHORIZATION_FAILED);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions[1].PartitionIndex, 1);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions[1].ErrorCode, EKafkaErrors::TOPIC_AUTHORIZATION_FAILED);
            }
        }
    }

    Y_UNIT_TEST(OffsetFetchTopicConsumerAutocreationScenario) {
        TInsecureTestServer testServer("1", false, false, true, true);

        TString nonExistedTopicName1 = "non-existent-topic-1";
        TString nonExistedTopicName2 = "non-existent-topic-2";
        TString nonExistedTopicName3 = "non-existent-topic-3";
        TString existedTopicName = "existent-topic";
        TString consumerName = "my-consumer";
        TString newConsumer1 = "new-consumer-1";
        TString newConsumer2 = "new-consumer-2";
        TString newConsumer3 = "new-consumer-3";

        ui32 defaultPartitionsCount = testServer.KikimrServer.get()->ServerSettings->AppConfig->GetKafkaProxyConfig().GetTopicCreationDefaultPartitions();

        {
            NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
            CreateTopic(pqClient, existedTopicName, 3, {consumerName});
        }

        { TKafkaTestClient client(testServer.Port);
            // authenticating as user with read and write rights
            TString userName = "user123@/Root";
            TString userPassword = "UsErPassword";
            client.PlainAuthenticateToKafka(userName, userPassword);
            {
                // existent topic with existent consumer
                std::map<TString, std::vector<i32>> topicsToPartions;
                topicsToPartions[existedTopicName] = std::vector<i32>{0, 1, 2};
                auto msg = client.OffsetFetch(consumerName, topicsToPartions);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].ErrorCode, 0);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Name, existedTopicName);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions.size(), 3);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions[0].PartitionIndex, 0);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions[0].ErrorCode, EKafkaErrors::NONE_ERROR);
            }

            {
                // existent topic with non-existent consumer
                std::map<TString, std::vector<i32>> topicsToPartions;
                topicsToPartions[existedTopicName] = std::vector<i32>{0, 1, 2};
                auto msg = client.OffsetFetch(newConsumer1, topicsToPartions);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].ErrorCode, 0);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Name, existedTopicName);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions.size(), 3);
                for (size_t i = 0; i < msg->Groups[0].Topics[0].Partitions.size(); i++) {
                    UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions[0].PartitionIndex, 0);
                    UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions[0].ErrorCode, EKafkaErrors::NONE_ERROR);
                }
            }

            {
                // existent topic with existent consumer but non-existent partition should return an error
                std::map<TString, std::vector<i32>> topicsToPartions;
                i32 tooBigpartitionIndex = 100;
                topicsToPartions[existedTopicName] = std::vector<i32>{0, tooBigpartitionIndex};
                auto msg = client.OffsetFetch(newConsumer1, topicsToPartions);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].ErrorCode, 0);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Name, existedTopicName);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions.size(), 2);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions[0].PartitionIndex, 0);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions[0].ErrorCode, EKafkaErrors::NONE_ERROR);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions[1].PartitionIndex, tooBigpartitionIndex);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions[1].ErrorCode, EKafkaErrors::RESOURCE_NOT_FOUND);
            }

            {
                // existent topic with existent consumer but non-existent partition should return an error
                std::map<TString, std::vector<i32>> topicsToPartions;
                i32 tooBigpartitionIndex = 100;
                topicsToPartions[existedTopicName] = std::vector<i32>{tooBigpartitionIndex};
                auto msg = client.OffsetFetch(newConsumer1, topicsToPartions);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].ErrorCode, 0);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Name, existedTopicName);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions[0].PartitionIndex, tooBigpartitionIndex);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions[0].ErrorCode, EKafkaErrors::RESOURCE_NOT_FOUND);
            }

            {
                // check that if we request one topic several times, no error is caused by it
                TString newTopic = "completely-new-topic";
                TOffsetFetchRequestData request;
                TOffsetFetchRequestData::TOffsetFetchRequestGroup group;
                group.GroupId = consumerName;
                TOffsetFetchRequestData::TOffsetFetchRequestGroup::TOffsetFetchRequestTopics topic1;
                topic1.Name = newTopic;
                topic1.PartitionIndexes = std::vector<i32>{1000, 0};

                TOffsetFetchRequestData::TOffsetFetchRequestGroup::TOffsetFetchRequestTopics topic2;
                topic2.Name = newTopic;
                topic2.PartitionIndexes = std::vector<i32>{1, 2, 0};

                TOffsetFetchRequestData::TOffsetFetchRequestGroup::TOffsetFetchRequestTopics topic3;
                topic3.Name = newTopic;
                topic3.PartitionIndexes = std::vector<i32>{100, 0};

                group.Topics.push_back(topic1);
                group.Topics.push_back(topic2);
                group.Topics.push_back(topic3);
                request.Groups.push_back(group);
                auto msg = client.OffsetFetch(request);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics.size(), 3);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions[0].ErrorCode, EKafkaErrors::RESOURCE_NOT_FOUND);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions[1].ErrorCode, EKafkaErrors::NONE_ERROR);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[1].Partitions[0].ErrorCode, EKafkaErrors::NONE_ERROR);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[1].Partitions[1].ErrorCode, EKafkaErrors::RESOURCE_NOT_FOUND);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[1].Partitions[2].ErrorCode, EKafkaErrors::NONE_ERROR);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[2].Partitions[0].ErrorCode, EKafkaErrors::RESOURCE_NOT_FOUND);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[2].Partitions[1].ErrorCode, EKafkaErrors::NONE_ERROR);
            }


            {
                // existent topic with non-existent consumer and non-existent partition should return an error
                std::map<TString, std::vector<i32>> topicsToPartions;
                i32 tooBigpartitionIndex = 100;
                topicsToPartions[existedTopicName] = std::vector<i32>{tooBigpartitionIndex};
                auto msg = client.OffsetFetch(newConsumer2, topicsToPartions);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].ErrorCode, 0);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Name, existedTopicName);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].ErrorCode, EKafkaErrors::NONE_ERROR);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions[0].PartitionIndex, tooBigpartitionIndex);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions[0].ErrorCode, EKafkaErrors::RESOURCE_NOT_FOUND);
            }

            {
                // an offset fetch request for both existent topic and non-existent topic
                std::map<TString, std::vector<i32>> topicsToPartions;
                topicsToPartions[existedTopicName] = std::vector<i32>{0, 1, 2};
                topicsToPartions[nonExistedTopicName1] = std::vector<i32>{0, 1};
                auto msg = client.OffsetFetch(newConsumer1, topicsToPartions);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].ErrorCode, 0);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics.size(), 2);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Name, existedTopicName);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions.size(), 3);
                for (const auto& partition : msg->Groups[0].Topics[0].Partitions) {
                    UNIT_ASSERT_VALUES_EQUAL(partition.ErrorCode, EKafkaErrors::NONE_ERROR);
                }
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[1].Name, nonExistedTopicName1);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[1].Partitions.size(), defaultPartitionsCount);
                for (const auto& partition : msg->Groups[0].Topics[1].Partitions) {
                    UNIT_ASSERT_VALUES_EQUAL(partition.ErrorCode, EKafkaErrors::NONE_ERROR);
                }
            }

            {
                // non-existent topic with non-existent consumer
                std::map<TString, std::vector<i32>> topicsToPartions;
                topicsToPartions[nonExistedTopicName2] = std::vector<i32>{0};
                auto msg = client.OffsetFetch(newConsumer1, topicsToPartions);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].ErrorCode, 0);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Name, nonExistedTopicName2);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions[0].PartitionIndex, 0);
                UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions[0].ErrorCode, EKafkaErrors::NONE_ERROR);
            }
        }

        {
            TKafkaTestClient client(testServer.Port);
            // authenticating as a user with no write rights
            TString userName = "usernorights@/Root";
            TString userPassword = "dummyPass";
            client.PlainAuthenticateToKafka(userName, userPassword);

            std::map<TString, std::vector<i32>> topicsToPartions;
            topicsToPartions[nonExistedTopicName3] = std::vector<i32>{0};
            auto msg = client.OffsetFetch(newConsumer1, topicsToPartions);
            UNIT_ASSERT_VALUES_EQUAL(msg->Groups.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions[0].ErrorCode, EKafkaErrors::UNKNOWN_TOPIC_OR_PARTITION);
        }

        {
            TKafkaTestClient client(testServer.Port);
            // authenticating as a user with only read rights
            TString userName = "useronlyreadrights@/Root";
            TString userPassword = "AbAcAbA";
            client.PlainAuthenticateToKafka(userName, userPassword);

            std::map<TString, std::vector<i32>> topicsToPartions;
            topicsToPartions[nonExistedTopicName3] = std::vector<i32>{0};
            auto msg = client.OffsetFetch(newConsumer1, topicsToPartions);
            UNIT_ASSERT_VALUES_EQUAL(msg->Groups.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions[0].ErrorCode, EKafkaErrors::UNKNOWN_TOPIC_OR_PARTITION);

            std::map<TString, std::vector<i32>> topicsToPartions1;
            topicsToPartions1[existedTopicName] = std::vector<i32>{0};
            auto msg1 = client.OffsetFetch(newConsumer3, topicsToPartions1);
            UNIT_ASSERT_VALUES_EQUAL(msg1->Groups.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg1->Groups[0].Topics.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg1->Groups[0].Topics[0].Partitions[0].ErrorCode, EKafkaErrors::RESOURCE_NOT_FOUND);


            NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
            auto describeTopicSettings = NTopic::TDescribeTopicSettings().IncludeStats(true);
            auto result = pqClient.DescribeTopic(existedTopicName, describeTopicSettings).GetValueSync();
            UNIT_ASSERT(result.IsSuccess());
            const auto& consumers = result.GetTopicDescription().GetConsumers();
            UNIT_ASSERT_EQUAL(consumers.size(), 3);
            std::vector<TString> topicConsumerNames;
            for (const  NYdb::NTopic::TConsumer& consumer : consumers) {
                topicConsumerNames.push_back(TString(consumer.GetConsumerName()));
            }
            UNIT_ASSERT(std::find(topicConsumerNames.begin(), topicConsumerNames.end(), newConsumer3) == topicConsumerNames.end());
        }
    }

    Y_UNIT_TEST(MetadataTopicAutocreationEnabledScenario) {
        TInsecureTestServer testServer("1", false, false, true, true);

        TString nonExistedTopicName1 = "non-existent-topic-1";
        TString nonExistedTopicName2 = "non-existent-topic-2";
        TString existedTopicName = "existent-topic";
        TString consumerName = "my-consumer";
        ui32 existedTopicPartitionsNum = 3;

        ui32 defaultPartitionsCount = testServer.KikimrServer.get()->ServerSettings->AppConfig->GetKafkaProxyConfig().GetTopicCreationDefaultPartitions();

        {
            NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
            CreateTopic(pqClient, existedTopicName, existedTopicPartitionsNum, {consumerName});
        }

        {
            TKafkaTestClient client(testServer.Port);
            TString userName = "user123@/Root";
            TString userPassword = "UsErPassword";
            client.PlainAuthenticateToKafka(userName, userPassword);
            TVector<TString> nonExistentTopics = {nonExistedTopicName1};
            auto msg = client.Metadata(nonExistentTopics);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].Partitions.size(), defaultPartitionsCount);
            for (const auto &partitionInfo : msg->Topics[0].Partitions) {
                UNIT_ASSERT_VALUES_EQUAL(partitionInfo.ErrorCode, EKafkaErrors::NONE_ERROR);
            }
        }

        {
            // check that if user has only read rights, topic is not created
            TKafkaTestClient client(testServer.Port);
            TString userName = "useronlyreadrights@/Root";
            TString userPassword = "AbAcAbA";
            client.PlainAuthenticateToKafka(userName, userPassword);
            TVector<TString> nonExistentTopic = {nonExistedTopicName2};
            auto msg = client.Metadata(nonExistentTopic);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].Partitions.size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].ErrorCode, EKafkaErrors::TOPIC_AUTHORIZATION_FAILED);
        }

        {
            // check that if user has no rights, topic is not created
            TKafkaTestClient client(testServer.Port);
            TString userName = "usernorights@/Root";
            TString userPassword = "dummyPass";
            client.PlainAuthenticateToKafka(userName, userPassword);
            TVector<TString> nonExistentTopic = {nonExistedTopicName2};
            auto msg = client.Metadata(nonExistentTopic);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].Partitions.size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].ErrorCode, EKafkaErrors::TOPIC_AUTHORIZATION_FAILED);
        }

        {
            TKafkaTestClient client(testServer.Port);
            TString userName = "user123@/Root";
            TString userPassword = "UsErPassword";
            client.PlainAuthenticateToKafka(userName, userPassword);
            TVector<TString> nonExistentTopics = {nonExistedTopicName2, existedTopicName};
            auto msg = client.Metadata(nonExistentTopics);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].Partitions.size(), defaultPartitionsCount);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[1].Partitions.size(), existedTopicPartitionsNum);
            for (int i = 0; i < 2; i++) {
                for (const auto &partitionInfo : msg->Topics[0].Partitions) {
                    UNIT_ASSERT_VALUES_EQUAL(partitionInfo.ErrorCode, EKafkaErrors::NONE_ERROR);
                }
            }
        }

    }

    Y_UNIT_TEST(DescribeGroupsScenario) {
        TInsecureTestServer testServer("1", false, true);

        TString topicName = "/Root/topic-0";
        ui64 totalPartitions = 24;
        TString groupId1 = "consumer-0";
        TString groupId2 = "consumer-1";
        TString groupId3 = "consumer-2";

        TString protocolType = "consumer";
        TString protocolName = "range";

        TKafkaTestClient clientA(testServer.Port, "ClientA");
        TKafkaTestClient clientB(testServer.Port, "ClientB");
        TKafkaTestClient clientC(testServer.Port, "ClientC");

        // Checking that DescribeGroups method works correctly if tables have not been inited yet

        std::vector<std::optional<TString>> requestedGroups;
        requestedGroups.push_back(groupId1);
        auto response0 = clientA.DescribeGroups(requestedGroups);

        UNIT_ASSERT_VALUES_EQUAL(response0->Groups.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response0->Groups[0].GroupId, groupId1);
        UNIT_ASSERT_VALUES_EQUAL(response0->Groups[0].Members.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(response0->Groups[0].ErrorCode, (TKafkaInt16)EKafkaErrors::GROUP_ID_NOT_FOUND);

        // Creating 3 group members. One member of group "consumer-0" and two members of group "consumer-1"

        {
            NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
            auto result = pqClient
                .CreateTopic(
                    topicName,
                    NYdb::NTopic::TCreateTopicSettings()
                        .PartitioningSettings(totalPartitions, 100)
                        .BeginAddConsumer(groupId1).EndAddConsumer()
                        .BeginAddConsumer(groupId2).EndAddConsumer()
                )
                .ExtractValueSync();
            UNIT_ASSERT_C(
                result.IsSuccess(),
                "CreateTopic failed, issues: " << result.GetIssues().ToString()
            );
        }



        std::vector<TString> topics = {topicName};
        i32 heartbeatTimeout = 15000;
        i32 rebalanceTimeout = 5000;

        TRequestHeaderData headerAJoin = clientA.Header(NKafka::EApiKey::JOIN_GROUP, 9);
        TRequestHeaderData headerBJoin = clientB.Header(NKafka::EApiKey::JOIN_GROUP, 9);
        TRequestHeaderData headerCJoin = clientC.Header(NKafka::EApiKey::JOIN_GROUP, 9);

        TJoinGroupRequestData joinReq1;
        joinReq1.GroupId = groupId1;
        joinReq1.ProtocolType = protocolType;
        joinReq1.SessionTimeoutMs = heartbeatTimeout;
        joinReq1.RebalanceTimeoutMs = rebalanceTimeout;

        NKafka::TJoinGroupRequestData::TJoinGroupRequestProtocol protocol;
        protocol.Name = protocolName;

        TConsumerProtocolSubscription subscribtion;
        for (auto& topic : topics) {
            subscribtion.Topics.push_back(topic);
        }
        TKafkaVersion version = 3;
        TWritableBuf buf(nullptr, subscribtion.Size(version) + sizeof(version));
        TKafkaWritable writable(buf);
        writable << version;
        subscribtion.Write(writable, version);
        protocol.Metadata = TKafkaRawBytes(buf.GetFrontBuffer().data(), buf.GetFrontBuffer().size());

        joinReq1.Protocols.push_back(protocol);

        TJoinGroupRequestData joinReqA = joinReq1;
        joinReqA.GroupInstanceId = "instanceA";

        TJoinGroupRequestData joinReq2 = joinReq1;
        joinReq2.GroupId = groupId2;

        TJoinGroupRequestData joinReqB = joinReq2;
        joinReqB.GroupInstanceId = "instanceB";

        TJoinGroupRequestData joinReqC = joinReq2;
        joinReqC.GroupInstanceId = "instanceC";

        clientA.WriteToSocket(headerAJoin, joinReqA);
        clientB.WriteToSocket(headerBJoin, joinReqB);
        clientC.WriteToSocket(headerCJoin, joinReqC);

        auto joinRespA = clientA.ReadResponse<TJoinGroupResponseData>(headerAJoin);
        auto joinRespB = clientB.ReadResponse<TJoinGroupResponseData>(headerBJoin);
        auto joinRespC = clientC.ReadResponse<TJoinGroupResponseData>(headerCJoin);

        UNIT_ASSERT_VALUES_EQUAL(joinRespA->ErrorCode, (TKafkaInt16)EKafkaErrors::NONE_ERROR);
        UNIT_ASSERT_VALUES_EQUAL(joinRespB->ErrorCode, (TKafkaInt16)EKafkaErrors::NONE_ERROR);
        UNIT_ASSERT_VALUES_EQUAL(joinRespC->ErrorCode, (TKafkaInt16)EKafkaErrors::NONE_ERROR);

        // check that DescribeGroups information is returned correctly when one group is requested

        auto response1 = clientA.DescribeGroups(requestedGroups);
        UNIT_ASSERT_VALUES_EQUAL(response1->Groups.size(), 1);
        auto& groupResponse = response1->Groups[0];
        UNIT_ASSERT(groupResponse.GroupId.has_value());
        UNIT_ASSERT_VALUES_EQUAL(*groupResponse.GroupId, groupId1);
        UNIT_ASSERT_VALUES_EQUAL(groupResponse.Members.size(), 1);

        // check that for two existing requested groups DescribeGroups returns correct member information
        // and for one unexisting requested group the returned response constains error

        requestedGroups.push_back(groupId2);
        requestedGroups.push_back(groupId3);
        auto response2 = clientA.DescribeGroups(requestedGroups);
        UNIT_ASSERT_VALUES_EQUAL(response2->Groups.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(response2->Groups[0].GroupId, groupId1);
        UNIT_ASSERT_VALUES_EQUAL(response2->Groups[0].Members.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response2->Groups[0].Members[0].MemberId, joinRespA->MemberId);
        UNIT_ASSERT_VALUES_EQUAL(response2->Groups[0].ErrorCode, (TKafkaInt16)EKafkaErrors::NONE_ERROR);
        UNIT_ASSERT_VALUES_EQUAL(response2->Groups[1].Members.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(response2->Groups[1].GroupId, groupId2);
        UNIT_ASSERT_VALUES_EQUAL(response2->Groups[1].ErrorCode, (TKafkaInt16)EKafkaErrors::NONE_ERROR);
        UNIT_ASSERT_VALUES_EQUAL(response2->Groups[2].GroupId, groupId3);
        UNIT_ASSERT_VALUES_EQUAL(response2->Groups[2].Members.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(response2->Groups[2].ErrorCode, (TKafkaInt16)EKafkaErrors::GROUP_ID_NOT_FOUND);

        ui32 memberIdBCount = 0;
        ui32 memberIdCCount = 0;
        ui32 wrongMemberIdCount = 0;
        for (auto& member : response2->Groups[1].Members) {
            if (member.MemberId == joinRespB->MemberId) {
                memberIdBCount += 1;
            } else if (member.MemberId == joinRespC->MemberId) {
                memberIdCCount += 1;
            } else {
                wrongMemberIdCount += 1;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(memberIdBCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(memberIdCCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(wrongMemberIdCount, 0);
    }

    Y_UNIT_TEST(ListGroupsScenario) {
        TInsecureTestServer testServer("1", false, true);
        TString groupId1 = "consumer-0";
        TString groupId2 = "consumer-1";
        TString topicName = "/Root/topic-0";
        ui64 totalPartitions = 24;
        TString protocolType = "consumer";
        TString protocolName = "range";

        TKafkaTestClient clientA(testServer.Port, "ClientA");
        TKafkaTestClient clientB(testServer.Port, "ClientB");

        // check that ListGroups doesn't fail if tables have not been inited yet

        std::vector<std::optional<TString>> statesFilter = {"PreparingRebalance"};
        auto responseBeforeTablesInit = clientA.ListGroups(statesFilter);
        UNIT_ASSERT_VALUES_EQUAL(responseBeforeTablesInit->Groups.size(), 0);

        {
            NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
            auto result = pqClient
                .CreateTopic(
                    topicName,
                    NYdb::NTopic::TCreateTopicSettings()
                        .PartitioningSettings(totalPartitions, 100)
                        .BeginAddConsumer(groupId1).EndAddConsumer()
                        .BeginAddConsumer(groupId2).EndAddConsumer()
                )
                .ExtractValueSync();
            UNIT_ASSERT_C(
                result.IsSuccess(),
                "CreateTopic failed, issues: " << result.GetIssues().ToString()
            );
        }


        // check that before adding any consumers response will contain no groups

        TListGroupsRequestData requestGroups;
        auto responseEmpty = clientA.ListGroups(requestGroups);
        Cout << "Received TListGroupsRequestData with " << responseEmpty->Groups.size() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(responseEmpty->Groups.size(), 0);

        std::vector<TString> topics = {topicName};
        i32 heartbeatTimeout = 15000;

        auto joinRespA = clientA.JoinAndSyncGroupAndWaitPartitions(topics, groupId1, totalPartitions, protocolName, totalPartitions, heartbeatTimeout);
        auto joinRespB = clientB.JoinAndSyncGroupAndWaitPartitions(topics, groupId2, totalPartitions, protocolName, totalPartitions, heartbeatTimeout);

        // check that after two consumers have joined to two groups, they will be returned with correct status

        auto response = clientA.ListGroups(requestGroups);

        Cout << "Received TListGroupsRequestData with " << response->Groups.size() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(response->Groups.size(), 2);
        ui32 first_group_count = 0;
        ui32 second_group_count = 0;

        // check that all metadata is correct and groups are in "preparing rebalance" state
        for (auto group : response->Groups) {
            UNIT_ASSERT_C(group.GroupId.has_value(),"Error, no groupId received");
            UNIT_ASSERT_C(group.GroupState.has_value(),"Error, no GroupState received");
            UNIT_ASSERT_C(group.ProtocolType.has_value(),"Error, no ProtocolType received");
            UNIT_ASSERT_C(*group.GroupId == groupId1 || *group.GroupId == groupId2,"Error, wrong GroupId name" << group.GroupId);

            if (*group.GroupId == groupId1) {
                first_group_count += 1;
            } else if (*group.GroupId == groupId2) {
                second_group_count += 1;
            }

            UNIT_ASSERT_VALUES_EQUAL(*group.GroupState, "CompletingRebalance");
            UNIT_ASSERT_VALUES_EQUAL(*group.ProtocolType, protocolType);

            Cout << "********" << Endl;
            Cout << "GroupId: " << *group.GroupId << Endl;
            Cout << "GroupState: " << *group.GroupState << Endl;
            Cout << "ProtocolType: " << *group.ProtocolType  << Endl;

        }
        UNIT_ASSERT_VALUES_EQUAL(first_group_count, 1);
        UNIT_ASSERT_VALUES_EQUAL(second_group_count, 1);


        // now we want to check that after calling JoinGroup() Group2 will be in state of "Preparing Rebalance"
        // because another consumer has joined Group2 recently

        clientA.JoinGroup(topics, groupId2, protocolName, heartbeatTimeout);

        TListGroupsRequestData requestGroups1;
        auto response1 = clientB.ListGroups(requestGroups1);
        Cout << "Received TListGroupsRequestData with " << response1->Groups.size() << Endl;

        first_group_count = 0;
        second_group_count = 0;
        for (auto group : response1->Groups) {
            UNIT_ASSERT_C(group.GroupId.has_value(),"Error, no groupId received");
            UNIT_ASSERT_C(group.GroupState.has_value(),"Error, no GroupState received");
            UNIT_ASSERT_C(group.ProtocolType.has_value(),"Error, no ProtocolType received");
            UNIT_ASSERT_C(*group.GroupId == groupId1 || *group.GroupId == groupId2, "Error, wrong GroupId name" << group.GroupId);

            if (*group.GroupId == groupId1) {
                first_group_count += 1;
                UNIT_ASSERT_VALUES_EQUAL(*group.GroupState, "CompletingRebalance");
            } else if (*group.GroupId == groupId2) {
                second_group_count += 1;
                UNIT_ASSERT_VALUES_EQUAL(*group.GroupState, "PreparingRebalance");
            }
            UNIT_ASSERT_VALUES_EQUAL(*group.ProtocolType, protocolType);

            Cout << "********" << Endl;
            Cout << "GroupId: " <<  *group.GroupId  << Endl;
            Cout << "GroupState: " <<  *group.GroupState  << Endl;
            Cout << "ProtocolType: " <<  *group.ProtocolType  << Endl;
        }
        UNIT_ASSERT_VALUES_EQUAL(first_group_count, 1);
        UNIT_ASSERT_VALUES_EQUAL(second_group_count, 1);


        // now we want to check that if StatesFilter is filled in TListGroupsRequestData
        // than only consumers of certain states from StatesFilter are returned

        TListGroupsRequestData requestGroupsStateFilter;
        requestGroupsStateFilter.StatesFilter.push_back("PreparingRebalance");
        auto responseStateFilter = clientA.ListGroups(requestGroupsStateFilter);

        first_group_count = 0;
        second_group_count = 0;
        UNIT_ASSERT_VALUES_EQUAL(responseStateFilter->Groups.size(), 1);
        for (auto group : responseStateFilter->Groups) {
            UNIT_ASSERT_C(group.GroupId.has_value(),"Error, no groupId received");
            UNIT_ASSERT_C(group.GroupState.has_value(),"Error, no GroupState received");
            UNIT_ASSERT_C(group.ProtocolType.has_value(),"Error, no ProtocolType received");
            UNIT_ASSERT_C(*group.GroupId == groupId1 || *group.GroupId == groupId2,"Error, wrong GroupId name" << group.GroupId);
            UNIT_ASSERT_VALUES_EQUAL(*group.GroupId, groupId2);
            UNIT_ASSERT_VALUES_EQUAL(*group.GroupState, "PreparingRebalance");
            UNIT_ASSERT_VALUES_EQUAL(*group.ProtocolType, protocolType);

            Cout << "********" << Endl;
            Cout << "GroupId: " << *group.GroupId << Endl;
            Cout << "GroupState: " << *group.GroupState << Endl;
            Cout << "ProtocolType: " << *group.ProtocolType  << Endl;
        }
    }

    Y_UNIT_TEST(NativeKafkaBalanceScenario) {
        TInsecureTestServer testServer("1", false, true);

        TString topicName = "/Root/topic-0";
        ui64 totalPartitions = 24;
        TString groupId = "consumer-0";

        TString protocolType = "consumer";
        TString protocolName = "range";

        {
            NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
            auto result = pqClient
                .CreateTopic(
                    topicName,
                    NYdb::NTopic::TCreateTopicSettings()
                        .PartitioningSettings(totalPartitions, 100)
                        .BeginAddConsumer(groupId).EndAddConsumer()
                )
                .ExtractValueSync();
            UNIT_ASSERT_C(
                result.IsSuccess(),
                "CreateTopic failed, issues: " << result.GetIssues().ToString()
            );
        }

        TKafkaTestClient clientA(testServer.Port, "ClientA");
        TKafkaTestClient clientB(testServer.Port, "ClientB");
        TKafkaTestClient clientC(testServer.Port, "ClientC");

        {
            auto rA = clientA.ApiVersions();
            auto rB = clientB.ApiVersions();
            auto rC = clientC.ApiVersions();
            UNIT_ASSERT_VALUES_EQUAL(rA->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT_VALUES_EQUAL(rB->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT_VALUES_EQUAL(rC->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        }
        {
            auto rA = clientA.SaslHandshake("PLAIN");
            auto rB = clientB.SaslHandshake("PLAIN");
            auto rC = clientC.SaslHandshake("PLAIN");
            UNIT_ASSERT_VALUES_EQUAL(rA->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT_VALUES_EQUAL(rB->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT_VALUES_EQUAL(rC->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        }
        {
            TString user = "ouruser@/Root";
            TString pass = "ourUserPassword";
            auto rA = clientA.SaslPlainAuthenticate(user, pass);
            auto rB = clientB.SaslPlainAuthenticate(user, pass);
            auto rC = clientC.SaslPlainAuthenticate(user, pass);
            UNIT_ASSERT_VALUES_EQUAL(rA->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT_VALUES_EQUAL(rB->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT_VALUES_EQUAL(rC->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        }

        std::vector<TString> topics = {topicName};
        i32 heartbeatTimeout = 15000;
        i32 rebalanceTimeout = 5000;

        // CHECK THREE READERS GETS 1/3 OF PARTITIONS

        TRequestHeaderData headerAJoin = clientA.Header(NKafka::EApiKey::JOIN_GROUP, 9);
        TRequestHeaderData headerBJoin = clientB.Header(NKafka::EApiKey::JOIN_GROUP, 9);
        TRequestHeaderData headerCJoin = clientC.Header(NKafka::EApiKey::JOIN_GROUP, 9);

        TJoinGroupRequestData joinReq;
        joinReq.GroupId = groupId;
        joinReq.ProtocolType = protocolType;
        joinReq.SessionTimeoutMs = heartbeatTimeout;
        joinReq.RebalanceTimeoutMs = rebalanceTimeout;

        NKafka::TJoinGroupRequestData::TJoinGroupRequestProtocol protocol;
        protocol.Name = protocolName;

        TConsumerProtocolSubscription subscribtion;
        for (auto& topic : topics) {
            subscribtion.Topics.push_back(topic);
        }
        TKafkaVersion version = 3;
        TWritableBuf buf(nullptr, subscribtion.Size(version) + sizeof(version));
        TKafkaWritable writable(buf);
        writable << version;
        subscribtion.Write(writable, version);
        protocol.Metadata = TKafkaRawBytes(buf.GetFrontBuffer().data(), buf.GetFrontBuffer().size());

        joinReq.Protocols.push_back(protocol);

        TJoinGroupRequestData joinReqA = joinReq;
        joinReqA.GroupInstanceId = "instanceA";
        TJoinGroupRequestData joinReqB = joinReq;
        joinReqB.GroupInstanceId = "instanceB";
        TJoinGroupRequestData joinReqC = joinReq;
        joinReqC.GroupInstanceId = "instanceC";

        clientA.WriteToSocket(headerAJoin, joinReqA);
        clientB.WriteToSocket(headerBJoin, joinReqB);
        clientC.WriteToSocket(headerCJoin, joinReqC);

        auto joinRespA = clientA.ReadResponse<TJoinGroupResponseData>(headerAJoin);
        auto joinRespB = clientB.ReadResponse<TJoinGroupResponseData>(headerBJoin);
        auto joinRespC = clientC.ReadResponse<TJoinGroupResponseData>(headerCJoin);

        UNIT_ASSERT_VALUES_EQUAL(joinRespA->ErrorCode, (TKafkaInt16)EKafkaErrors::NONE_ERROR);
        UNIT_ASSERT_VALUES_EQUAL(joinRespB->ErrorCode, (TKafkaInt16)EKafkaErrors::NONE_ERROR);
        UNIT_ASSERT_VALUES_EQUAL(joinRespC->ErrorCode, (TKafkaInt16)EKafkaErrors::NONE_ERROR);

        bool isLeaderA = (joinRespA->Leader == joinRespA->MemberId);
        bool isLeaderB = (joinRespB->Leader == joinRespB->MemberId);

        TMessagePtr<TJoinGroupResponseData> leaderResp = isLeaderA ? joinRespA
                                    : isLeaderB ? joinRespB
                                    : joinRespC;

        // anyclient can make MakeRangeAssignment request, cause result does not depend on the client
        std::vector<TSyncGroupRequestData::TSyncGroupRequestAssignment> assignments = clientA.MakeRangeAssignment(leaderResp, totalPartitions);

        TRequestHeaderData syncHeaderA = clientA.Header(NKafka::EApiKey::SYNC_GROUP, 5);
        TRequestHeaderData syncHeaderB = clientB.Header(NKafka::EApiKey::SYNC_GROUP, 5);
        TRequestHeaderData syncHeaderC = clientC.Header(NKafka::EApiKey::SYNC_GROUP, 5);

        TSyncGroupRequestData syncReqA;
        syncReqA.GroupId = groupId;
        syncReqA.ProtocolType = protocolType;
        syncReqA.ProtocolName = protocolName;
        syncReqA.GenerationId = joinRespA->GenerationId;
        syncReqA.MemberId = joinRespA->MemberId.value();

        TSyncGroupRequestData syncReqB = syncReqA;
        syncReqB.GenerationId = joinRespB->GenerationId;
        syncReqB.MemberId = joinRespB->MemberId.value();

        TSyncGroupRequestData syncReqC = syncReqA;
        syncReqC.GenerationId = joinRespC->GenerationId;
        syncReqC.MemberId = joinRespC->MemberId.value();

        if (isLeaderA) {
            syncReqA.Assignments = assignments;
        } else if (isLeaderB) {
            syncReqB.Assignments = assignments;
        } else {
            syncReqC.Assignments = assignments;
        }

        clientA.WriteToSocket(syncHeaderA, syncReqA);
        clientB.WriteToSocket(syncHeaderB, syncReqB);
        clientC.WriteToSocket(syncHeaderC, syncReqC);

        auto syncRespA = clientA.ReadResponse<TSyncGroupResponseData>(syncHeaderA);
        auto syncRespB = clientB.ReadResponse<TSyncGroupResponseData>(syncHeaderB);
        auto syncRespC = clientC.ReadResponse<TSyncGroupResponseData>(syncHeaderC);

        UNIT_ASSERT_VALUES_EQUAL(syncRespA->ErrorCode, (TKafkaInt16)EKafkaErrors::NONE_ERROR);
        UNIT_ASSERT_VALUES_EQUAL(syncRespB->ErrorCode, (TKafkaInt16)EKafkaErrors::NONE_ERROR);
        UNIT_ASSERT_VALUES_EQUAL(syncRespC->ErrorCode, (TKafkaInt16)EKafkaErrors::NONE_ERROR);

        auto countPartitions = [topicName](const TConsumerProtocolAssignment& assignment) {
            size_t sum = 0;
            for (auto& ta : assignment.AssignedPartitions) {
                UNIT_ASSERT_VALUES_EQUAL(ta.Topic, topicName);
                sum += ta.Partitions.size();
            }
            return sum;
        };

        size_t countA = countPartitions(clientA.GetAssignments(syncRespA->Assignment));
        size_t countB = countPartitions(clientB.GetAssignments(syncRespB->Assignment));
        size_t countC = countPartitions(clientC.GetAssignments(syncRespC->Assignment));

        UNIT_ASSERT_VALUES_EQUAL(countA, size_t(totalPartitions / 3));
        UNIT_ASSERT_VALUES_EQUAL(countB, size_t(totalPartitions / 3));
        UNIT_ASSERT_VALUES_EQUAL(countC, size_t(totalPartitions / 3));

        UNIT_ASSERT_VALUES_EQUAL(
            clientA.Heartbeat(joinRespA->MemberId.value(), joinRespA->GenerationId, groupId)->ErrorCode,
            static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR)
        );
        UNIT_ASSERT_VALUES_EQUAL(
            clientB.Heartbeat(joinRespB->MemberId.value(), joinRespB->GenerationId, groupId)->ErrorCode,
            static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR)
        );
        UNIT_ASSERT_VALUES_EQUAL(
            clientC.Heartbeat(joinRespC->MemberId.value(), joinRespC->GenerationId, groupId)->ErrorCode,
            static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR)
        );

        // CHECK ONE CLIENT LEAVE, AND OTHERS GETS 1/2 OF PARTITIONS

        UNIT_ASSERT_VALUES_EQUAL(
            clientC.LeaveGroup(joinRespC->MemberId.value(), groupId)->ErrorCode,
            static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR)
        );

        clientA.WaitRebalance(joinRespA->MemberId.value(), joinRespA->GenerationId, groupId);
        clientB.WaitRebalance(joinRespB->MemberId.value(), joinRespB->GenerationId, groupId);

        TRequestHeaderData headerAJoin2 = clientA.Header(NKafka::EApiKey::JOIN_GROUP, 9);
        TRequestHeaderData headerBJoin2 = clientB.Header(NKafka::EApiKey::JOIN_GROUP, 9);

        joinReqA.MemberId = joinRespA->MemberId.value();
        joinReqB.MemberId = joinRespB->MemberId.value();

        TJoinGroupRequestData joinReqA2 = joinReqA;
        TJoinGroupRequestData joinReqB2 = joinReqB;

        clientA.WriteToSocket(headerAJoin2, joinReqA2);
        clientB.WriteToSocket(headerBJoin2, joinReqB2);

        auto joinRespA2 = clientA.ReadResponse<TJoinGroupResponseData>(headerAJoin2);
        auto joinRespB2 = clientB.ReadResponse<TJoinGroupResponseData>(headerBJoin2);

        UNIT_ASSERT_VALUES_EQUAL(joinRespA2->ErrorCode, (TKafkaInt16)EKafkaErrors::NONE_ERROR);
        UNIT_ASSERT_VALUES_EQUAL(joinRespB2->ErrorCode, (TKafkaInt16)EKafkaErrors::NONE_ERROR);

        bool isLeaderA2 = (joinRespA2->Leader == joinRespA2->MemberId);

        TMessagePtr<TJoinGroupResponseData> leaderResp2 = isLeaderA2 ? joinRespA2 : joinRespB2;

        std::vector<TSyncGroupRequestData::TSyncGroupRequestAssignment> assignments2 = clientA.MakeRangeAssignment(leaderResp2, totalPartitions);

        TRequestHeaderData syncHeaderA2 = clientA.Header(NKafka::EApiKey::SYNC_GROUP, 5);
        TRequestHeaderData syncHeaderB2 = clientB.Header(NKafka::EApiKey::SYNC_GROUP, 5);

        TSyncGroupRequestData syncReqA2;
        syncReqA2.GroupId = groupId;
        syncReqA2.ProtocolType = protocolType;
        syncReqA2.ProtocolName = protocolName;
        syncReqA2.GenerationId = joinRespA2->GenerationId;
        syncReqA2.MemberId = joinRespA2->MemberId.value();

        TSyncGroupRequestData syncReqB2 = syncReqA2;
        syncReqB2.GenerationId = joinRespB2->GenerationId;
        syncReqB2.MemberId = joinRespB2->MemberId.value();

        if (isLeaderA2) {
            syncReqA2.Assignments = assignments2;
        } else {
            syncReqB2.Assignments = assignments2;
        }

        clientA.WriteToSocket(syncHeaderA2, syncReqA2);
        clientB.WriteToSocket(syncHeaderB2, syncReqB2);

        auto syncRespA2 = clientA.ReadResponse<TSyncGroupResponseData>(syncHeaderA2);
        auto syncRespB2 = clientB.ReadResponse<TSyncGroupResponseData>(syncHeaderB2);

        UNIT_ASSERT_VALUES_EQUAL(syncRespA2->ErrorCode, (TKafkaInt16)EKafkaErrors::NONE_ERROR);
        UNIT_ASSERT_VALUES_EQUAL(syncRespB2->ErrorCode, (TKafkaInt16)EKafkaErrors::NONE_ERROR);

        size_t countA2 = countPartitions(clientA.GetAssignments(syncRespA2->Assignment));
        size_t countB2 = countPartitions(clientB.GetAssignments(syncRespB2->Assignment));

        UNIT_ASSERT_VALUES_EQUAL(countA2, size_t(totalPartitions / 2));
        UNIT_ASSERT_VALUES_EQUAL(countB2, size_t(totalPartitions / 2));

        UNIT_ASSERT_VALUES_EQUAL(
            clientA.Heartbeat(joinRespA2->MemberId.value(), joinRespA2->GenerationId, groupId)->ErrorCode,
            static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR)
        );

        UNIT_ASSERT_VALUES_EQUAL(
            clientB.Heartbeat(joinRespB2->MemberId.value(), joinRespB2->GenerationId, groupId)->ErrorCode,
            static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR)
        );

        // CHECK ONE READER DEAD (NO HEARTBEAT)

        Sleep(TDuration::Seconds(5));

        UNIT_ASSERT_VALUES_EQUAL(
            clientA.Heartbeat(joinRespA2->MemberId.value(), joinRespA2->GenerationId, groupId)->ErrorCode,
            static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR)
        );

        Sleep(TDuration::Seconds(25));

        UNIT_ASSERT_VALUES_EQUAL(
            clientA.Heartbeat(joinRespA2->MemberId.value(), joinRespA2->GenerationId, groupId)->ErrorCode,
            static_cast<TKafkaInt16>(EKafkaErrors::REBALANCE_IN_PROGRESS)
        );

        // LAST READER GETS ALL PARTITIONS
        clientA.JoinAndSyncGroupAndWaitPartitions(topics, groupId, totalPartitions, protocolName, totalPartitions, heartbeatTimeout);


        // CHECK IF MASTER DIE AFTER JOIN

        TRequestHeaderData headerAJoin3 = clientA.Header(NKafka::EApiKey::JOIN_GROUP, 9);
        TRequestHeaderData headerBJoin3 = clientB.Header(NKafka::EApiKey::JOIN_GROUP, 9);

        TJoinGroupRequestData joinReqA3 = joinReqA;
        TJoinGroupRequestData joinReqB3 = joinReqB;

        clientA.WriteToSocket(headerAJoin2, joinReqA2);
        clientB.WriteToSocket(headerBJoin2, joinReqB2);

        auto joinRespA3 = clientA.ReadResponse<TJoinGroupResponseData>(headerAJoin2);
        auto joinRespB3 = clientB.ReadResponse<TJoinGroupResponseData>(headerBJoin2);

        UNIT_ASSERT_VALUES_EQUAL(joinRespA2->ErrorCode, (TKafkaInt16)EKafkaErrors::NONE_ERROR);
        UNIT_ASSERT_VALUES_EQUAL(joinRespB2->ErrorCode, (TKafkaInt16)EKafkaErrors::NONE_ERROR);

        bool isLeaderA3 = (joinRespA3->Leader == joinRespA3->MemberId);

        TSyncGroupRequestData syncReqNotMaster;
        syncReqNotMaster.GroupId = groupId;
        syncReqNotMaster.ProtocolType = protocolType;
        syncReqNotMaster.ProtocolName = protocolName;

        TRequestHeaderData syncHeaderNotMaster;
        if (isLeaderA3) {
            syncReqNotMaster.GenerationId = joinRespB3->GenerationId;
            syncReqNotMaster.MemberId = joinRespB3->MemberId.value();
            syncHeaderNotMaster = clientB.Header(NKafka::EApiKey::SYNC_GROUP, 5);
            clientB.WriteToSocket(syncHeaderNotMaster, syncReqNotMaster);
            auto noMasterSyncResponse = clientB.ReadResponse<TSyncGroupResponseData>(syncHeaderNotMaster);
            UNIT_ASSERT_VALUES_EQUAL(noMasterSyncResponse->ErrorCode, (TKafkaInt16)EKafkaErrors::REBALANCE_IN_PROGRESS);
        } else {
            syncReqNotMaster.GenerationId = joinRespA3->GenerationId;
            syncReqNotMaster.MemberId = joinRespA3->MemberId.value();
            syncHeaderNotMaster = clientA.Header(NKafka::EApiKey::SYNC_GROUP, 5);
            clientA.WriteToSocket(syncHeaderNotMaster, syncReqNotMaster);
            auto noMasterSyncResponse = clientA.ReadResponse<TSyncGroupResponseData>(syncHeaderNotMaster);
            UNIT_ASSERT_VALUES_EQUAL(noMasterSyncResponse->ErrorCode, (TKafkaInt16)EKafkaErrors::REBALANCE_IN_PROGRESS);
        }
    }

    Y_UNIT_TEST(InitProducerId_withoutTransactionalIdShouldReturnRandomInt) {
        TInsecureTestServer testServer;

        TKafkaTestClient kafkaClient(testServer.Port);

        auto resp1 = kafkaClient.InitProducerId();
        auto resp2 = kafkaClient.InitProducerId();

        // validate first response
        UNIT_ASSERT_VALUES_EQUAL(resp1->ErrorCode, EKafkaErrors::NONE_ERROR);
        UNIT_ASSERT_GT(resp1->ProducerId, 0);
        UNIT_ASSERT_VALUES_EQUAL(resp1->ProducerEpoch, 0);
        // validate second response
        UNIT_ASSERT_VALUES_EQUAL(resp2->ErrorCode, EKafkaErrors::NONE_ERROR);
        UNIT_ASSERT_GT(resp2->ProducerId, 0);
        UNIT_ASSERT_VALUES_EQUAL(resp2->ProducerEpoch, 0);
        // validate different values for different responses
        UNIT_ASSERT_VALUES_UNEQUAL(resp1->ProducerId, resp2->ProducerId);
    }

    Y_UNIT_TEST(InitProducerId_forNewTransactionalIdShouldReturnIncrementingInt) {
        TInsecureTestServer testServer;

        TKafkaTestClient kafkaClient(testServer.Port);

        // use random transactional id for each request to avoid parallel execution problems
        auto resp1 = kafkaClient.InitProducerId(TStringBuilder() << "my-tx-producer-" << RandomNumber<ui64>());
        auto resp2 = kafkaClient.InitProducerId(TStringBuilder() << "my-tx-producer-" << RandomNumber<ui64>());

        // validate first response
        UNIT_ASSERT_VALUES_EQUAL(resp1->ErrorCode, EKafkaErrors::NONE_ERROR);
        UNIT_ASSERT_GT(resp1->ProducerId, 0);
        UNIT_ASSERT_VALUES_EQUAL(resp1->ProducerEpoch, 0);
        // validate second response
        UNIT_ASSERT_VALUES_EQUAL(resp2->ErrorCode, EKafkaErrors::NONE_ERROR);
        UNIT_ASSERT_GT(resp2->ProducerId, 0);
        UNIT_ASSERT_VALUES_EQUAL(resp2->ProducerEpoch, 0);
        // validate different values for different responses
        UNIT_ASSERT_VALUES_UNEQUAL(resp1->ProducerId, resp2->ProducerId);
    }

    Y_UNIT_TEST(InitProducerId_forSqlInjectionShouldReturnWithoutDropingDatabase) {
        TInsecureTestServer testServer;

        TKafkaTestClient kafkaClient(testServer.Port);

        auto resp1 = kafkaClient.InitProducerId("; DROP TABLE kafka_transactional_producers");

        // validate first response
        UNIT_ASSERT_VALUES_EQUAL(resp1->ErrorCode, EKafkaErrors::NONE_ERROR);
        UNIT_ASSERT_GT(resp1->ProducerId, 0);
        UNIT_ASSERT_VALUES_EQUAL(resp1->ProducerEpoch, 0);
    }

    Y_UNIT_TEST(InitProducerId_forPreviouslySeenTransactionalIdShouldReturnSameProducerIdAndIncrementEpoch) {
        TInsecureTestServer testServer;

        TKafkaTestClient kafkaClient(testServer.Port);
        // use random transactional id for each request to avoid parallel execution problems
        auto transactionalId = TStringBuilder() << "my-tx-producer-" << RandomNumber<ui64>();

        auto resp1 = kafkaClient.InitProducerId(transactionalId);
        auto resp2 = kafkaClient.InitProducerId(transactionalId);

        // validate first response
        UNIT_ASSERT_VALUES_EQUAL(resp1->ErrorCode, EKafkaErrors::NONE_ERROR);
        UNIT_ASSERT_GT(resp1->ProducerId, 0);
        UNIT_ASSERT_VALUES_EQUAL(resp1->ProducerEpoch, 0);
        // validate second response
        UNIT_ASSERT_VALUES_EQUAL(resp2->ErrorCode, EKafkaErrors::NONE_ERROR);
        UNIT_ASSERT_VALUES_EQUAL(resp2->ProducerId, resp1->ProducerId);
        UNIT_ASSERT_VALUES_EQUAL(resp2->ProducerEpoch, 1);
    }

    Y_UNIT_TEST(InitProducerId_forPreviouslySeenTransactionalIdShouldReturnNewProducerIdIfEpochOverflown) {
        TInsecureTestServer testServer;

        TKafkaTestClient kafkaClient(testServer.Port);
        // use random transactional id for each request to avoid parallel execution problems
        auto transactionalId = TStringBuilder() << "my-tx-producer-" << RandomNumber<ui64>();

        // this first request will init table
        auto resp1 = kafkaClient.InitProducerId(transactionalId);
        // update epoch to be last available
        NYdb::NTable::TTableClient tableClient(*testServer.Driver);
        TValueBuilder rows;
        rows.BeginList();
        rows.AddListItem()
            .BeginStruct()
                .AddMember("database").Utf8("/Root")
                .AddMember("transactional_id").Utf8(transactionalId)
                .AddMember("producer_id").Int64(resp1->ProducerId)
                .AddMember("producer_epoch").Int16(std::numeric_limits<i16>::max() - 1)
                .AddMember("updated_at").Datetime(TInstant::Now())
            .EndStruct();
        rows.EndList();
        auto upsertResult = tableClient.BulkUpsert("//Root/.metadata/kafka_transactional_producers", rows.Build()).GetValueSync();
        UNIT_ASSERT_EQUAL(upsertResult.GetStatus(), EStatus::SUCCESS);

        auto resp2 = kafkaClient.InitProducerId(transactionalId);

        // validate first response
        UNIT_ASSERT_VALUES_EQUAL(resp1->ErrorCode, EKafkaErrors::NONE_ERROR);
        UNIT_ASSERT_GT(resp1->ProducerId, 0);
        UNIT_ASSERT_VALUES_EQUAL(resp1->ProducerEpoch, 0);
        // validate second response
        UNIT_ASSERT_VALUES_EQUAL(resp2->ErrorCode, EKafkaErrors::NONE_ERROR);
        UNIT_ASSERT_GT(resp2->ProducerId, 0);
        UNIT_ASSERT_VALUES_EQUAL(resp2->ProducerEpoch, 0);
        // new producer.id should be given
        UNIT_ASSERT_VALUES_UNEQUAL(resp1->ProducerId, resp2->ProducerId);
    }

    Y_UNIT_TEST(InitProducerIf_withInvalidTransactionTimeout_shouldReturnError) {
        TInsecureTestServer testServer;
        TKafkaTestClient kafkaClient(testServer.Port);
        // use random transactional id for each request to avoid parallel execution problems
        auto transactionalId = TStringBuilder() << "my-tx-producer-" << TGUID::Create().AsUuidString();

        auto resp = kafkaClient.InitProducerId(transactionalId, testServer.KikimrServer->GetRuntime()->GetAppData().KafkaProxyConfig.GetTransactionTimeoutMs() + 1);
        UNIT_ASSERT_VALUES_EQUAL(resp->ErrorCode, EKafkaErrors::INVALID_TRANSACTION_TIMEOUT);
    }

    Y_UNIT_TEST(CommitTransactionScenario) {
        TInsecureTestServer testServer("1", false, true);
        TKafkaTestClient kafkaClient(testServer.Port);
        NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
        // use random values to avoid parallel execution problems
        TString inputTopicName = TStringBuilder() << "input-topic-" << RandomNumber<ui64>();
        TString outputTopicName = TStringBuilder() << "output-topic-" << RandomNumber<ui64>();
        TString transactionalId = TStringBuilder() << "my-tx-producer-" << RandomNumber<ui64>();
        TString consumerName = "my-consumer";

        // create input and output topics
        CreateTopic(pqClient, inputTopicName, 3, {consumerName});
        CreateTopic(pqClient, outputTopicName, 3, {consumerName});

        // produce data to input topic (to commit offsets in further steps)
        auto inputProduceResponse = kafkaClient.Produce({inputTopicName, 0}, {{"key1", "val1"}, {"key2", "val2"}, {"key3", "val3"}});
        UNIT_ASSERT_VALUES_EQUAL(inputProduceResponse->Responses[0].PartitionResponses[0].ErrorCode, EKafkaErrors::NONE_ERROR);

        // init producer id
        auto initProducerIdResp = kafkaClient.InitProducerId(transactionalId, 30000);
        UNIT_ASSERT_VALUES_EQUAL(initProducerIdResp->ErrorCode, EKafkaErrors::NONE_ERROR);
        TProducerInstanceId producerInstanceId = {initProducerIdResp->ProducerId, initProducerIdResp->ProducerEpoch};

        // add partitions to txn
        std::unordered_map<TString, std::vector<ui32>> topicPartitionsToAddToTxn;
        topicPartitionsToAddToTxn[outputTopicName] = std::vector<ui32>{0, 1};
        auto addPartsResponse = kafkaClient.AddPartitionsToTxn(transactionalId, producerInstanceId, topicPartitionsToAddToTxn);
        UNIT_ASSERT_VALUES_EQUAL(addPartsResponse->Results[0].Results[0].ErrorCode, EKafkaErrors::NONE_ERROR);
        UNIT_ASSERT_VALUES_EQUAL(addPartsResponse->Results[0].Results[1].ErrorCode, EKafkaErrors::NONE_ERROR);

        // produce data
        // to part 0
        auto out0ProduceResponse = kafkaClient.Produce({outputTopicName, 0}, {{"0", "123"}}, 0, producerInstanceId, transactionalId);
        UNIT_ASSERT_VALUES_EQUAL(out0ProduceResponse->Responses[0].PartitionResponses[0].ErrorCode, EKafkaErrors::NONE_ERROR);
        // to part 1
        auto out1ProduceResponse = kafkaClient.Produce({outputTopicName, 1}, {{"1", "987"}}, 0, producerInstanceId, transactionalId);
        UNIT_ASSERT_VALUES_EQUAL(out1ProduceResponse->Responses[0].PartitionResponses[0].ErrorCode, EKafkaErrors::NONE_ERROR);

        // init consumer
        std::vector<TString> topicsToSubscribe;
        topicsToSubscribe.push_back(outputTopicName);
        TString protocolName = "range";
        auto consumerInfo = kafkaClient.JoinAndSyncGroupAndWaitPartitions(topicsToSubscribe, consumerName, 3, protocolName, 3, 15000);

        kafkaClient.ValidateNoDataInTopics({{outputTopicName, {0, 1}}});

        // add offsets to txn
        auto addOffsetsResponse = kafkaClient.AddOffsetsToTxn(transactionalId, producerInstanceId, consumerName);
        UNIT_ASSERT_VALUES_EQUAL(addOffsetsResponse->ErrorCode, EKafkaErrors::NONE_ERROR);

        // txn offset commit
        std::unordered_map<TString, std::vector<std::pair<ui32, ui64>>> offsetsToCommit;
        offsetsToCommit[inputTopicName] = std::vector<std::pair<ui32, ui64>>{{0, 2}};
        auto txnOffsetCommitResponse = kafkaClient.TxnOffsetCommit(transactionalId, producerInstanceId, consumerName, consumerInfo.GenerationId, offsetsToCommit);
        UNIT_ASSERT_VALUES_EQUAL(txnOffsetCommitResponse->Topics[0].Partitions[0].ErrorCode, EKafkaErrors::NONE_ERROR);

        // end txn
        auto endTxnResponse = kafkaClient.EndTxn(transactionalId, producerInstanceId, true);
        UNIT_ASSERT_VALUES_EQUAL(endTxnResponse->ErrorCode, EKafkaErrors::NONE_ERROR);

        // validate data is accessible in target topic
        auto fetchResponse1 = kafkaClient.Fetch({{outputTopicName, {0, 1}}});
        UNIT_ASSERT_VALUES_EQUAL(fetchResponse1->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        UNIT_ASSERT_VALUES_EQUAL(fetchResponse1->Responses[0].Partitions[0].Records->Records.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fetchResponse1->Responses[0].Partitions[1].Records->Records.size(), 1);
        auto record1 = fetchResponse1->Responses[0].Partitions[0].Records->Records[0];
        UNIT_ASSERT_VALUES_EQUAL(TString(record1.Key.value().data(), record1.Key.value().size()), "0");
        UNIT_ASSERT_VALUES_EQUAL(TString(record1.Value.value().data(), record1.Value.value().size()), "123");
        auto record2 = fetchResponse1->Responses[0].Partitions[1].Records->Records[0];
        UNIT_ASSERT_VALUES_EQUAL(TString(record2.Key.value().data(), record2.Key.value().size()), "1");
        UNIT_ASSERT_VALUES_EQUAL(TString(record2.Value.value().data(), record2.Value.value().size()), "987");

        // validate consumer offset committed
        std::map<TString, std::vector<i32>> topicsToPartitionsToFetch;
        topicsToPartitionsToFetch[inputTopicName] = std::vector<i32>{0};
        auto offsetFetchResponse = kafkaClient.OffsetFetch(consumerName, topicsToPartitionsToFetch);
        UNIT_ASSERT_VALUES_EQUAL(offsetFetchResponse->ErrorCode, EKafkaErrors::NONE_ERROR);
        UNIT_ASSERT_VALUES_EQUAL(offsetFetchResponse->Groups[0].Topics[0].Partitions[0].CommittedOffset, 2);

        {
            // Commit the second transaction to ensure we properly handle producer epoch in transactions.

            auto initProducerIdResp = kafkaClient.InitProducerId(transactionalId, 30000);
            UNIT_ASSERT_VALUES_EQUAL(initProducerIdResp->ErrorCode, EKafkaErrors::NONE_ERROR);
            TProducerInstanceId producerInstanceId = {initProducerIdResp->ProducerId, initProducerIdResp->ProducerEpoch};

            std::unordered_map<TString, std::vector<ui32>> topicPartitionsToAddToTxn;
            topicPartitionsToAddToTxn[outputTopicName] = std::vector<ui32>{0};
            auto addPartsResponse = kafkaClient.AddPartitionsToTxn(transactionalId, producerInstanceId, topicPartitionsToAddToTxn);
            UNIT_ASSERT_VALUES_EQUAL(addPartsResponse->Results[0].Results[0].ErrorCode, EKafkaErrors::NONE_ERROR);

            auto out0ProduceResponse = kafkaClient.Produce({outputTopicName, 0}, {{"1", "123"}, {"2", "234"}}, 0, producerInstanceId, transactionalId);
            UNIT_ASSERT_VALUES_EQUAL(out0ProduceResponse->Responses[0].PartitionResponses[0].ErrorCode, EKafkaErrors::NONE_ERROR);

            auto consumerInfo = kafkaClient.JoinAndSyncGroupAndWaitPartitions(topicsToSubscribe, consumerName, 3, protocolName, 3, 15000);

            auto addOffsetsResponse = kafkaClient.AddOffsetsToTxn(transactionalId, producerInstanceId, consumerName);
            UNIT_ASSERT_VALUES_EQUAL(addOffsetsResponse->ErrorCode, EKafkaErrors::NONE_ERROR);

            std::unordered_map<TString, std::vector<std::pair<ui32, ui64>>> offsetsToCommit{{inputTopicName, {{0, 3}}}};
            auto txnOffsetCommitResponse = kafkaClient.TxnOffsetCommit(transactionalId, producerInstanceId, consumerName, consumerInfo.GenerationId, offsetsToCommit);
            UNIT_ASSERT_VALUES_EQUAL(txnOffsetCommitResponse->Topics[0].Partitions[0].ErrorCode, EKafkaErrors::NONE_ERROR);

            auto endTxnResponse = kafkaClient.EndTxn(transactionalId, producerInstanceId, true);
            UNIT_ASSERT_VALUES_EQUAL(endTxnResponse->ErrorCode, EKafkaErrors::NONE_ERROR);
        }
    }

    Y_UNIT_TEST(ReadOnlyTransactionScenario) {
        TInsecureTestServer testServer("1", false, true);
        TKafkaTestClient kafkaClient(testServer.Port);
        NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
        // use random values to avoid parallel execution problems
        TString inputTopicName = TStringBuilder() << "input-topic-" << RandomNumber<ui64>();
        TString transactionalId = TStringBuilder() << "my-tx-producer-" << RandomNumber<ui64>();
        TString consumerName = "my-consumer";

        // create input and output topics
        CreateTopic(pqClient, inputTopicName, 3, {consumerName});

        // produce data to input topic (to commit offsets in further steps)
        auto inputProduceResponse = kafkaClient.Produce({inputTopicName, 0}, {{"key1", "val1"}, {"key2", "val2"}, {"key3", "val3"}});
        UNIT_ASSERT_VALUES_EQUAL(inputProduceResponse->Responses[0].PartitionResponses[0].ErrorCode, EKafkaErrors::NONE_ERROR);

        // init producer id
        auto initProducerIdResp = kafkaClient.InitProducerId(transactionalId, 30000);
        UNIT_ASSERT_VALUES_EQUAL(initProducerIdResp->ErrorCode, EKafkaErrors::NONE_ERROR);
        TProducerInstanceId producerInstanceId = {initProducerIdResp->ProducerId, initProducerIdResp->ProducerEpoch};

        // init consumer
        std::vector<TString> topicsToSubscribe;
        topicsToSubscribe.push_back(inputTopicName);
        TString protocolName = "range";
        auto consumerInfo = kafkaClient.JoinAndSyncGroupAndWaitPartitions(topicsToSubscribe, consumerName, 3, protocolName, 3, 15000);

        // add offsets to txn
        auto addOffsetsResponse = kafkaClient.AddOffsetsToTxn(transactionalId, producerInstanceId, consumerName);
        UNIT_ASSERT_VALUES_EQUAL(addOffsetsResponse->ErrorCode, EKafkaErrors::NONE_ERROR);

        // txn offset commit
        std::unordered_map<TString, std::vector<std::pair<ui32, ui64>>> offsetsToCommit;
        offsetsToCommit[inputTopicName] = std::vector<std::pair<ui32, ui64>>{{0, 2}};
        auto txnOffsetCommitResponse = kafkaClient.TxnOffsetCommit(transactionalId, producerInstanceId, consumerName, consumerInfo.GenerationId, offsetsToCommit);
        UNIT_ASSERT_VALUES_EQUAL(txnOffsetCommitResponse->Topics[0].Partitions[0].ErrorCode, EKafkaErrors::NONE_ERROR);

        // end txn. Check that it works successfully without ADD_PARTITIONS_TO_TXN request
        auto endTxnResponse = kafkaClient.EndTxn(transactionalId, producerInstanceId, true);
        UNIT_ASSERT_VALUES_EQUAL(endTxnResponse->ErrorCode, EKafkaErrors::NONE_ERROR);

        //validate consumer offset committed
        std::map<TString, std::vector<i32>> topicsToPartitionsToFetch;
        topicsToPartitionsToFetch[inputTopicName] = std::vector<i32>{0};
        auto offsetFetchResponse = kafkaClient.OffsetFetch(consumerName, topicsToPartitionsToFetch);
        UNIT_ASSERT_VALUES_EQUAL(offsetFetchResponse->ErrorCode, EKafkaErrors::NONE_ERROR);
        UNIT_ASSERT_VALUES_EQUAL(offsetFetchResponse->Groups[0].Topics[0].Partitions[0].CommittedOffset, 2);
    }

    Y_UNIT_TEST(Several_Subsequent_Transactions_Scenario) {
        TInsecureTestServer testServer("1", false, true);
        TKafkaTestClient kafkaClient(testServer.Port);
        NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
        // use random values to avoid parallel execution problems
        TString inputTopicName = TStringBuilder() << "input-topic-" << RandomNumber<ui64>();
        TString outputTopicName = TStringBuilder() << "output-topic-" << RandomNumber<ui64>();
        TString transactionalId = TStringBuilder() << "my-tx-producer-" << RandomNumber<ui64>();
        TString consumerName = "my-consumer";

        // create input and output topics
        CreateTopic(pqClient, inputTopicName, 3, {consumerName});
        CreateTopic(pqClient, outputTopicName, 3, {consumerName});

        // produce data to input topic (to commit offsets in further steps)
        auto inputProduceResponse = kafkaClient.Produce({inputTopicName, 0}, {{"key1", "val1"}, {"key2", "val2"}, {"key3", "val3"}});
        UNIT_ASSERT_VALUES_EQUAL(inputProduceResponse->Responses[0].PartitionResponses[0].ErrorCode, EKafkaErrors::NONE_ERROR);

        // init producer id
        auto initProducerIdResp = kafkaClient.InitProducerId(transactionalId, 30000);
        UNIT_ASSERT_VALUES_EQUAL(initProducerIdResp->ErrorCode, EKafkaErrors::NONE_ERROR);
        TProducerInstanceId producerInstanceId = {initProducerIdResp->ProducerId, initProducerIdResp->ProducerEpoch};

        ui32 totalTxns = 3;
        for (ui32 i = 0; i < totalTxns; ++i) {
            // add partitions to txn
            std::unordered_map<TString, std::vector<ui32>> topicPartitionsToAddToTxn;
            topicPartitionsToAddToTxn[outputTopicName] = std::vector<ui32>{0, 1};
            auto addPartsResponse = kafkaClient.AddPartitionsToTxn(transactionalId, producerInstanceId, topicPartitionsToAddToTxn);
            UNIT_ASSERT_VALUES_EQUAL(addPartsResponse->Results[0].Results[0].ErrorCode, EKafkaErrors::NONE_ERROR);
            UNIT_ASSERT_VALUES_EQUAL(addPartsResponse->Results[0].Results[1].ErrorCode, EKafkaErrors::NONE_ERROR);

            // produce data
            // to part 0
            auto out0ProduceResponse = kafkaClient.Produce({outputTopicName, 0}, {{std::to_string(i), "123"}, {std::to_string(i + totalTxns * 2), "456"}}, i, producerInstanceId, transactionalId);
            UNIT_ASSERT_VALUES_EQUAL_C(out0ProduceResponse->Responses[0].PartitionResponses[0].ErrorCode, EKafkaErrors::NONE_ERROR, TStringBuilder() << "Txn " << i + 1);
            // to part 1
            // checking all ProduceRequests recreation if WriteId is reused

            kafkaClient.ProduceAsync({outputTopicName, 1}, {{std::to_string(i + totalTxns), "987"}}, i, producerInstanceId, transactionalId);
            kafkaClient.ProduceAsync({outputTopicName, 1}, {{std::to_string(i + totalTxns * 3), "111"}}, i, producerInstanceId, transactionalId);

            auto out1ProduceResponse = kafkaClient.ReadLastResult(11 * i + 4);
            UNIT_ASSERT_VALUES_EQUAL_C(out1ProduceResponse->Responses[0].PartitionResponses[0].ErrorCode, EKafkaErrors::NONE_ERROR, TStringBuilder() << "Txn " << i + 1);

            auto out2ProduceResponse = kafkaClient.ReadLastResult(11 * i + 5);
            UNIT_ASSERT_VALUES_EQUAL_C(out2ProduceResponse->Responses[0].PartitionResponses[0].ErrorCode, EKafkaErrors::NONE_ERROR, TStringBuilder() << "Txn " << i + 1);

            // init consumer
            std::vector<TString> topicsToSubscribe;
            topicsToSubscribe.push_back(outputTopicName);
            TString protocolName = "range";
            auto consumerInfo = kafkaClient.JoinAndSyncGroupAndWaitPartitions(topicsToSubscribe, consumerName, 3, protocolName, 3, 15000);

            // add offsets to txn
            auto addOffsetsResponse = kafkaClient.AddOffsetsToTxn(transactionalId, producerInstanceId, consumerName);
            UNIT_ASSERT_VALUES_EQUAL(addOffsetsResponse->ErrorCode, EKafkaErrors::NONE_ERROR);

            // txn offset commit
            std::unordered_map<TString, std::vector<std::pair<ui32, ui64>>> offsetsToCommit;
            offsetsToCommit[inputTopicName] = std::vector<std::pair<ui32, ui64>>{{0, i + 1}};
            auto txnOffsetCommitResponse = kafkaClient.TxnOffsetCommit(transactionalId, producerInstanceId, consumerName, consumerInfo.GenerationId, offsetsToCommit);
            UNIT_ASSERT_VALUES_EQUAL(txnOffsetCommitResponse->Topics[0].Partitions[0].ErrorCode, EKafkaErrors::NONE_ERROR);

            // end txn
            auto endTxnResponse = kafkaClient.EndTxn(transactionalId, producerInstanceId, true);
            UNIT_ASSERT_VALUES_EQUAL(endTxnResponse->ErrorCode, EKafkaErrors::NONE_ERROR);

            // validate data is accessible in target topic
            auto fetchResponse1 = kafkaClient.Fetch({{outputTopicName, {0, 1}}});
            UNIT_ASSERT_VALUES_EQUAL(fetchResponse1->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT_VALUES_EQUAL(fetchResponse1->Responses[0].Partitions[0].Records->Records.size(), (i + 1) * 2);
            UNIT_ASSERT_VALUES_EQUAL(fetchResponse1->Responses[0].Partitions[1].Records->Records.size(), i + 1);
            auto record11 = fetchResponse1->Responses[0].Partitions[0].Records->Records[i * 2];
            UNIT_ASSERT_VALUES_EQUAL(TString(record11.Key.value().data(), record11.Key.value().size()), std::to_string(i));
            UNIT_ASSERT_VALUES_EQUAL(TString(record11.Value.value().data(), record11.Value.value().size()), "123");
            auto record12 = fetchResponse1->Responses[0].Partitions[0].Records->Records[i * 2 + 1];
            UNIT_ASSERT_VALUES_EQUAL(TString(record12.Key.value().data(), record12.Key.value().size()), std::to_string( i+ totalTxns * 2));
            UNIT_ASSERT_VALUES_EQUAL(TString(record12.Value.value().data(), record12.Value.value().size()), "456");
            auto record2 = fetchResponse1->Responses[0].Partitions[1].Records->Records[i];
            UNIT_ASSERT_VALUES_EQUAL(TString(record2.Key.value().data(), record2.Key.value().size()), std::to_string(i + totalTxns));
            UNIT_ASSERT_VALUES_EQUAL(TString(record2.Value.value().data(), record2.Value.value().size()), "987");

            // validate consumer offset committed
            std::map<TString, std::vector<i32>> topicsToPartitionsToFetch;
            topicsToPartitionsToFetch[inputTopicName] = std::vector<i32>{0};
            auto offsetFetchResponse = kafkaClient.OffsetFetch(consumerName, topicsToPartitionsToFetch);
            UNIT_ASSERT_VALUES_EQUAL(offsetFetchResponse->ErrorCode, EKafkaErrors::NONE_ERROR);
            UNIT_ASSERT_VALUES_EQUAL(offsetFetchResponse->Groups[0].Topics[0].Partitions[0].CommittedOffset, i + 1);
        }
    }

    Y_UNIT_TEST(Several_Writes_In_One_Transaction) {
        TInsecureTestServer testServer("1", false, true);
        TKafkaTestClient kafkaClient(testServer.Port);
        NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
        // use random values to avoid parallel execution problems
        TString topicName = TStringBuilder() << "input-topic-" << RandomNumber<ui64>();
        TString transactionalId = TStringBuilder() << "my-tx-producer-" << RandomNumber<ui64>();
        TString consumerName = "my-consumer";

        CreateTopic(pqClient, topicName, 1, {consumerName});

        // init producer id
        auto initProducerIdResp = kafkaClient.InitProducerId(transactionalId, 30000);
        UNIT_ASSERT_VALUES_EQUAL(initProducerIdResp->ErrorCode, EKafkaErrors::NONE_ERROR);
        TProducerInstanceId producerInstanceId = {initProducerIdResp->ProducerId, initProducerIdResp->ProducerEpoch};

        // add partitions to txn
        std::unordered_map<TString, std::vector<ui32>> topicPartitionsToAddToTxn;
        topicPartitionsToAddToTxn[topicName] = std::vector<ui32>{0};
        auto addPartsResponse = kafkaClient.AddPartitionsToTxn(transactionalId, producerInstanceId, topicPartitionsToAddToTxn);
        UNIT_ASSERT_VALUES_EQUAL(addPartsResponse->Results[0].Results[0].ErrorCode, EKafkaErrors::NONE_ERROR);

        // produce data
        auto produceResponse0 = kafkaClient.Produce({topicName, 0}, {{"0", "123"}}, 0, producerInstanceId, transactionalId);
        UNIT_ASSERT_VALUES_EQUAL(produceResponse0->Responses[0].PartitionResponses[0].ErrorCode, EKafkaErrors::NONE_ERROR);
        auto produceResponse1 = kafkaClient.Produce({topicName, 0}, {{"1", "234"}}, 1, producerInstanceId, transactionalId);
        UNIT_ASSERT_VALUES_EQUAL(produceResponse1->Responses[0].PartitionResponses[0].ErrorCode, EKafkaErrors::NONE_ERROR);

        // init consumer
        std::vector<TString> topicsToSubscribe;
        topicsToSubscribe.push_back(topicName);
        TString protocolName = "range";
        auto consumerInfo = kafkaClient.JoinAndSyncGroupAndWaitPartitions(topicsToSubscribe, consumerName, 1, protocolName, 1, 15000);

        // end txn
        auto endTxnResponse = kafkaClient.EndTxn(transactionalId, producerInstanceId, true);
        UNIT_ASSERT_VALUES_EQUAL(endTxnResponse->ErrorCode, EKafkaErrors::NONE_ERROR);

        // validate data is accessible in target topic
        auto fetchResponse1 = kafkaClient.Fetch({{topicName, {0}}});
        UNIT_ASSERT_VALUES_EQUAL(fetchResponse1->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        UNIT_ASSERT_VALUES_EQUAL(fetchResponse1->Responses[0].Partitions[0].Records->Records.size(), 2);
        auto record1 = fetchResponse1->Responses[0].Partitions[0].Records->Records[0];
        UNIT_ASSERT_VALUES_EQUAL(TString(record1.Key.value().data(), record1.Key.value().size()), "0");
        UNIT_ASSERT_VALUES_EQUAL(TString(record1.Value.value().data(), record1.Value.value().size()), "123");
        auto record2 = fetchResponse1->Responses[0].Partitions[0].Records->Records[1];
        UNIT_ASSERT_VALUES_EQUAL(TString(record2.Key.value().data(), record2.Key.value().size()), "1");
        UNIT_ASSERT_VALUES_EQUAL(TString(record2.Value.value().data(), record2.Value.value().size()), "234");
    }

    Y_UNIT_TEST(Commit_Transaction_After_timeout_should_return_producer_fenced) {
        TInsecureTestServer testServer("1", false, true);
        TKafkaTestClient kafkaClient(testServer.Port);
        NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
        // use random values to avoid parallel execution problems
        TString outputTopicName = TStringBuilder() << "output-topic-" << TGUID::Create().AsUuidString();
        TString transactionalId = TStringBuilder() << "my-tx-producer-" << TGUID::Create().AsUuidString();
        TString consumerName = "my-consumer";

        // create input and output topics
        CreateTopic(pqClient, outputTopicName, 3, {consumerName});

        // init producer id
        ui64 txnTimeoutMs = 1000;
        auto initProducerIdResp = kafkaClient.InitProducerId(transactionalId, txnTimeoutMs);
        UNIT_ASSERT_VALUES_EQUAL(initProducerIdResp->ErrorCode, EKafkaErrors::NONE_ERROR);
        TProducerInstanceId producerInstanceId = {initProducerIdResp->ProducerId, initProducerIdResp->ProducerEpoch};

        // add partitions to txn
        std::unordered_map<TString, std::vector<ui32>> topicPartitionsToAddToTxn;
        topicPartitionsToAddToTxn[outputTopicName] = std::vector<ui32>{0};
        auto addPartsResponse = kafkaClient.AddPartitionsToTxn(transactionalId, producerInstanceId, topicPartitionsToAddToTxn);
        UNIT_ASSERT_VALUES_EQUAL(addPartsResponse->Results[0].Results[0].ErrorCode, EKafkaErrors::NONE_ERROR);

        // produce data
        // to part 0
        auto out0ProduceResponse = kafkaClient.Produce({outputTopicName, 0}, {{"0", "123"}}, 0, producerInstanceId, transactionalId);
        UNIT_ASSERT_VALUES_EQUAL(out0ProduceResponse->Responses[0].PartitionResponses[0].ErrorCode, EKafkaErrors::NONE_ERROR);

        // init consumer
        std::vector<TString> topicsToSubscribe{outputTopicName};
        TString protocolName = "range";
        auto consumerInfo = kafkaClient.JoinAndSyncGroupAndWaitPartitions(topicsToSubscribe, consumerName, 3, protocolName, 3, 15000);

        kafkaClient.ValidateNoDataInTopics({{outputTopicName, {0}}});

        // move time forward after transaction timeout
        Sleep(TDuration::MilliSeconds(txnTimeoutMs));

        // end txn
        auto endTxnResponse = kafkaClient.EndTxn(transactionalId, producerInstanceId, true);
        UNIT_ASSERT_VALUES_EQUAL(endTxnResponse->ErrorCode, EKafkaErrors::PRODUCER_FENCED);

        // validate data is still not assessible in target topic
        auto fetchResponse1 = kafkaClient.Fetch({{outputTopicName, {0}}});
        UNIT_ASSERT_VALUES_EQUAL(fetchResponse1->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        UNIT_ASSERT(!fetchResponse1->Responses[0].Partitions[0].Records.has_value());
    }

    Y_UNIT_TEST(AbortTransactionScenario) {
        TInsecureTestServer testServer("1", false, true);
        TKafkaTestClient kafkaClient(testServer.Port);
        NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
        // use random values to avoid parallel execution problems
        TString inputTopicName = TStringBuilder() << "input-topic-" << RandomNumber<ui64>();
        TString outputTopicName = TStringBuilder() << "output-topic-" << RandomNumber<ui64>();
        TString transactionalId = TStringBuilder() << "my-tx-producer-" << RandomNumber<ui64>();
        TString consumerName = "my-consumer";

        // create input and output topics
        CreateTopic(pqClient, inputTopicName, 3, {consumerName});
        CreateTopic(pqClient, outputTopicName, 3, {consumerName});

        // produce data to input topic (to commit offsets in further steps)
        auto inputProduceResponse = kafkaClient.Produce({inputTopicName, 0}, {{"key1", "val1"}, {"key2", "val2"}});
        UNIT_ASSERT_VALUES_EQUAL(inputProduceResponse->Responses[0].PartitionResponses[0].ErrorCode, EKafkaErrors::NONE_ERROR);

        // init producer id
        auto initProducerIdResp = kafkaClient.InitProducerId(transactionalId, 30000);
        UNIT_ASSERT_VALUES_EQUAL(initProducerIdResp->ErrorCode, EKafkaErrors::NONE_ERROR);
        TProducerInstanceId producerInstanceId = {initProducerIdResp->ProducerId, initProducerIdResp->ProducerEpoch};

        // add partitions to txn
        std::unordered_map<TString, std::vector<ui32>> topicPartitionsToAddToTxn;
        auto addPartsResponse = kafkaClient.AddPartitionsToTxn(transactionalId, producerInstanceId, topicPartitionsToAddToTxn);
        // produce data
        // to part 0
        auto out0ProduceResponse = kafkaClient.Produce({outputTopicName, 0}, {{"0", "123"}}, 0, producerInstanceId, transactionalId);
        UNIT_ASSERT_VALUES_EQUAL(out0ProduceResponse->Responses[0].PartitionResponses[0].ErrorCode, EKafkaErrors::NONE_ERROR);
        // to part 1
        auto out1ProduceResponse = kafkaClient.Produce({outputTopicName, 1}, {{"1", "987"}}, 0, producerInstanceId, transactionalId);
        UNIT_ASSERT_VALUES_EQUAL(out1ProduceResponse->Responses[0].PartitionResponses[0].ErrorCode, EKafkaErrors::NONE_ERROR);

        // init consumer
        std::vector<TString> topicsToSubscribe;
        topicsToSubscribe.push_back(outputTopicName);
        TString protocolName = "range";
        auto consumerInfo = kafkaClient.JoinAndSyncGroupAndWaitPartitions(topicsToSubscribe, consumerName, 3, protocolName, 3, 15000);

        kafkaClient.ValidateNoDataInTopics({{outputTopicName, {0, 1}}});

        // add offsets to txn
        auto addOffsetsResponse = kafkaClient.AddOffsetsToTxn(transactionalId, producerInstanceId, consumerName);
        UNIT_ASSERT_VALUES_EQUAL(addOffsetsResponse->ErrorCode, EKafkaErrors::NONE_ERROR);

        // txn offset commit
        std::unordered_map<TString, std::vector<std::pair<ui32, ui64>>> offsetsToCommit;
        offsetsToCommit[inputTopicName] = std::vector<std::pair<ui32, ui64>>{{0, 2}};
        auto txnOffsetCommitResponse = kafkaClient.TxnOffsetCommit(transactionalId, producerInstanceId, consumerName, consumerInfo.GenerationId, offsetsToCommit);
        UNIT_ASSERT_VALUES_EQUAL(txnOffsetCommitResponse->Topics[0].Partitions[0].ErrorCode, EKafkaErrors::NONE_ERROR);

        // end txn
        auto endTxnResponse = kafkaClient.EndTxn(transactionalId, producerInstanceId, false);
        UNIT_ASSERT_VALUES_EQUAL(endTxnResponse->ErrorCode, EKafkaErrors::NONE_ERROR);

        kafkaClient.ValidateNoDataInTopics({{outputTopicName, {0, 1}}});

        // validate consumer offset not committed
        std::map<TString, std::vector<i32>> topicsToPartitionsToFetch;
        topicsToPartitionsToFetch[inputTopicName] = std::vector<i32>{0};
        auto offsetFetchResponse = kafkaClient.OffsetFetch(consumerName, topicsToPartitionsToFetch);
        UNIT_ASSERT_VALUES_EQUAL(offsetFetchResponse->ErrorCode, EKafkaErrors::NONE_ERROR);
        UNIT_ASSERT_VALUES_EQUAL(offsetFetchResponse->Groups[0].Topics[0].Partitions[0].CommittedOffset, 0);
    }

    Y_UNIT_TEST(TransactionShouldBeAbortedIfPartitionIsAddedToTransactionButNoWritesToItWereReceived) {
        TInsecureTestServer testServer("1", false, true);
        TKafkaTestClient kafkaClient(testServer.Port);
        NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
        // use random values to avoid parallel execution problems
        TString inputTopicName = TStringBuilder() << "input-topic-" << RandomNumber<ui64>();
        TString outputTopicName = TStringBuilder() << "output-topic-" << RandomNumber<ui64>();
        TString transactionalId = TStringBuilder() << "my-tx-producer-" << RandomNumber<ui64>();
        TString consumerName = "my-consumer";

        // create output topic
        CreateTopic(pqClient, outputTopicName, 3, {consumerName});

        // init producer id
        auto initProducerIdResp = kafkaClient.InitProducerId(transactionalId, 30000);
        UNIT_ASSERT_VALUES_EQUAL(initProducerIdResp->ErrorCode, EKafkaErrors::NONE_ERROR);
        TProducerInstanceId producerInstanceId = {initProducerIdResp->ProducerId, initProducerIdResp->ProducerEpoch};

        // add partitions to txn
        std::unordered_map<TString, std::vector<ui32>> topicPartitionsToAddToTxn;
        topicPartitionsToAddToTxn[outputTopicName] = std::vector<ui32>{0};
        auto addPartsResponse = kafkaClient.AddPartitionsToTxn(transactionalId, producerInstanceId, topicPartitionsToAddToTxn);
        UNIT_ASSERT_VALUES_EQUAL(addPartsResponse->Results[0].Results[0].ErrorCode, EKafkaErrors::NONE_ERROR);

        // end txn
        auto endTxnResponse = kafkaClient.EndTxn(transactionalId, producerInstanceId, true);
        UNIT_ASSERT_VALUES_EQUAL(endTxnResponse->ErrorCode, EKafkaErrors::BROKER_NOT_AVAILABLE);
    }

    Y_UNIT_TEST(ProducerFencedInTransactionScenario) {
        TInsecureTestServer testServer("1", false, true);
        TKafkaTestClient kafkaClient(testServer.Port);
        NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
        // use random values to avoid parallel execution problems
        TString inputTopicName = TStringBuilder() << "input-topic-" << RandomNumber<ui64>();
        TString outputTopicName = TStringBuilder() << "output-topic-" << RandomNumber<ui64>();
        TString transactionalId = TStringBuilder() << "my-tx-producer-" << RandomNumber<ui64>();
        TString consumerName = "my-consumer";

        // create input and output topics
        CreateTopic(pqClient, inputTopicName, 3, {consumerName});
        CreateTopic(pqClient, outputTopicName, 3, {consumerName});

        // produce data to input topic (to commit offsets in further steps)
        auto inputProduceResponse = kafkaClient.Produce({inputTopicName, 0}, {{"key1", "val1"}, {"key2", "val2"}});
        UNIT_ASSERT_VALUES_EQUAL(inputProduceResponse->Responses[0].PartitionResponses[0].ErrorCode, EKafkaErrors::NONE_ERROR);

        // init producer id
        auto initProducerIdResp0 = kafkaClient.InitProducerId(transactionalId, 30000);
        UNIT_ASSERT_VALUES_EQUAL(initProducerIdResp0->ErrorCode, EKafkaErrors::NONE_ERROR);
        TProducerInstanceId producerInstanceId = {initProducerIdResp0->ProducerId, initProducerIdResp0->ProducerEpoch};

        // add partitions to txn
        std::unordered_map<TString, std::vector<ui32>> topicPartitionsToAddToTxn;
        topicPartitionsToAddToTxn[outputTopicName] = std::vector<ui32>{0, 1};
        auto addPartsResponse = kafkaClient.AddPartitionsToTxn(transactionalId, producerInstanceId, topicPartitionsToAddToTxn);
        UNIT_ASSERT_VALUES_EQUAL(addPartsResponse->Results[0].Results[0].ErrorCode, EKafkaErrors::NONE_ERROR);
        UNIT_ASSERT_VALUES_EQUAL(addPartsResponse->Results[0].Results[1].ErrorCode, EKafkaErrors::NONE_ERROR);

        // produce data
        // to part 0
        auto out0ProduceResponse = kafkaClient.Produce({outputTopicName, 0}, {{"0", "123"}}, 0, producerInstanceId, transactionalId);
        // to part 1
        // init consumer
        std::vector<TString> topicsToSubscribe;
        topicsToSubscribe.push_back(outputTopicName);
        TString protocolName = "range";
        auto consumerInfo = kafkaClient.JoinAndSyncGroupAndWaitPartitions(topicsToSubscribe, consumerName, 3, protocolName, 3, 15000);

        kafkaClient.ValidateNoDataInTopics({{outputTopicName, {0, 1}}});

        // add offsets to txn
        auto addOffsetsResponse = kafkaClient.AddOffsetsToTxn(transactionalId, producerInstanceId, consumerName);
        UNIT_ASSERT_VALUES_EQUAL(addOffsetsResponse->ErrorCode, EKafkaErrors::NONE_ERROR);

        // txn offset commit
        std::unordered_map<TString, std::vector<std::pair<ui32, ui64>>> offsetsToCommit;
        offsetsToCommit[inputTopicName] = std::vector<std::pair<ui32, ui64>>{{0, 2}};
        auto txnOffsetCommitResponse = kafkaClient.TxnOffsetCommit(transactionalId, producerInstanceId, consumerName, consumerInfo.GenerationId, offsetsToCommit);
        UNIT_ASSERT_VALUES_EQUAL(txnOffsetCommitResponse->Topics[0].Partitions[0].ErrorCode, EKafkaErrors::NONE_ERROR);

        // producer reinitialized - transaction from previous one should be fenced
        auto initProducerIdResp1 = kafkaClient.InitProducerId(transactionalId);
        UNIT_ASSERT_VALUES_EQUAL(initProducerIdResp1->ErrorCode, EKafkaErrors::NONE_ERROR);

        // end txn
        auto endTxnResponse = kafkaClient.EndTxn(transactionalId, producerInstanceId, true);
        UNIT_ASSERT_VALUES_EQUAL(endTxnResponse->ErrorCode, EKafkaErrors::PRODUCER_FENCED);

        // validate data is not accessible in target topic
        kafkaClient.ValidateNoDataInTopics({{outputTopicName, {0, 1}}});

        // validate consumer offset not committed
        std::map<TString, std::vector<i32>> topicsToPartitionsToFetch;
        topicsToPartitionsToFetch[inputTopicName] = std::vector<i32>{0};
        auto offsetFetchResponse = kafkaClient.OffsetFetch(consumerName, topicsToPartitionsToFetch);
        UNIT_ASSERT_VALUES_EQUAL(offsetFetchResponse->ErrorCode, EKafkaErrors::NONE_ERROR);
        UNIT_ASSERT_VALUES_EQUAL(offsetFetchResponse->Groups[0].Topics[0].Partitions[0].CommittedOffset, 0);
    }

    Y_UNIT_TEST(ConsumerFencedInTransactionScenario) {
        TInsecureTestServer testServer("1", false, true);
        TKafkaTestClient kafkaClient(testServer.Port);
        NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
        // use random values to avoid parallel execution problems
        TString inputTopicName = TStringBuilder() << "input-topic-" << RandomNumber<ui64>();
        TString outputTopicName = TStringBuilder() << "output-topic-" << RandomNumber<ui64>();
        TString transactionalId = TStringBuilder() << "my-tx-producer-" << RandomNumber<ui64>();
        TString consumerName = "my-consumer";

        // create input and output topics
        CreateTopic(pqClient, inputTopicName, 3, {consumerName});
        CreateTopic(pqClient, outputTopicName, 4, {consumerName});

        // produce data to input topic (to commit offsets in further steps)
        auto inputProduceResponse = kafkaClient.Produce({inputTopicName, 0}, {{"key1", "val1"}, {"key2", "val2"}});
        UNIT_ASSERT_VALUES_EQUAL(inputProduceResponse->Responses[0].PartitionResponses[0].ErrorCode, EKafkaErrors::NONE_ERROR);

        // init producer id
        auto initProducerIdResp0 = kafkaClient.InitProducerId(transactionalId, 30000);
        UNIT_ASSERT_VALUES_EQUAL(initProducerIdResp0->ErrorCode, EKafkaErrors::NONE_ERROR);
        TProducerInstanceId producerInstanceId = {initProducerIdResp0->ProducerId, initProducerIdResp0->ProducerEpoch};

        // add partitions to txn
        std::unordered_map<TString, std::vector<ui32>> topicPartitionsToAddToTxn;
        topicPartitionsToAddToTxn[outputTopicName] = std::vector<ui32>{0, 1};
        auto addPartsResponse = kafkaClient.AddPartitionsToTxn(transactionalId, producerInstanceId, topicPartitionsToAddToTxn);
        UNIT_ASSERT_VALUES_EQUAL(addPartsResponse->Results[0].Results[0].ErrorCode, EKafkaErrors::NONE_ERROR);
        UNIT_ASSERT_VALUES_EQUAL(addPartsResponse->Results[0].Results[1].ErrorCode, EKafkaErrors::NONE_ERROR);

        // produce data
        // to part 0
        auto out0ProduceResponse = kafkaClient.Produce({outputTopicName, 0}, {{"0", "123"}}, 0, producerInstanceId, transactionalId);
        UNIT_ASSERT_VALUES_EQUAL(out0ProduceResponse->Responses[0].PartitionResponses[0].ErrorCode, EKafkaErrors::NONE_ERROR);
        // to part 1
        auto out1ProduceResponse = kafkaClient.Produce({outputTopicName, 1}, {{"1", "987"}}, 0, producerInstanceId, transactionalId);
        UNIT_ASSERT_VALUES_EQUAL(out1ProduceResponse->Responses[0].PartitionResponses[0].ErrorCode, EKafkaErrors::NONE_ERROR);
        // init consumer
        std::vector<TString> topicsToSubscribe;
        topicsToSubscribe.push_back(outputTopicName);
        TString protocolName = "range";
        auto consumerInfo = kafkaClient.JoinAndSyncGroupAndWaitPartitions(topicsToSubscribe, consumerName, 4, protocolName, 4, 15000);

        kafkaClient.ValidateNoDataInTopics({{outputTopicName, {0, 1}}});

        // add offsets to txn
        auto addOffsetsResponse = kafkaClient.AddOffsetsToTxn(transactionalId, producerInstanceId, consumerName);
        UNIT_ASSERT_VALUES_EQUAL(addOffsetsResponse->ErrorCode, EKafkaErrors::NONE_ERROR);

        // txn offset commit
        std::unordered_map<TString, std::vector<std::pair<ui32, ui64>>> offsetsToCommit;
        offsetsToCommit[inputTopicName] = std::vector<std::pair<ui32, ui64>>{{0, 2}};
        auto txnOffsetCommitResponse = kafkaClient.TxnOffsetCommit(transactionalId, producerInstanceId, consumerName, consumerInfo.GenerationId, offsetsToCommit);
        UNIT_ASSERT_VALUES_EQUAL(txnOffsetCommitResponse->Topics[0].Partitions[0].ErrorCode, EKafkaErrors::NONE_ERROR);

        // consumer generation updated - transaction from previous consumer generaion should be fenced
        TKafkaTestClient kafkaClient2(testServer.Port);
        TReadInfo readInfo = kafkaClient2.JoinAndSyncGroup(topicsToSubscribe, consumerName, protocolName, 15000, 4);
        UNIT_ASSERT_VALUES_EQUAL(readInfo.GenerationId, consumerInfo.GenerationId + 1);
        kafkaClient.WaitRebalance(consumerInfo.MemberId, consumerInfo.GenerationId, consumerName);

        // end txn
        auto endTxnResponse = kafkaClient.EndTxn(transactionalId, producerInstanceId, true);
        UNIT_ASSERT_VALUES_EQUAL(endTxnResponse->ErrorCode, EKafkaErrors::PRODUCER_FENCED);

        kafkaClient.ValidateNoDataInTopics({{outputTopicName, {0, 1}}});

        // validate consumer offset not committed
        std::map<TString, std::vector<i32>> topicsToPartitionsToFetch;
        topicsToPartitionsToFetch[inputTopicName] = std::vector<i32>{0};
        auto offsetFetchResponse = kafkaClient.OffsetFetch(consumerName, topicsToPartitionsToFetch);
        UNIT_ASSERT_VALUES_EQUAL(offsetFetchResponse->ErrorCode, EKafkaErrors::NONE_ERROR);
        UNIT_ASSERT_VALUES_EQUAL(offsetFetchResponse->Groups[0].Topics[0].Partitions[0].CommittedOffset, 0);
    }

    Y_UNIT_TEST(ReadMetadataFromTopicProto) {
        TInsecureTestServer testServer("1", false, true);
        TKafkaTestClient kafkaClient(testServer.Port);
        NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
        // use random values to avoid parallel execution problems
        TString topicName = TStringBuilder()
                                << "topic-" << RandomNumber<ui64>();
        TString consumerName = "my-consumer";

        // create input and output topics
        CreateTopic(pqClient, topicName, 3, {consumerName});

        // produce data to input topic (to commit offsets in further steps)
        NYdb::NTopic::TWriteSessionSettings wsSettings;
        wsSettings.Path(topicName).ProducerId("12345").PartitionId(0);
        auto writer = pqClient.CreateSimpleBlockingWriteSession(wsSettings);
        NYdb::NTopic::TWriteMessage msg1("Data-12345");
        NYdb::NTopic::TWriteMessage msg2("Data-67890");
        msg1.MessageMeta({{"__key", "key1"}});
        msg2.MessageMeta({{"__key", "key2"}});
        writer->Write(std::move(msg1));
        writer->Write(std::move(msg2));
        writer->Close();

        // validate data is accessible in target topic
        auto fetchResponse1 = kafkaClient.Fetch({{topicName, {0}}});
        UNIT_ASSERT_VALUES_EQUAL(
            fetchResponse1->ErrorCode,
            static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        UNIT_ASSERT_VALUES_EQUAL(fetchResponse1->Responses[0].Partitions.size(), 1);

        UNIT_ASSERT_VALUES_EQUAL(
            fetchResponse1->Responses[0].Partitions[0].Records->Records.size(),
            2);
        auto record1 = fetchResponse1->Responses[0].Partitions[0].Records->Records[0];
        UNIT_ASSERT_VALUES_EQUAL(TString(record1.Key.value().data(), record1.Key.value().size()), "key1");

        auto record2 = fetchResponse1->Responses[0].Partitions[0].Records->Records[1];
        UNIT_ASSERT_VALUES_EQUAL(TString(record2.Key.value().data(), record2.Key.value().size()), "key2");
    }

    Y_UNIT_TEST(ProduceMetrics) {
        TInsecureTestServer testServer("2");

        TString topicName = "/Root/topic-0-test";

        NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
        CreateTopic(pqClient, topicName, 1, {"consumer-0"});

        auto settings = NTopic::TReadSessionSettings()
                            .AppendTopics(NTopic::TTopicReadSettings(topicName))
                            .ConsumerName("consumer-0");
        auto topicReader = pqClient.CreateReadSession(settings);

        TKafkaTestClient client(testServer.Port);

        client.ApiVersions();
        client.SaslHandshake();
        client.SaslPlainAuthenticate("ouruser@/Root", "ourUserPassword");
        client.InitProducerId();


        // send test message with bad topic name
        {
            TString key = "record-key";
            TString value = "record-value";

            TKafkaRecordBatch batch;
            batch.BaseOffset = 3;
            batch.BaseSequence = 5;
            batch.Magic = 2; // Current supported
            batch.Records.resize(1);
            batch.Records[0].Key = TKafkaRawBytes(key.data(), key.size());
            batch.Records[0].Value = TKafkaRawBytes(value.data(), value.size());

            TProduceRequestData request;
            request.Acks = -1;
            request.TopicData.resize(2);

            request.TopicData[0].Name = topicName;
            request.TopicData[0].PartitionData.resize(1);
            request.TopicData[0].PartitionData[0].Index = 0;
            request.TopicData[0].PartitionData[0].Records = batch;

            request.TopicData[1].Name = "/Root/topic-0-test-not-exists";
            request.TopicData[1].PartitionData.resize(1);
            request.TopicData[1].PartitionData[0].Index = 0;
            request.TopicData[1].PartitionData[0].Records = batch;

            TRequestHeaderData header = client.Header(NKafka::EApiKey::PRODUCE, 9);
            auto msg = client.WriteAndRead<TProduceResponseData>(header, request);

            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].PartitionResponses[0].ErrorCode,
                                     static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[1].PartitionResponses[0].ErrorCode,
                                     static_cast<TKafkaInt16>(EKafkaErrors::UNKNOWN_TOPIC_OR_PARTITION));
        }

        {
            auto sender = testServer.KikimrServer->GetRuntime()->AllocateEdgeActor();
            testServer.KikimrServer->GetRuntime()->Send(MakeKafkaMetricsServiceID(), sender, new TEvKafka::TEvGetCountersRequest());

            TAutoPtr<IEventHandle> handle;
            auto ev = testServer.KikimrServer->GetRuntime()->GrabEdgeEvents<TEvKafka::TEvGetCountersResponse>(handle, TDuration::Seconds(1));
            auto* event = std::get<TEvKafka::TEvGetCountersResponse*>(ev);
            UNIT_ASSERT_C(event, "No counters");

            TStringStream sb;
            event->Counters->OutputPlainText(sb, "  ");
            Cerr << ">>>>> COUNTERS '" << sb.Str() << "'" << Endl;

            auto getCounterValue = [&](auto& topic, auto& counter) {
                return event->Counters->GetSubgroup("counters", "datastreams")
                    ->GetSubgroup("database", "/Root")
                    ->GetSubgroup("cloud_id", "somecloud")
                    ->GetSubgroup("folder_id", "somefolder")
                    ->GetSubgroup("database_id", "root")
                    ->GetSubgroup("topic", topic)
                    ->GetNamedCounter("name", counter, true)
                    ->Val();
            };

            // expected:
            //
            // counters=datastreams:
            //     database=/Root:
            //         cloud_id=somecloud:
            //             folder_id=somefolder:
            //                 database_id=root:
            //                     topic=topic-0-test:
            //                         name=api.kafka.produce.successful_messages: 1
            //                         name=api.kafka.produce.total_messages: 1
            //                     topic=unknown:
            //                         name=api.kafka.produce.failed_messages: 1
            //                         name=api.kafka.produce.total_messages: 1
            UNIT_ASSERT_VALUES_EQUAL(getCounterValue("topic-0-test", "api.kafka.produce.successful_messages"), 1);
            UNIT_ASSERT_VALUES_EQUAL(getCounterValue("topic-0-test", "api.kafka.produce.total_messages"), 1);

            UNIT_ASSERT_VALUES_EQUAL(getCounterValue("unknown", "api.kafka.produce.failed_messages"), 1);
            UNIT_ASSERT_VALUES_EQUAL(getCounterValue("unknown", "api.kafka.produce.total_messages"), 1);
        }
    } // Y_UNIT_TEST(ProduceScenario)
} // Y_UNIT_TEST_SUITE(KafkaProtocol)
