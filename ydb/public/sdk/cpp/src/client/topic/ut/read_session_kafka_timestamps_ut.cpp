#include "ut_utils/topic_sdk_test_setup.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/codecs.h>
#include <ydb/public/sdk/cpp/src/library/kafka/kafka_messages_int.h>
#include <ydb/public/sdk/cpp/src/library/kafka/kafka_records.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace NYdb::inline Dev::NTopic::NTests {

    Y_UNIT_TEST_SUITE(ReadSessionKafkaTimestamps) {
        Y_UNIT_TEST(ReadKafkaBatchesWithWrappingTimestamps) {
            TTopicSdkTestSetup setup{TEST_CASE_NAME};
            auto client = setup.MakeClient();
            const TDuration timeout = TDuration::Seconds(30);
            // Store Kafka bytes under a test codec so the server does not cut the
            // batch before it reaches the reader's Kafka metadata handling.
            TCodecMap::GetTheCodecMap().Set(static_cast<ui32>(ECodec::CUSTOM), std::make_unique<TKafkaBatchCodec>());
            const auto altered = client.AlterTopic(setup.GetTopicPath(), TAlterTopicSettings()
                                                                             .SetSupportedCodecs({ECodec::CUSTOM})
                                                                             .ClientTimeout(timeout))
                                     .GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(altered.GetStatus(), EStatus::SUCCESS, altered.GetIssues().ToString());

            constexpr i64 minTimestamp = std::numeric_limits<i64>::min();
            constexpr i64 maxTimestamp = std::numeric_limits<i64>::max();
            const std::vector<std::string> payloads = {"small message", std::string(512_KB + 1, 'x')};
            std::vector<TInstant> expectedTimestamps;
            auto writeSession = client.CreateSimpleBlockingWriteSession(TWriteSessionSettings()
                                                                            .Path(setup.GetTopicPath())
                                                                            .ProducerId("timestamp-producer")
                                                                            .MessageGroupId("timestamp-producer")
                                                                            .Codec(ECodec::RAW));
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
                        UNIT_ASSERT(writeSession->Write(std::move(message), nullptr, timeout));
                        ++nextSequence;
                    }
                }
            }
            UNIT_ASSERT(writeSession->Close(timeout));

            size_t received = 0;
            auto result = setup.Read(setup.GetTopicPath(), setup.GetConsumerName(),
                                     [&](TReadSessionEvent::TDataReceivedEvent& event) {
                                         for (const auto& message : event.GetMessages()) {
                                             UNIT_ASSERT(received < expectedTimestamps.size());
                                             UNIT_ASSERT(!message.HasException());
                                             UNIT_ASSERT_VALUES_EQUAL(message.GetData(), payloads[received % payloads.size()]);
                                             UNIT_ASSERT_VALUES_EQUAL(message.GetOffset(), received);
                                             UNIT_ASSERT_VALUES_EQUAL(message.GetSeqNo(), received + 1);
                                             UNIT_ASSERT_VALUES_EQUAL(message.GetCreateTime(), expectedTimestamps[received]);
                                             ++received;
                                         }
                                         return received < expectedTimestamps.size();
                                     }, std::nullopt, timeout);
            UNIT_ASSERT(!result.Timeout);
            UNIT_ASSERT_VALUES_EQUAL(received, expectedTimestamps.size());
            UNIT_ASSERT(result.Reader->Close(timeout));
            UNIT_ASSERT_VALUES_EQUAL(result.Reader->GetCounters()->MessagesRead->Val(), received);
        }
    } // Y_UNIT_TEST_SUITE(ReadSessionKafkaTimestamps)

} // namespace NYdb::inline Dev::NTopic::NTests
