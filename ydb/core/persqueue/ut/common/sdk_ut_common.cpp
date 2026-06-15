#include "sdk_ut_common.h"

#include <ydb/core/protos/feature_flags.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash_set.h>

namespace NKikimr::NPQ::NTest {

void EnableTopicBatching(Tests::TServerSettings& settings) {
    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableTopicMessagesBatching(true);
    featureFlags.SetEnableTopicWriteOffsetDeltaInKeys(true);
    settings.SetFeatureFlags(featureFlags);
}

void WriteKafkaBatchMessages(
    NYdb::NTopic::TTopicClient& client,
    const TString& topicPath,
    const TString& producerId,
    size_t dataSize,
    ui64 maxBatchMessageCount,
    const TVector<std::tuple<ui64, ui32, char>>& writes,
    bool directWriteToPartition)
{
    NYdb::NTopic::TWriteSessionSettings writeSettings;
    writeSettings.Path(topicPath)
        .ProducerId(producerId)
        .MessageGroupId(producerId)
        .PartitionId(0)
        .Codec(NYdb::NTopic::ECodec::RAW)
        .BatchFlushMessageCount(maxBatchMessageCount)
        .MessageFormat(NYdb::NTopic::EMessageFormat::KAFKA_BATCH)
        .DirectWriteToPartition(directWriteToPartition)
        .BatchFlushInterval(TDuration::Seconds(1))
        .BatchFlushSizeBytes(10_MB);

    auto writeSession = client.CreateWriteSession(writeSettings);

    std::optional<NYdb::NTopic::TContinuationToken> token;

    for (const auto& [firstSeqNo, messageCount, fill] : writes) {
        const ui64 lastSeqNo = firstSeqNo + messageCount - 1;
        ui64 nextSeqNo = firstSeqNo;
        THashSet<ui64> ackedSeqNos;

        while (ackedSeqNos.size() < messageCount) {
            if (token.has_value() && nextSeqNo <= lastSeqNo) {
                NYdb::NTopic::TWriteMessage message(TString(dataSize, fill));
                message.SeqNo(nextSeqNo++);
                writeSession->Write(std::move(*token), std::move(message));
                token.reset();
                continue;
            }

            auto event = writeSession->GetEvent(true);
            UNIT_ASSERT(event.has_value());
            if (auto* ready = std::get_if<NYdb::NTopic::TWriteSessionEvent::TReadyToAcceptEvent>(&*event)) {
                token = std::move(ready->ContinuationToken);
            } else if (auto* ack = std::get_if<NYdb::NTopic::TWriteSessionEvent::TAcksEvent>(&*event)) {
                for (const auto& item : ack->Acks) {
                    UNIT_ASSERT_C(
                        item.State == NYdb::NTopic::TWriteSessionEvent::TWriteAck::EES_WRITTEN,
                        TStringBuilder() << "Unexpected ack state: " << item.State);
                    UNIT_ASSERT_C(
                        item.SeqNo >= firstSeqNo && item.SeqNo <= lastSeqNo,
                        TStringBuilder() << "Unexpected ack seqNo: " << item.SeqNo);
                    ackedSeqNos.insert(item.SeqNo);
                }
            } else if (auto* closed = std::get_if<NYdb::NTopic::TSessionClosedEvent>(&*event)) {
                UNIT_FAIL(TStringBuilder() << "Unexpected session close: " << closed->DebugString());
            }
        }
    }

    UNIT_ASSERT(writeSession->Close(TDuration::Seconds(5)));
}

} // namespace NKikimr::NPQ::NTest
