#include "sdk_ut_common.h"

#include <ydb/core/protos/feature_flags.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash.h>
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
    bool directWriteToPartition,
    TInstant baseCreateTimestamp)
{
    NYdb::NTopic::TWriteSessionSettings writeSettings;
    writeSettings.Path(topicPath)
        .ProducerId(producerId)
        .MessageGroupId(producerId)
        .PartitionId(0)
        .Codec(NYdb::NTopic::ECodec::KAFKA_BATCH)
        .BatchFlushMessageCount(maxBatchMessageCount)
        .DirectWriteToPartition(directWriteToPartition)
        .BatchFlushInterval(TDuration::Seconds(1))
        .BatchFlushSizeBytes(10_MB);

    auto writeSession = client.CreateWriteSession(writeSettings);

    std::optional<NYdb::NTopic::TContinuationToken> token;
    const auto getEvent = [&]() {
        UNIT_ASSERT_C(writeSession->WaitEvent().Wait(TDuration::Seconds(10)),
            "WriteKafkaBatchMessages: write session event timeout");
        auto event = writeSession->GetEvent(false);
        UNIT_ASSERT_C(event.has_value(), "WriteKafkaBatchMessages: empty write session event after wait");
        return event;
    };

    for (const auto& [firstSeqNo, messageCount, fill] : writes) {
        const ui64 lastSeqNo = firstSeqNo + messageCount - 1;
        ui64 nextSeqNo = firstSeqNo;
        THashSet<ui64> ackedSeqNos;
        THashMap<ui64, TString> inFlightData;
        while (ackedSeqNos.size() < messageCount) {
            if (token.has_value() && nextSeqNo <= lastSeqNo) {
                const auto [it, inserted] = inFlightData.emplace(nextSeqNo, TString(dataSize, fill));
                UNIT_ASSERT(inserted);
                NYdb::NTopic::TWriteMessage message(it->second);
                message.SeqNo(nextSeqNo);
                message.CreateTimestamp(baseCreateTimestamp + TDuration::MilliSeconds(nextSeqNo - 1));
                ++nextSeqNo;
                writeSession->Write(std::move(*token), std::move(message));
                token.reset();
                continue;
            }

            auto event = getEvent();
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
                    inFlightData.erase(item.SeqNo);
                }
            } else if (auto* closed = std::get_if<NYdb::NTopic::TSessionClosedEvent>(&*event)) {
                UNIT_FAIL(TStringBuilder() << "Unexpected session close: " << closed->DebugString());
            }
        }
    }

    UNIT_ASSERT(writeSession->Close(TDuration::Seconds(5)));
}

} // namespace NKikimr::NPQ::NTest
