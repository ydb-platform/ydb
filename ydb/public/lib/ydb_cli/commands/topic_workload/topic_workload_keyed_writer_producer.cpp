#include "topic_workload_keyed_writer_producer.h"
#include "topic_workload_writer_producer_utils.h"

#include <util/generic/guid.h>

using namespace NYdb::NConsoleClient;

TTopicWorkloadKeyedWriterProducer::TTopicWorkloadKeyedWriterProducer(
    const TTopicWorkloadKeyedWriterParams& params,
    std::shared_ptr<TTopicWorkloadStatsCollector> statsCollector,
    const TString& producerId,
    const std::string& sessionId,
    const NUnifiedAgent::TClock& clock
)
    : MessageId_(1)
    , ProducerId_(producerId)
    , SessionId_(sessionId)
    , Params_(params)
    , StatsCollector_(std::move(statsCollector))
    , Clock_(clock)
    , KeyPrefix_("message_key")
    , KeyId_(0)
{
    WRITE_LOG(Params_.Log, ELogPriority::TLOG_INFO,
              TStringBuilder() << "Created keyed producer with id " << ProducerId_);
}

void TTopicWorkloadKeyedWriterProducer::SetProducer(std::shared_ptr<NYdb::NTopic::IProducer> producer)
{
    Producer_ = producer;
    InflightMessagesCount_.store(0);
}

std::string TTopicWorkloadKeyedWriterProducer::GetKey() const
{
    if (Params_.ProducerKeysCount > 0) {
        return std::format("{}_{}", KeyPrefix_, KeyId_);
    }

    return TGUID::CreateTimebased().AsGuidString();
}

void TTopicWorkloadKeyedWriterProducer::Send(const TInstant&,
                                             std::optional<NYdb::NTable::TTransaction> transaction)
{
    Y_ASSERT(Producer_);

    const TString data = NYdb::NConsoleClient::NTopicWorkloadWriterInternal::GetGeneratedMessage(Params_, MessageId_);
    const std::string key = GetKey();

    const TInstant enqueueTimestamp = Clock_.Now();
    InflightMessagesCreateTs_.Insert(MessageId_, enqueueTimestamp);
    InflightMessagesCount_.fetch_add(1, std::memory_order_relaxed);

    NYdb::NTopic::TWriteMessage writeMessage(data);
    writeMessage.SeqNo(MessageId_);
    writeMessage.CreateTimestamp(enqueueTimestamp);
    writeMessage.MessageMeta(NYdb::NConsoleClient::NTopicWorkloadWriterInternal::MakeKeyMeta(key));
    writeMessage.Key(key);

    if (transaction.has_value()) {
        writeMessage.Tx(transaction.value());
    }

    auto result = Producer_->Write(std::move(writeMessage));
    if (!result.IsQueued()) {
        TStringBuilder errorMessage;
        errorMessage << "Failed to write message with id " << MessageId_
                     << " for producer " << ProducerId_
                     << " in writer " << Params_.WriterIdx;

        if (result.ErrorMessage) {
            errorMessage << " error message: " << *result.ErrorMessage;
        }
        if (result.ClosedDescription) {
            errorMessage << " closed description: " << result.ClosedDescription->DebugString();
        }
        WRITE_LOG(Params_.Log, ELogPriority::TLOG_ERR, errorMessage);
    }
    Y_ASSERT(result.IsQueued());

    WRITE_LOG(Params_.Log, ELogPriority::TLOG_DEBUG,
              TStringBuilder() << "Sent keyed message with id " << MessageId_
                               << " for producer " << ProducerId_
                               << " in writer " << Params_.WriterIdx);

    ++MessageId_;

    if (Params_.ProducerKeysCount > 0 && ++KeyId_ >= Params_.ProducerKeysCount) {
        KeyId_ = 0;
    }
}

void TTopicWorkloadKeyedWriterProducer::Close()
{
    if (Producer_) {
        Y_ASSERT(Producer_->Close(TDuration::Seconds(5)).IsSuccess());
    }
}

void TTopicWorkloadKeyedWriterProducer::HandleAckEvent(NYdb::NTopic::TWriteSessionEvent::TAcksEvent& event)
{
    const auto now = Clock_.Now();
    for (const auto& ack : event.Acks) {
        const ui64 ackedMessageId = ack.SeqNo;
        Y_ABORT_UNLESS(ackedMessageId == ++AckedMessageId_, "AckedMessageId %d is not in sequence, expected %d", ackedMessageId, AckedMessageId_);

        TInstant createTimestamp = now;
        if (InflightMessagesCreateTs_.TryRemove(ackedMessageId, createTimestamp)) {
            InflightMessagesCount_.fetch_sub(1, std::memory_order_relaxed);
        } else {
            *Params_.ErrorFlag = 1;
            WRITE_LOG(Params_.Log, ELogPriority::TLOG_ERR,
                      TStringBuilder() << "Unknown AckedMessageId " << ackedMessageId);
        }

        const auto inflightTime = (now - createTimestamp);
        StatsCollector_->AddWriterEvent(Params_.WriterIdx,
                                        {Params_.MessageSize, inflightTime.MilliSeconds(), InflightMessagesCnt()});
    }
}

void TTopicWorkloadKeyedWriterProducer::HandleSessionClosed(const NYdb::NTopic::TSessionClosedEvent& event)
{
    WRITE_LOG(Params_.Log, ELogPriority::TLOG_DEBUG, TStringBuilder()
        << "Keyed producer " << ProducerId_
        << ": got close event: " << event.DebugString());
}

ui64 TTopicWorkloadKeyedWriterProducer::GetCurrentMessageId() const
{
    return MessageId_;
}

size_t TTopicWorkloadKeyedWriterProducer::InflightMessagesCnt() const
{
    return InflightMessagesCount_.load(std::memory_order_relaxed);
}