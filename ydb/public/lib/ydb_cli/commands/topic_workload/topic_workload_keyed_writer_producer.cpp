#include "topic_workload_keyed_writer_producer.h"

#include <util/random/random.h>

using namespace NYdb::NConsoleClient;

TTopicWorkloadKeyedWriterProducer::TTopicWorkloadKeyedWriterProducer(
    const TTopicWorkloadKeyedWriterParams& params,
    std::shared_ptr<TTopicWorkloadStatsCollector> statsCollector,
    const TString& producerId,
    const NUnifiedAgent::TClock& clock
)
    : MessageId_(1)
    , ProducerId_(producerId)
    , Params_(params)
    , StatsCollector_(std::move(statsCollector))
    , Clock_(clock)
{
    WRITE_LOG(Params_.Log, ELogPriority::TLOG_INFO,
              TStringBuilder() << "Created keyed producer with id " << ProducerId_);
}

void TTopicWorkloadKeyedWriterProducer::SetWriteSession(std::shared_ptr<NYdb::NTopic::IKeyedWriteSession> writeSession)
{
    WriteSession_ = std::move(writeSession);
    InflightMessagesCount_.store(0);
}

void TTopicWorkloadKeyedWriterProducer::Send(const TInstant& createTimestamp,
                                             std::optional<NYdb::NTable::TTransaction> transaction)
{
    Y_ASSERT(WriteSession_);

    const TString data = GetGeneratedMessage();
    const std::string key = GenerateKeyValue();

    InflightMessagesCreateTs_.Insert(MessageId_, createTimestamp);
    InflightMessagesCount_.fetch_add(1, std::memory_order_relaxed);

    NYdb::NTopic::TWriteMessage writeMessage(data);
    writeMessage.SeqNo(MessageId_);
    writeMessage.CreateTimestamp(createTimestamp);
    writeMessage.MessageMeta(GenerateMessageMeta(key));

    if (transaction.has_value()) {
        writeMessage.Tx(transaction.value());
    }

    TTransactionBase* txPtr = writeMessage.GetTxPtr();
    auto continuationToken = GetContinuationToken();
    WriteSession_->Write(std::move(continuationToken), key, std::move(writeMessage), txPtr);

    WRITE_LOG(Params_.Log, ELogPriority::TLOG_DEBUG,
              TStringBuilder() << "Sent keyed message with id " << MessageId_
                               << " for producer " << ProducerId_
                               << " in writer " << Params_.WriterIdx);

    ++MessageId_;
}

void TTopicWorkloadKeyedWriterProducer::Close()
{
    if (WriteSession_) {
        WriteSession_->Close(TDuration::Zero());
    }
}

TString TTopicWorkloadKeyedWriterProducer::GetGeneratedMessage() const
{
    return Params_.GeneratedMessages[MessageId_ % TTopicWorkloadKeyedWriterWorker::GENERATED_MESSAGES_COUNT];
}

std::string TTopicWorkloadKeyedWriterProducer::GenerateKeyValue() const
{
    // If key prefix is provided, keep it compatible with non-keyed writer's "__key" meta:
    // {prefix}.{(messageId + seed) % keyCount}
    if (Params_.KeyPrefix.Defined()) {
        TStringBuilder sb;
        sb << *Params_.KeyPrefix;
        if (Params_.KeyCount > 0) {
            sb << '.' << ((MessageId_ + Params_.KeySeed) % Params_.KeyCount);
        } else {
            sb << '.' << MessageId_;
        }
        return sb;
    }

    // Otherwise generate a reasonably distributed key.
    // If KeyCount is set, still limit to that many distinct keys.
    if (Params_.KeyCount > 0) {
        return ToString((MessageId_ + Params_.KeySeed) % Params_.KeyCount);
    }

    const ui64 rnd = RandomNumber<ui64>();
    return ToString(rnd);
}

NYdb::NTopic::TWriteMessage::TMessageMeta TTopicWorkloadKeyedWriterProducer::GenerateMessageMeta(const std::string& key) const
{
    NYdb::NTopic::TWriteMessage::TMessageMeta meta;
    // Keep the same meta key name as non-keyed writer for debugging/compatibility.
    meta.emplace_back("__key", key);
    return meta;
}

void TTopicWorkloadKeyedWriterProducer::WaitForContinuationToken(const TDuration& timeout)
{
    auto deadline = Clock_.Now() + timeout;
    while (!HasContinuationTokens()) {
        WRITE_LOG(Params_.Log, ELogPriority::TLOG_DEBUG, TStringBuilder()
            << "WriterId " << Params_.WriterIdx
            << " keyed producer id " << ProducerId_
            << ": WaitEvent for timeToNextMessage " << timeout);

        const bool foundEvent = WriteSession_->WaitEvent().Wait(deadline);
        WRITE_LOG(Params_.Log, ELogPriority::TLOG_DEBUG, TStringBuilder()
            << "Keyed producer " << ProducerId_
            << " in writer " << Params_.WriterIdx
            << ": foundEvent - " << foundEvent);

        if (!foundEvent) {
            return;
        }
    }
}

void TTopicWorkloadKeyedWriterProducer::HandleAckEvent(NYdb::NTopic::TWriteSessionEvent::TAcksEvent& event)
{
    const auto now = Clock_.Now();
    for (const auto& ack : event.Acks) {
        const ui64 ackedMessageId = ack.SeqNo;
        WRITE_LOG(Params_.Log, ELogPriority::TLOG_DEBUG, TStringBuilder() << "Got ack for keyed write " << ackedMessageId);

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

        WRITE_LOG(Params_.Log, ELogPriority::TLOG_DEBUG,
                  TStringBuilder() << "Ack PartitionId " << ack.Details->PartitionId << " Offset "
                                   << ack.Details->Offset << " InflightTime " << inflightTime << " WriteTime "
                                   << ack.Stat->WriteTime);
    }
}

void TTopicWorkloadKeyedWriterProducer::HandleReadyToAcceptEvent(NYdb::NTopic::TWriteSessionEvent::TReadyToAcceptEvent& event)
{
    std::lock_guard lk(Lock_);
    ContinuationTokens_.push(std::move(event.ContinuationToken));
}

void TTopicWorkloadKeyedWriterProducer::HandleSessionClosed(const NYdb::NTopic::TSessionClosedEvent& event)
{
    WRITE_LOG(Params_.Log, ELogPriority::TLOG_DEBUG, TStringBuilder()
        << "Keyed producer " << ProducerId_
        << ": got close event: " << event.DebugString());
}

bool TTopicWorkloadKeyedWriterProducer::HasContinuationTokens()
{
    std::lock_guard lk(Lock_);
    return !ContinuationTokens_.empty();
}

NYdb::NTopic::TContinuationToken TTopicWorkloadKeyedWriterProducer::GetContinuationToken()
{
    std::lock_guard lk(Lock_);
    auto token = std::move(ContinuationTokens_.front());    
    ContinuationTokens_.pop();
    return token;
}

ui64 TTopicWorkloadKeyedWriterProducer::GetCurrentMessageId() const
{
    return MessageId_;
}

size_t TTopicWorkloadKeyedWriterProducer::InflightMessagesCnt() const
{
    return InflightMessagesCount_.load(std::memory_order_relaxed);
}

