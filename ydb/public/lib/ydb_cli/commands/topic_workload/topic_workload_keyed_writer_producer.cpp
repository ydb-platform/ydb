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

void TTopicWorkloadKeyedWriterProducer::SetWriteSession(std::shared_ptr<NYdb::NTopic::IKeyedWriteSession> writeSession)
{
    WriteSession_ = std::move(writeSession);
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
    Y_ASSERT(WriteSession_);

    const TString data = NYdb::NConsoleClient::NTopicWorkloadWriterInternal::GetGeneratedMessage(Params_, MessageId_);
    const std::string key = GetKey();

    // Для внутренних метрик задержки записи нам важно измерять время от фактической
    // постановки сообщения в клиенскую очередь до прихода ack, а не "идеальное"
    // время генерации нагрузки. Поэтому для CreateTimestamp и внутренних метрик
    // используем текущее время, а не ожидаемое время генерации из workload-а.
    const TInstant enqueueTimestamp = Clock_.Now();
    InflightMessagesCreateTs_.Insert(MessageId_, enqueueTimestamp);
    InflightMessagesCount_.fetch_add(1, std::memory_order_relaxed);

    NYdb::NTopic::TWriteMessage writeMessage(data);
    writeMessage.SeqNo(MessageId_);
    writeMessage.CreateTimestamp(enqueueTimestamp);
    writeMessage.MessageMeta(NYdb::NConsoleClient::NTopicWorkloadWriterInternal::MakeKeyMeta(key));

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

    if (Params_.ProducerKeysCount > 0 && ++KeyId_ >= Params_.ProducerKeysCount) {
        KeyId_ = 0;
    }
}

void TTopicWorkloadKeyedWriterProducer::Close()
{
    if (WriteSession_) {
        WriteSession_->Close(TDuration::Zero());
    }
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

        WRITE_LOG(Params_.Log, ELogPriority::TLOG_ERR,
                  TStringBuilder() << "Ack PartitionId " << ack.Details->PartitionId << " Offset "
                                   << ack.Details->Offset << " InflightTime " << inflightTime << " WriteTime "
                                   << ack.Stat->WriteTime << " SessionId " << SessionId_ << " ack SeqNo " << ack.SeqNo);
    }
}

void TTopicWorkloadKeyedWriterProducer::HandleReadyToAcceptEvent(NYdb::NTopic::TWriteSessionEvent::TReadyToAcceptEvent& event)
{
    std::lock_guard lk(Lock_);
    ContinuationTokens_.push(std::move(event.ContinuationToken));
    WRITE_LOG(Params_.Log, ELogPriority::TLOG_DEBUG, TStringBuilder()
        << "Producer " << ProducerId_
        << " in writer " << Params_.WriterIdx
        << ": Got new ContinuationToken token");
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