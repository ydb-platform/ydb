#include "topic_workload_writer_producer.h"
#include "topic_workload_writer.h"

using namespace NYdb::NConsoleClient;

TTopicWorkloadWriterProducer::TTopicWorkloadWriterProducer(
        const TTopicWorkloadWriterParams& params,
        std::shared_ptr<NYdb::NConsoleClient::TTopicWorkloadStatsCollector> statsCollector,
        const TString& producerId,
        const ui64 partitionId,
        const NUnifiedAgent::TClock& clock
) :
        MessageId_(1),
        ProducerId_(producerId),
        PartitionId_(partitionId),
        Params_(params),
        StatsCollector_(statsCollector),
        Clock_(clock) {
    WRITE_LOG(Params_.Log, ELogPriority::TLOG_INFO,
              TStringBuilder() << "Created Producer with id " << ProducerId_ << " for partition " << PartitionId_);
}

void TTopicWorkloadWriterProducer::SetWriteSession(std::shared_ptr<NYdb::NTopic::IWriteSession> writeSession) {
    WriteSession_ = writeSession;
}

void TTopicWorkloadWriterProducer::Send(const TInstant& createTimestamp,
                                        std::optional<NYdb::NTable::TTransaction> transaction) {
    Y_ASSERT(WriteSession_);

    TString data = GetGeneratedMessage();
    InflightMessagesCreateTs_[MessageId_] = createTimestamp;

    NTopic::TWriteMessage writeMessage(data);
    writeMessage.SeqNo(MessageId_);
    writeMessage.CreateTimestamp(createTimestamp);

    if (transaction.has_value()) {
        writeMessage.Tx(transaction.value());
    }

    WriteSession_->Write(std::move(*ContinuationToken_), std::move(writeMessage));

    WRITE_LOG(Params_.Log, ELogPriority::TLOG_DEBUG,
              TStringBuilder() << "Sent message with id " << MessageId_
                << " for producer " << ProducerId_
                << " in writer " << Params_.WriterIdx
                );

    MessageId_++;
}

void TTopicWorkloadWriterProducer::Close() {
    if (WriteSession_)
        WriteSession_->Close(TDuration::Zero());
}


TString TTopicWorkloadWriterProducer::GetGeneratedMessage() const {
    return Params_.GeneratedMessages[MessageId_ % TTopicWorkloadWriterWorker::GENERATED_MESSAGES_COUNT];
}

bool TTopicWorkloadWriterProducer::WaitForInitSeqNo() {
    Y_ASSERT(WriteSession_);

    NThreading::TFuture<ui64> InitSeqNo = WriteSession_->GetInitSeqNo();
    while (!*Params_.ErrorFlag) {
        if (!InitSeqNo.HasValue() && !InitSeqNo.Wait(TDuration::Seconds(1))) {
            WRITE_LOG(Params_.Log, ELogPriority::TLOG_WARNING,
                      TStringBuilder() << "No initial sequence number for ProducerId " << ProducerId_ << " PartitionId "
                                       << PartitionId_);
            Sleep(TDuration::Seconds(1));
            continue;
        }
        if (InitSeqNo.HasException()) {
            try {
                InitSeqNo.GetValue();
            } catch (const yexception& e) {
                WRITE_LOG(Params_.Log, ELogPriority::TLOG_ERR, TStringBuilder()
                    << "Producer " << ProducerId_
                    << " in writer " << Params_.WriterIdx
                    << " for partition " << PartitionId_
                    << ". Future exception: " << e.what());
            }
            *Params_.ErrorFlag = 1;
            return false;
        }

        WRITE_LOG(Params_.Log, ELogPriority::TLOG_DEBUG,
                  TStringBuilder() << "Sequence number initialized " << InitSeqNo.GetValue());
        if (MessageId_ != InitSeqNo.GetValue() + 1) {
            MessageId_ = InitSeqNo.GetValue() + 1;
        }

        return true;
    }

    return false;
}

void TTopicWorkloadWriterProducer::WaitForContinuationToken(const TDuration& timeout) {
    WRITE_LOG(Params_.Log, ELogPriority::TLOG_DEBUG, TStringBuilder()
            << "WriterId " << Params_.WriterIdx
            << " producer id " << ProducerId_
            << " for partition " << PartitionId_
            << ": WaitEvent for timeToNextMessage " << timeout);

    // only TReadyToAcceptEvent will come here, cause we subscribed for other event types in constructor
    // we are waiting for this event, cause we can't proceed without ContinuationToken from previous write
    bool foundEvent = WriteSession_->WaitEvent().Wait(timeout);

    WRITE_LOG(Params_.Log, ELogPriority::TLOG_DEBUG, TStringBuilder()
            << "Producer " << ProducerId_
            << " in writer " << Params_.WriterIdx
            << " for partition " << PartitionId_
            << ": foundEvent - " << foundEvent);

    if (foundEvent) {
        auto variant = WriteSession_->GetEvent(true).GetRef();
        if (std::holds_alternative<NYdb::NTopic::TWriteSessionEvent::TReadyToAcceptEvent>(variant)) {
            auto event = std::get<NYdb::NTopic::TWriteSessionEvent::TReadyToAcceptEvent>(variant);
            ContinuationToken_ = std::move(event.ContinuationToken);
            WRITE_LOG(Params_.Log, ELogPriority::TLOG_DEBUG, TStringBuilder()
                              << "Producer " << ProducerId_
                              << " in writer " << Params_.WriterIdx
                              << " for partition " << PartitionId_
                              << ": Got new ContinuationToken token");
        } else {
            ythrow yexception() << "Unexpected event type in WaitForContinuationToken";
        }
    }
}

void TTopicWorkloadWriterProducer::HandleAckEvent(NYdb::NTopic::TWriteSessionEvent::TAcksEvent& event) {
    auto now = Clock_.Now();
    //! Acks just confirm that message was received and saved by server
    //! successfully. Here we just count acked messages to check, that everything
    //! written is confirmed.
    for (const auto& ack: event.Acks) {
        ui64 AckedMessageId = ack.SeqNo;
        WRITE_LOG(Params_.Log, ELogPriority::TLOG_DEBUG, TStringBuilder() << "Got ack for write " << AckedMessageId);

        auto inflightMessageIter = InflightMessagesCreateTs_.find(AckedMessageId);
        if (inflightMessageIter == InflightMessagesCreateTs_.end()) {
            *Params_.ErrorFlag = 1;
            WRITE_LOG(Params_.Log, ELogPriority::TLOG_ERR,
                      TStringBuilder() << "Unknown AckedMessageId " << AckedMessageId);
        }

        auto inflightTime = (now - inflightMessageIter->second);
        InflightMessagesCreateTs_.erase(inflightMessageIter);

        StatsCollector_->AddWriterEvent(Params_.WriterIdx, {Params_.MessageSize, inflightTime.MilliSeconds(),
                                                          InflightMessagesCreateTs_.size()});

        WRITE_LOG(Params_.Log, ELogPriority::TLOG_DEBUG,
                  TStringBuilder() << "Ack PartitionId " << ack.Details->PartitionId << " Offset "
                                   << ack.Details->Offset << " InflightTime " << inflightTime << " WriteTime "
                                   << ack.Stat->WriteTime << " MinTimeInPartitionQueue "
                                   << ack.Stat->MinTimeInPartitionQueue << " MaxTimeInPartitionQueue "
                                   << ack.Stat->MaxTimeInPartitionQueue << " PartitionQuotedTime "
                                   << ack.Stat->PartitionQuotedTime << " TopicQuotedTime "
                                   << ack.Stat->TopicQuotedTime);
    }
}

void TTopicWorkloadWriterProducer::HandleSessionClosed(const NYdb::NTopic::TSessionClosedEvent& event) {
    WRITE_LOG(Params_.Log, ELogPriority::TLOG_DEBUG, TStringBuilder()
        << "Producer " << ProducerId_
        << ": got close event: " << event.DebugString());
}

bool TTopicWorkloadWriterProducer::ContinuationTokenDefined() {
    return !!ContinuationToken_;
}

ui64 TTopicWorkloadWriterProducer::GetCurrentMessageId() {
    return MessageId_;
}

ui64 TTopicWorkloadWriterProducer::GetPartitionId() {
    return PartitionId_;
}

size_t TTopicWorkloadWriterProducer::InflightMessagesCnt() {
    return InflightMessagesCreateTs_.size();
}
