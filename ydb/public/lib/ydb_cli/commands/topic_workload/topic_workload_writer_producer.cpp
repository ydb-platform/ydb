#include "topic_workload_writer_producer.h"
#include "topic_workload_writer.h"

using namespace NYdb::NConsoleClient;

TTopicWorkloadWriterProducer::TTopicWorkloadWriterProducer(
        const TTopicWorkloadWriterParams& params,
        std::shared_ptr<NYdb::NConsoleClient::TTopicWorkloadStatsCollector> statsCollector,
        const TString &producerId,
        const ui64 partitionId
) :
        MessageId(1),
        ProducerId(producerId),
        PartitionId(partitionId),
        Params(params),
        StatsCollector(statsCollector) {
    NYdb::NTopic::TWriteSessionSettings settings;
    settings.Codec((NYdb::NTopic::ECodec) Params.Codec);
    settings.Path(Params.TopicName);
    settings.ProducerId(ProducerId);

    NYdb::NTopic::TWriteSessionSettings::TEventHandlers eventHandlers;
    eventHandlers.AcksHandler([this](auto &&PH1) { HandleAckEvent(std::forward<decltype(PH1)>(PH1)); });
    eventHandlers.SessionClosedHandler(
            std::bind(&TTopicWorkloadWriterProducer::HandleSessionClosed, this, std::placeholders::_1));
    settings.EventHandlers(eventHandlers);

    if (Params.UseAutoPartitioning) {
        settings.MessageGroupId(ProducerId);
    } else {
        settings.PartitionId(PartitionId);
    }

    settings.DirectWriteToPartition(Params.Direct);

    WriteSession = NYdb::NTopic::TTopicClient(Params.Driver).CreateWriteSession(settings);

    WRITE_LOG(Params.Log, ELogPriority::TLOG_INFO,
              TStringBuilder() << "Created Producer with id " << ProducerId << " for partition " << PartitionId);
}

TTopicWorkloadWriterProducer::~TTopicWorkloadWriterProducer() {
    if (WriteSession)
        WriteSession->Close(TDuration::Zero());
    WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG,
              TStringBuilder() << "Destructor for producer  " << ProducerId << " was called");
}

void TTopicWorkloadWriterProducer::Send(const TInstant &createTimestamp,
                                        std::optional<NYdb::NTable::TTransaction> transaction) {
    TString data = GetGeneratedMessage();
    InflightMessagesCreateTs[MessageId] = createTimestamp;

    NTopic::TWriteMessage writeMessage(data);
    writeMessage.SeqNo(MessageId);
    writeMessage.CreateTimestamp(createTimestamp);

    if (transaction.has_value()) {
        writeMessage.Tx(transaction.value());
    }

    WriteSession->Write(std::move(ContinuationToken.GetRef()), std::move(writeMessage));

    ContinuationToken.Clear();

    WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG,
              TStringBuilder() << "Sent message with id " << MessageId
                << " for producer " << ProducerId
                << " in writer " << Params.WriterIdx
                );

    MessageId++;
}

void TTopicWorkloadWriterProducer::Close() {
    if (WriteSession)
        WriteSession->Close(TDuration::Zero());
}


TString TTopicWorkloadWriterProducer::GetGeneratedMessage() const {
    return Params.GeneratedMessages[MessageId % TTopicWorkloadWriterWorker::GENERATED_MESSAGES_COUNT];
}

bool TTopicWorkloadWriterProducer::WaitForInitSeqNo() {
    NThreading::TFuture<ui64> InitSeqNo = WriteSession->GetInitSeqNo();
    while (!*Params.ErrorFlag) {
        if (!InitSeqNo.HasValue() && !InitSeqNo.Wait(TDuration::Seconds(1))) {
            WRITE_LOG(Params.Log, ELogPriority::TLOG_WARNING,
                      TStringBuilder() << "No initial sequence number for ProducerId " << ProducerId << " PartitionId "
                                       << PartitionId);
            Sleep(TDuration::Seconds(1));
            continue;
        }
        if (InitSeqNo.HasException()) {
            try {
                InitSeqNo.GetValue();
            } catch (yexception e) {
                WRITE_LOG(Params.Log, ELogPriority::TLOG_ERR, TStringBuilder()
                    << "Producer " << ProducerId
                    << " in writer " << Params.WriterIdx
                    << " for partition " << PartitionId
                    << ". Future exception: " << e.what());
            }
            *Params.ErrorFlag = 1;
            return false;
        }

        WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG,
                  TStringBuilder() << "Sequence number initialized " << InitSeqNo.GetValue());
        if (MessageId != InitSeqNo.GetValue() + 1) {
            MessageId = InitSeqNo.GetValue() + 1;
        }

        return true;
    }

    return false;
}

void TTopicWorkloadWriterProducer::WaitForContinuationToken(const TDuration &timeout) {
    // only TReadyToAcceptEvent will come here, cause we subscribed for other event types in constructor
    // we are waiting for this event, cause we can't proceed without ContinuationToken from previous write
    bool foundEvent = WriteSession->WaitEvent().Wait(timeout);

    WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG, TStringBuilder()
            << "Producer " << ProducerId
            << " in writer " << Params.WriterIdx
            << " for partition " << PartitionId
            << ": foundEvent - " << foundEvent);

    if (foundEvent) {
        auto variant = WriteSession->GetEvent(true).GetRef();
        if (std::holds_alternative<NYdb::NTopic::TWriteSessionEvent::TReadyToAcceptEvent>(variant)) {
            auto event = std::get<NYdb::NTopic::TWriteSessionEvent::TReadyToAcceptEvent>(variant);
            ContinuationToken = std::move(event.ContinuationToken);
            WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG, TStringBuilder()
                              << "Producer " << ProducerId
                              << " in writer " << Params.WriterIdx
                              << " for partition " << PartitionId
                              << ": Got new ContinuationToken token");
        }
    }

}

void TTopicWorkloadWriterProducer::HandleAckEvent(NYdb::NTopic::TWriteSessionEvent::TAcksEvent &event) {
    auto now = Now();
    //! Acks just confirm that message was received and saved by server
    //! successfully. Here we just count acked messages to check, that everything
    //! written is confirmed.
    for (const auto &ack: event.Acks) {
        ui64 AckedMessageId = ack.SeqNo;
        WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG, TStringBuilder() << "Got ack for write " << AckedMessageId);

        auto inflightMessageIter = InflightMessagesCreateTs.find(AckedMessageId);
        if (inflightMessageIter == InflightMessagesCreateTs.end()) {
            *Params.ErrorFlag = 1;
            WRITE_LOG(Params.Log, ELogPriority::TLOG_ERR,
                      TStringBuilder() << "Unknown AckedMessageId " << AckedMessageId);
        }

        auto inflightTime = (now - inflightMessageIter->second);
        InflightMessagesCreateTs.erase(inflightMessageIter);

        StatsCollector->AddWriterEvent(Params.WriterIdx, {Params.MessageSize, inflightTime.MilliSeconds(),
                                                          InflightMessagesCreateTs.size()});

        WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG,
                  TStringBuilder() << "Ack PartitionId " << ack.Details->PartitionId << " Offset "
                                   << ack.Details->Offset << " InflightTime " << inflightTime << " WriteTime "
                                   << ack.Stat->WriteTime << " MinTimeInPartitionQueue "
                                   << ack.Stat->MinTimeInPartitionQueue << " MaxTimeInPartitionQueue "
                                   << ack.Stat->MaxTimeInPartitionQueue << " PartitionQuotedTime "
                                   << ack.Stat->PartitionQuotedTime << " TopicQuotedTime "
                                   << ack.Stat->TopicQuotedTime);
    }
}

void TTopicWorkloadWriterProducer::HandleSessionClosed(const NYdb::NTopic::TSessionClosedEvent &event) {
    WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG, TStringBuilder()
        << "Producer " << ProducerId
        << ": got close event: " << event.DebugString());
    //! Session is closed, stop any work with it.
    *Params.ErrorFlag = 1;
}
