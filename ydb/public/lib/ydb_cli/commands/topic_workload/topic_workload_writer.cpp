#include "topic_workload_writer.h"
#include "topic_workload_describe.h"

#include <util/generic/overloaded.h>

using namespace NYdb::NConsoleClient;

TTopicWorkloadWriterWorker::TTopicWorkloadWriterWorker(
    TTopicWorkloadWriterParams&& params)
    : Params(params)
    , MessageId(1)
    , StatsCollector(params.StatsCollector)

{
    CreateWorker();
}

TTopicWorkloadWriterWorker::~TTopicWorkloadWriterWorker()
{
    if (WriteSession)
        WriteSession->Close();
}

void TTopicWorkloadWriterWorker::Close() {
    Closed->store(true);
    if (WriteSession)
        WriteSession->Close(TDuration::Zero());
}

const size_t GENERATED_MESSAGES_COUNT = 32;

std::vector<TString> TTopicWorkloadWriterWorker::GenerateMessages(size_t messageSize) {
    std::vector<TString> generatedMessages;
    for (size_t i = 0; i < GENERATED_MESSAGES_COUNT; i++) {
        TStringBuilder stringBuilder;
        while (stringBuilder.Size() < messageSize)
            stringBuilder << RandomNumber<ui64>(UINT64_MAX);
        stringBuilder.resize(messageSize);
        generatedMessages.push_back(std::move(stringBuilder));
    }
    return generatedMessages;
}

TString TTopicWorkloadWriterWorker::GetGeneratedMessage() const {
    return Params.GeneratedMessages[MessageId % GENERATED_MESSAGES_COUNT];
}

TInstant TTopicWorkloadWriterWorker::GetCreateTimestamp() const {
    return StartTimestamp + TDuration::Seconds((double)BytesWritten / Params.ByteRate * Params.ProducerThreadCount);
}

bool TTopicWorkloadWriterWorker::WaitForInitSeqNo()
{
    NThreading::TFuture<ui64> InitSeqNo = WriteSession->GetInitSeqNo();
    while (!*Params.ErrorFlag) {
        if (!InitSeqNo.HasValue() && !InitSeqNo.Wait(TDuration::Seconds(1))) {
            WRITE_LOG(Params.Log, ELogPriority::TLOG_WARNING, TStringBuilder() << "No initial sequence number for ProducerId " << Params.ProducerId << " PartitionId " << Params.PartitionId);
            Sleep(TDuration::Seconds(1));
            continue;
        }
        if (InitSeqNo.HasException()) {
            try {
                InitSeqNo.GetValue();
            } catch (yexception e) {
                WRITE_LOG(Params.Log, ELogPriority::TLOG_ERR, TStringBuilder() << "Future exception: " << e.what());
            }
            *Params.ErrorFlag = 1;
            return false;
        }

        WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG, TStringBuilder() << "Sequence number initialized " << InitSeqNo.GetValue());
        if (MessageId != InitSeqNo.GetValue() + 1) {
            MessageId = InitSeqNo.GetValue() + 1;
            AckedMessageId = MessageId - 1;
        }

        return true;
    }

    return false;
}

void TTopicWorkloadWriterWorker::Process(TInstant endTime) {
    Sleep(TDuration::Seconds((float)Params.WarmupSec * Params.WriterIdx / Params.ProducerThreadCount));
    
    TInstant commitTime = TInstant::Now() + TDuration::Seconds(Params.CommitPeriod);

    StartTimestamp = Now();
    WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG, TStringBuilder() << "StartTimestamp " << StartTimestamp);

    while (!*Params.ErrorFlag)
    {
        auto now = Now();
        if (now > endTime)
            break;

        TDuration timeToNextMessage = Params.ByteRate == 0 ? TDuration::Zero() : GetCreateTimestamp() - now;

        if (timeToNextMessage > TDuration::Zero())
        {
            WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG, TStringBuilder() << "WaitEvent for timeToNextMessage " << timeToNextMessage);
            WriteSession->WaitEvent().Wait(timeToNextMessage);
        } else
            WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG, TStringBuilder() << "No WaitEvent");

        while (!*Params.ErrorFlag) {
            auto events = WriteSession->GetEvents(false);
            WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG, TStringBuilder() << "Got " << events.size() << " events.");

            for (auto& e : events)
                ProcessEvent(e);

            now = Now();

            bool writingAllowed = ContinuationToken.Defined();
            WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG, TStringBuilder() << "ContinuationToken.Defined() " << ContinuationToken.Defined());

            if (Params.ByteRate != 0)
            {
                ui64 bytesMustBeWritten = (now - StartTimestamp).SecondsFloat() * Params.ByteRate / Params.ProducerThreadCount;
                writingAllowed &= BytesWritten < bytesMustBeWritten;
                WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG, TStringBuilder() << "BytesWritten " << BytesWritten << " bytesMustBeWritten " << bytesMustBeWritten << " writingAllowed " << writingAllowed);
            }
            else
            {
                writingAllowed &= InflightMessages.size() <= 1_MB / Params.MessageSize;
                WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG, TStringBuilder() << "Inflight size " << InflightMessages.size() << " writingAllowed " << writingAllowed);
            }

            if (writingAllowed && !WaitForCommitTx)
            {
                TString data = GetGeneratedMessage();

                TMaybe<TInstant> createTimestamp = Params.ByteRate == 0 ? TMaybe<TInstant>(Nothing()) : GetCreateTimestamp();

                InflightMessages[MessageId] = createTimestamp.GetOrElse(now);

                BytesWritten += Params.MessageSize;

                if (TxSupport && !TxSupport->Transaction) {
                    TxSupport->BeginTx();
                }

                NTopic::TWriteMessage writeMessage(data);
                writeMessage.SeqNo(MessageId);
                writeMessage.CreateTimestamp(createTimestamp);
                if (TxSupport) {
                    writeMessage.Tx(*TxSupport->Transaction);
                }

                WriteSession->Write(std::move(ContinuationToken.GetRef()), std::move(writeMessage));

                if (TxSupport) {
                    TxSupport->AppendRow("");
                }

                WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG, TStringBuilder() << "Written message " << MessageId << " CreateTimestamp " << createTimestamp << " delta from now " << (Params.ByteRate == 0 ? TDuration() : now - *createTimestamp.Get()));
                ContinuationToken.Clear();
                MessageId++;

                if (TxSupport) {
                    TryCommitTx(Params, commitTime);
                }
            }
            else
            {
                if (TxSupport) {
                    TryCommitTx(Params, commitTime);
                }

                Sleep(TDuration::MilliSeconds(1));
            }

            if (events.empty())
                break;
        }
    }
}

bool TTopicWorkloadWriterWorker::ProcessEvent(
    NYdb::NTopic::TWriteSessionEvent::TEvent& event) {
    return std::visit(
        TOverloaded{
            [this](const NYdb::NTopic::TWriteSessionEvent::TAcksEvent& event) {
                return ProcessAckEvent(event);
            },
            [this](NYdb::NTopic::TWriteSessionEvent::TReadyToAcceptEvent& event) {
                return ProcessReadyToAcceptEvent(event);
            },
            [this](const NYdb::NTopic::TSessionClosedEvent& event) {
                return ProcessSessionClosedEvent(event);
            }}, event);
};

bool TTopicWorkloadWriterWorker::ProcessAckEvent(
    const NYdb::NTopic::TWriteSessionEvent::TAcksEvent& event) {
    bool hasProgress = false;
    auto now = Now();
    //! Acks just confirm that message was received and saved by server
    //! successfully. Here we just count acked messages to check, that everything
    //! written is confirmed.
    for (const auto& ack : event.Acks) {
        WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG, TStringBuilder() << "Got ack for write " << AckedMessageId);
        AckedMessageId = ack.SeqNo;

        auto inflightMessageIter = InflightMessages.find(AckedMessageId);
        if (inflightMessageIter == InflightMessages.end())
        {
            *Params.ErrorFlag = 1;
            WRITE_LOG(Params.Log, ELogPriority::TLOG_ERR, TStringBuilder() << "Unknown AckedMessageId " << AckedMessageId);
            return false;
        }

        auto inflightTime = (now - inflightMessageIter->second);
        InflightMessages.erase(inflightMessageIter);

        StatsCollector->AddWriterEvent(Params.WriterIdx, {Params.MessageSize, inflightTime.MilliSeconds(), InflightMessages.size()});

        hasProgress = true;

        WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG, TStringBuilder() << "Ack PartitionId " << ack.Details->PartitionId << " Offset " << ack.Details->Offset << " InflightTime " << inflightTime << " WriteTime " << ack.Stat->WriteTime << " MinTimeInPartitionQueue " << ack.Stat->MinTimeInPartitionQueue << " MaxTimeInPartitionQueue " << ack.Stat->MaxTimeInPartitionQueue << " PartitionQuotedTime " << ack.Stat->PartitionQuotedTime << " TopicQuotedTime " << ack.Stat->TopicQuotedTime);
    }
    return hasProgress;
}

bool TTopicWorkloadWriterWorker::ProcessReadyToAcceptEvent(
    NYdb::NTopic::TWriteSessionEvent::TReadyToAcceptEvent& event) {
    //! TReadyToAcceptEvent provide continue tokens - an object to perform further
    //! writes.
    //!  Do NOT lose continue tokens!

    ContinuationToken = std::move(event.ContinuationToken);

    WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG, "Got new continue token.");

    return true;
}

bool TTopicWorkloadWriterWorker::ProcessSessionClosedEvent(
    const NYdb::NTopic::TSessionClosedEvent& event) {
    WRITE_LOG(Params.Log, ELogPriority::TLOG_EMERG, TStringBuilder() << "Got close event: " << event.DebugString());
    //! Session is closed, stop any work with it.
    *Params.ErrorFlag = 1;
    return false;
}

void TTopicWorkloadWriterWorker::CreateWorker() {
    WRITE_LOG(Params.Log, ELogPriority::TLOG_INFO, TStringBuilder() << "Create writer worker for ProducerId " << Params.ProducerId << " PartitionId " << Params.PartitionId);

    NYdb::NTopic::TWriteSessionSettings settings;
    settings.Codec((NYdb::NTopic::ECodec)Params.Codec);
    settings.Path(Params.TopicName);
    settings.ProducerId(Params.ProducerId);
    if (Params.UseAutoPartitioning) {
        settings.MessageGroupId(Params.ProducerId);
    } else {
        settings.PartitionId(Params.PartitionId);
    }

    settings.DirectWriteToPartition(Params.Direct);
    WriteSession = NYdb::NTopic::TTopicClient(Params.Driver).CreateWriteSession(settings);
}

void TTopicWorkloadWriterWorker::RetryableWriterLoop(TTopicWorkloadWriterParams& params) {
    const TInstant endTime = Now() + TDuration::Seconds(params.TotalSec + 3);

    while (!*params.ErrorFlag && Now() < endTime) {
        try {
            WriterLoop(params, endTime);
        } catch (const yexception& ex) {
            WRITE_LOG(params.Log, ELogPriority::TLOG_WARNING, TStringBuilder() << ex);
        }
    }
}

void TTopicWorkloadWriterWorker::WriterLoop(TTopicWorkloadWriterParams& params, TInstant endTime) {
    TTopicWorkloadWriterWorker writer(std::move(params));

    if (params.UseTransactions) {
        writer.TxSupport.emplace(params.Driver, "", "");
    }

    (*params.StartedCount)++;

    WRITE_LOG(params.Log, ELogPriority::TLOG_INFO, TStringBuilder() << "Writer started " << Now().ToStringUpToSeconds());

    if (!writer.WaitForInitSeqNo())
        return;

    writer.Process(endTime);

    WRITE_LOG(params.Log, ELogPriority::TLOG_INFO, TStringBuilder() << "Writer finished " << Now().ToStringUpToSeconds());
}

void TTopicWorkloadWriterWorker::TryCommitTx(TTopicWorkloadWriterParams& params,
                                             TInstant& commitTime)
{
    Y_ABORT_UNLESS(TxSupport);

    if ((commitTime > Now()) && (params.CommitMessages > TxSupport->Rows.size())) {
        return;
    }

    if (!InflightMessages.empty()) {
        WaitForCommitTx = true;
        return;
    }

    TryCommitTableChanges(params);

    commitTime += TDuration::Seconds(params.CommitPeriod);

    WaitForCommitTx = false;
}

void TTopicWorkloadWriterWorker::TryCommitTableChanges(TTopicWorkloadWriterParams& params)
{
    if (TxSupport->Rows.empty()) {
        return;
    }

    auto execTimes = TxSupport->CommitTx(params.UseTableSelect, params.UseTableUpsert);

    params.StatsCollector->AddWriterSelectEvent(params.WriterIdx, {execTimes.SelectTime.MilliSeconds()});
    params.StatsCollector->AddWriterUpsertEvent(params.WriterIdx, {execTimes.UpsertTime.MilliSeconds()});
    params.StatsCollector->AddWriterCommitTxEvent(params.WriterIdx, {execTimes.CommitTime.MilliSeconds()});
}
