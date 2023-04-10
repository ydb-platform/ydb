#include "topic_workload_writer.h"

#include <util/generic/overloaded.h>

using namespace NYdb::NConsoleClient;

TTopicWorkloadWriterWorker::TTopicWorkloadWriterWorker(
    TTopicWorkloadWriterParams&& params)
    : Params(params)
    , MessageId(1)
    , StartTimestamp(TInstant::Now())
    , StatsCollector(params.StatsCollector)

{
    Closed = std::make_shared<std::atomic<bool>>(false);
    GenerateMessages();
    CreateWorker();
}

TTopicWorkloadWriterWorker::~TTopicWorkloadWriterWorker()
{
    if (WriteSession)
        WriteSession->Close();
}

void TTopicWorkloadWriterWorker::CreateWorker() {
    if (!InitSeqNoProcessed) {
        *Params.ErrorFlag = 1;
        WRITE_LOG(Params.Log, ELogPriority::TLOG_WARNING, "WRITER no progress while writing by protocol.\n");
        return;
    }

    MessageGroupId = Params.MessageGroupId;

    WRITE_LOG(Params.Log, ELogPriority::TLOG_INFO, TStringBuilder() << "WRITER create worker for " << MessageGroupId << ".\n");

    if (WriteSession)
        WriteSession->Close();

    CreateTopicWorker();
}

void TTopicWorkloadWriterWorker::Close() {
    Closed->store(true);
    if (WriteSession)
        WriteSession->Close(TDuration::Zero());
}

const size_t GENERATED_MESSAGES_COUNT = 32;

void TTopicWorkloadWriterWorker::GenerateMessages() {
    TStringBuilder res;
    for (size_t i = 0; i < GENERATED_MESSAGES_COUNT; i++) {
        res.clear();
        while (res.Size() < Params.MessageSize)
            res << RandomNumber<ui64>(UINT64_MAX);
        GeneratedMessages.push_back(res);
    }
}

TString TTopicWorkloadWriterWorker::GetGeneratedMessage() const {
    return GeneratedMessages[MessageId % GENERATED_MESSAGES_COUNT];
}

TDuration TTopicWorkloadWriterWorker::Process() {
    if (!InitSeqNoProcessed) {
        if (!InitSeqNo.HasValue() && !InitSeqNo.Wait(TDuration::Seconds(1))) {
            WRITE_LOG(Params.Log, ELogPriority::TLOG_WARNING, "WRITER no initial sequence number\n");
            return TDuration::Seconds(1);
        }
        if (InitSeqNo.HasException()) {
            try {
                InitSeqNo.GetValue();
            } catch (yexception e) {
                WRITE_LOG(Params.Log, ELogPriority::TLOG_ERR, TStringBuilder() << "Future exception: " << e.what() << "\n");
            }
            *Params.ErrorFlag = 1;
            return TDuration::Zero();
        }

        InitSeqNoProcessed = true;
        WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG, TStringBuilder() << "WRITER sequence number initialized " << InitSeqNo.GetValue() << "\n");
        if (MessageId != InitSeqNo.GetValue() + 1) {
            MessageId = InitSeqNo.GetValue() + 1;
            AckedMessageId = MessageId - 1;
        }
    }

    ui64 elapsedSeconds = (Now() - StartTimestamp).Seconds();
    const ui64 bytesMustBeWritten = Params.ByteRate == 0 ? UINT64_MAX : elapsedSeconds * Params.ByteRate / Params.ProducerThreadCount;

    while (true) {
        auto events = WriteSession->GetEvents(false);
        WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG, TStringBuilder() << "WRITER Got " << events.size() << " events \n");

        for (auto& e : events)
            ProcessEvent(e);

        if (BytesWritten < bytesMustBeWritten && ContinuationToken.Defined()) {
            WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG, TStringBuilder() << "WRITER Writing message " << MessageId << "\n");

            TString data = GetGeneratedMessage();
            size_t messageSize = data.size();

            TMaybe<TInstant> createTimestamp =
                Params.ByteRate == 0 ? TMaybe<TInstant>(Nothing())
                                     : StartTimestamp + TDuration::Seconds((double)BytesWritten / Params.ByteRate * Params.ProducerThreadCount);

            InflightMessages[MessageId] = {messageSize, createTimestamp.GetOrElse(Now())};

            BytesWritten += messageSize;

            WriteSession->Write(std::move(ContinuationToken.GetRef()), data, MessageId++, createTimestamp);
            ContinuationToken.Clear();
        }

        if (events.empty())
            break;
    }

    const TDuration timeToNextMessage = TDuration::MilliSeconds(50);
    return timeToNextMessage;
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
    //! Acks just confirm that message was received and saved by server
    //! successfully. Here we just count acked messages to check, that everything
    //! written is confirmed.
    for (const auto& ack : event.Acks) {
        WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG, TStringBuilder() << "WRITER Got ack for write " << AckedMessageId << "\n");
        AckedMessageId = ack.SeqNo;

        auto inflightMessageIter = InflightMessages.find(AckedMessageId);
        if (inflightMessageIter == InflightMessages.end())
        {
            *Params.ErrorFlag = 1;
            WRITE_LOG(Params.Log, ELogPriority::TLOG_ERR, TStringBuilder() << "WRITER Unknown AckedMessageId " << AckedMessageId << "\n");
            return false;
        }

        ui64 writeTime = (Now() - inflightMessageIter->second.MessageTime).MilliSeconds();
        ui64 messageSize = inflightMessageIter->second.MessageSize;
        InflightMessages.erase(inflightMessageIter);

        StatsCollector->AddWriterEvent(messageSize, writeTime, InflightMessages.size());

        hasProgress = true;
    }
    return hasProgress;
}

bool TTopicWorkloadWriterWorker::ProcessReadyToAcceptEvent(
    NYdb::NTopic::TWriteSessionEvent::TReadyToAcceptEvent& event) {
    //! TReadyToAcceptEvent provide continue tokens - an object to perform further
    //! writes.
    //!  Do NOT lose continue tokens!

    ContinuationToken = std::move(event.ContinuationToken);

    WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG, "WRITER Got new continue token\n");

    return true;
}

bool TTopicWorkloadWriterWorker::ProcessSessionClosedEvent(
    const NYdb::NTopic::TSessionClosedEvent& event) {
    WRITE_LOG(Params.Log, ELogPriority::TLOG_INFO, TStringBuilder() << "WRITER Got close event: " << event.DebugString() << "\n");
    //! Session is closed, stop any work with it.
    Y_FAIL("session closed");
    return false;
}

void TTopicWorkloadWriterWorker::CreateTopicWorker() {
    WRITE_LOG(Params.Log, ELogPriority::TLOG_INFO, "WRITER Creating worker\n");
    Y_VERIFY(Params.Driver);
    NYdb::NTopic::TWriteSessionSettings settings;
    settings.Codec((NYdb::NTopic::ECodec)Params.Codec);
    settings.Path(TOPIC);
    settings.ProducerId(MessageGroupId).MessageGroupId(MessageGroupId);
    WriteSession = NYdb::NTopic::TTopicClient(*Params.Driver).CreateWriteSession(settings);
    InitSeqNo = WriteSession->GetInitSeqNo();
    InitSeqNoProcessed = false;
}

void TTopicWorkloadWriterWorker::WriterLoop(TTopicWorkloadWriterParams&& params) {
    TTopicWorkloadWriterWorker writer(std::move(params));

    (*params.StartedCount)++;

    WRITE_LOG(params.Log, ELogPriority::TLOG_INFO, TStringBuilder() << "WRITER started " << Now().ToStringUpToSeconds() << "\n");

    auto endTime = TInstant::Now() + TDuration::Seconds(params.Seconds);
    while (Now() < endTime && !*params.ErrorFlag) {
        TDuration delta = TDuration::MilliSeconds(100);
        auto d = writer.Process();
        delta = Min(d, delta);
        Sleep(delta);
    }


    WRITE_LOG(params.Log, ELogPriority::TLOG_INFO, TStringBuilder() << "WRITER finished " << Now().ToStringUpToSeconds() << "\n");
}
