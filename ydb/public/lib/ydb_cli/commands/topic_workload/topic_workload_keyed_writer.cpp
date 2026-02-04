#include "topic_workload_keyed_writer.h"
#include "topic_workload_keyed_writer_producer.h"

#include <util/generic/overloaded.h>
#include <util/generic/guid.h>

namespace NYdb::NConsoleClient {

TTopicWorkloadKeyedWriterWorker::TTopicWorkloadKeyedWriterWorker(const TTopicWorkloadKeyedWriterParams& params)
    : Params(params)
    , Closed(std::make_shared<std::atomic<bool>>(false))
    , StatsCollector(Params.StatsCollector)
{
    // Keyed write session is able to route messages to partitions using message keys,
    // so we create a single producer session per writer worker.
    Producers = std::vector<std::shared_ptr<TTopicWorkloadKeyedWriterProducer>>();
    Producers.reserve(1);
    Producers.push_back(CreateProducer());

    WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG, TStringBuilder()
        << "WriterId " << Params.WriterIdx
        << ": Initialized " << Producers.size() << " keyed producers");
}

TTopicWorkloadKeyedWriterWorker::~TTopicWorkloadKeyedWriterWorker()
{
    CloseProducers();
}

void TTopicWorkloadKeyedWriterWorker::Close()
{
    Closed->store(true);
    CloseProducers();
}

void TTopicWorkloadKeyedWriterWorker::CloseProducers()
{
    for (auto& producer : Producers) {
        producer->Close();
    }
}

std::vector<TString> TTopicWorkloadKeyedWriterWorker::GenerateMessages(size_t messageSize)
{
    return TTopicWorkloadWriterWorker::GenerateMessages(messageSize);
}

/*!
 * This method returns timestamp, when current message is expected to be generated.
 *
 * E.g. if we have 100 messages per second rate, then first message is expected at 0ms of first second, second after 10ms elapsed,
 * third after 20ms elapsed, 10th after 100ms elapsed and so on.
 * */
TInstant TTopicWorkloadKeyedWriterWorker::GetExpectedCurrMessageCreationTimestamp() const
{
    return StartTimestamp + TDuration::Seconds((double)BytesWritten / Params.BytesPerSec * Params.ProducerThreadCount);
}

void TTopicWorkloadKeyedWriterWorker::WaitTillNextMessageExpectedCreateTimeAndContinuationToken(
    std::shared_ptr<TTopicWorkloadKeyedWriterProducer> producer)
{
    auto now = Now();
    TDuration timeToNextMessage = Params.BytesPerSec == 0 ? TDuration::Zero() :
        GetExpectedCurrMessageCreationTimestamp() - now;

    if (timeToNextMessage > TDuration::Zero() || !producer->ContinuationTokenDefined()) {
        producer->WaitForContinuationToken(timeToNextMessage);
    }
}

void TTopicWorkloadKeyedWriterWorker::Process(TInstant endTime)
{
    Sleep(TDuration::Seconds((float)Params.WarmupSec * Params.WriterIdx / Params.ProducerThreadCount));

    TInstant commitTime = TInstant::Now() + TDuration::MilliSeconds(Params.CommitIntervalMs);

    StartTimestamp = Now();
    WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG, TStringBuilder()
        << "WriterIdx: " << Params.WriterIdx
        << ": StartTimestamp " << StartTimestamp);

    while (!*Params.ErrorFlag) {
        auto now = Now();
        if (now > endTime) {
            break;
        }

        auto producer = Producers[0];

        WaitTillNextMessageExpectedCreateTimeAndContinuationToken(producer);

        bool writingAllowed = producer->ContinuationTokenDefined();

        if (Params.BytesPerSec != 0) {
            ui64 bytesMustBeWritten = (now - StartTimestamp).SecondsFloat() * Params.BytesPerSec / Params.ProducerThreadCount;
            writingAllowed &= BytesWritten < bytesMustBeWritten;
        } else {
            writingAllowed &= InflightMessagesSize() <= 1_MB / Params.MessageSize;
        }

        if (writingAllowed && !WaitForCommitTx) {
            TInstant createTimestamp = GetCreateTimestampForNextMessage();
            BytesWritten += Params.MessageSize;

            std::optional<NYdb::NTable::TTransaction> transaction;
            if (TxSupport && !TxSupport->Transaction) {
                TxSupport->BeginTx();
            }
            if (TxSupport) {
                transaction = *TxSupport->Transaction;
            }

            producer->Send(createTimestamp, transaction);

            if (TxSupport) {
                TxSupport->AppendRow("");
                TryCommitTx(commitTime);
            }

            PartitionToWriteId++;
        } else {
            if (TxSupport) {
                TryCommitTx(commitTime);
            }
            Sleep(TDuration::MilliSeconds(1));
        }
    }
}

std::shared_ptr<TTopicWorkloadKeyedWriterProducer> TTopicWorkloadKeyedWriterWorker::CreateProducer()
{
    auto clock = NUnifiedAgent::TClock();
    auto producerId = TGUID::CreateTimebased().AsGuidString();

    auto producer = std::make_shared<TTopicWorkloadKeyedWriterProducer>(
        Params,
        StatsCollector,
        producerId,
        std::move(clock)
    );

    NYdb::NTopic::TKeyedWriteSessionSettings settings;
    settings.Codec((NYdb::NTopic::ECodec)Params.Codec);
    settings.Path(Params.TopicName);
    settings.ProducerIdPrefix(producerId);
    settings.PartitionChooserStrategy(NYdb::NTopic::TKeyedWriteSessionSettings::EPartitionChooserStrategy::Hash);
    if (Params.MaxMemoryUsageBytes.has_value()) {
        settings.MaxMemoryUsage(Params.MaxMemoryUsageBytes.value());
    }

    NYdb::NTopic::TWriteSessionSettings::TEventHandlers eventHandlers;
    eventHandlers.AcksHandler(
        std::bind(&TTopicWorkloadKeyedWriterProducer::HandleAckEvent, producer, std::placeholders::_1));
    eventHandlers.SessionClosedHandler(
        std::bind(&TTopicWorkloadKeyedWriterProducer::HandleSessionClosed, producer, std::placeholders::_1));
    settings.EventHandlers(eventHandlers);

    settings.DirectWriteToPartition(Params.Direct);

    producer->SetWriteSession(NYdb::NTopic::TTopicClient(Params.Driver).CreateKeyedWriteSession(settings));
    return producer;
}

size_t TTopicWorkloadKeyedWriterWorker::InflightMessagesSize()
{
    size_t total = 0;
    for (auto& producer : Producers) {
        total += producer->InflightMessagesCnt();
    }
    return total;
}

bool TTopicWorkloadKeyedWriterWorker::InflightMessagesEmpty()
{
    for (auto& producer : Producers) {
        if (producer->InflightMessagesCnt() != 0) {
            return false;
        }
    }
    return true;
}

void TTopicWorkloadKeyedWriterWorker::RetryableWriterLoop(const TTopicWorkloadKeyedWriterParams& params)
{
    auto errorFlag = params.ErrorFlag;
    const TInstant endTime = Now() + TDuration::Seconds(params.TotalSec + 3);

    while (!*errorFlag && Now() < endTime) {
        try {
            WriterLoop(params, endTime);
        } catch (const yexception& ex) {
            WRITE_LOG(params.Log, ELogPriority::TLOG_WARNING, TStringBuilder() << ex);
        }
    }
}

void TTopicWorkloadKeyedWriterWorker::WriterLoop(const TTopicWorkloadKeyedWriterParams& params, TInstant endTime)
{
    TTopicWorkloadKeyedWriterWorker writer(params);

    if (params.UseTransactions) {
        writer.TxSupport.emplace(params.Driver, "", "");
    }

    (*writer.Params.StartedCount)++;

    WRITE_LOG(writer.Params.Log, ELogPriority::TLOG_INFO,
              TStringBuilder() << "Keyed writer started " << Now().ToStringUpToSeconds());

    try {
        writer.Process(endTime);
    } catch (const std::runtime_error& re) {
        WRITE_LOG(writer.Params.Log, ELogPriority::TLOG_ERR, TStringBuilder()
            << "Keyed writer " << writer.Params.WriterIdx << " failed with error: " << re.what());
    } catch (...) {
        WRITE_LOG(writer.Params.Log, ELogPriority::TLOG_ERR, TStringBuilder()
            << "Keyed writer " << writer.Params.WriterIdx << " caught unknown exception: " << CurrentExceptionMessage());
    }

    WRITE_LOG(writer.Params.Log, ELogPriority::TLOG_INFO,
              TStringBuilder() << "Keyed writer finished " << Now().ToStringUpToSeconds());
}

void TTopicWorkloadKeyedWriterWorker::TryCommitTx(TInstant& commitTime)
{
    Y_ABORT_UNLESS(TxSupport);
    auto now = Now();

    bool commitTimeIsInFuture = now < commitTime;
    bool notEnoughRowsInCommit = TxSupport->Rows.size() < Params.CommitMessages;
    if (commitTimeIsInFuture && notEnoughRowsInCommit) {
        return;
    }

    TryCommitTableChanges();

    commitTime += TDuration::MilliSeconds(Params.CommitIntervalMs);
    WaitForCommitTx = false;
}

void TTopicWorkloadKeyedWriterWorker::TryCommitTableChanges()
{
    if (TxSupport->Rows.empty()) {
        return;
    }

    auto execTimes = TxSupport->CommitTx(Params.UseTableSelect, Params.UseTableUpsert);
    Params.StatsCollector->AddWriterSelectEvent(Params.WriterIdx, {execTimes.SelectTime.MilliSeconds()});
    Params.StatsCollector->AddWriterUpsertEvent(Params.WriterIdx, {execTimes.UpsertTime.MilliSeconds()});
    Params.StatsCollector->AddWriterCommitTxEvent(Params.WriterIdx, {execTimes.CommitTime.MilliSeconds()});
}

TInstant TTopicWorkloadKeyedWriterWorker::GetCreateTimestampForNextMessage()
{
    if (Params.UseCpuTimestamp || Params.BytesPerSec == 0) {
        return TInstant::Now();
    }
    return GetExpectedCurrMessageCreationTimestamp();
}

} // namespace NYdb::NConsoleClient