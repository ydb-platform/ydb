#include "topic_workload_keyed_writer.h"
#include "topic_workload_keyed_writer_producer.h"
#include "topic_workload_writer_worker_common.h"

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
    SessionId = TGUID::CreateTimebased().AsGuidString();
    Producers.push_back(CreateProducer());

    WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG, LogPrefix()
        << "WriterId " << Params.WriterIdx
        << ": Initialized " << Producers.size() << " keyed producers");
}

TTopicWorkloadKeyedWriterWorker::~TTopicWorkloadKeyedWriterWorker()
{
    CloseProducers();
}

TStringBuilder TTopicWorkloadKeyedWriterWorker::LogPrefix() {
    return TStringBuilder() << " SessionId: " << SessionId << " ";
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
    return NYdb::NConsoleClient::NTopicWorkloadWriterInternal::GetExpectedCurrMessageCreationTimestamp(
        StartTimestamp,
        BytesWritten,
        Params
    );
}

void TTopicWorkloadKeyedWriterWorker::Process(TInstant endTime)
{
    NYdb::NConsoleClient::NTopicWorkloadWriterInternal::ProcessWriterLoopCommon(
        Params,
        Producers,
        BytesWritten,
        PartitionToWriteId,
        StartTimestamp,
        TxSupport,
        WaitForCommitTx,
        endTime,
        [](const std::shared_ptr<TTopicWorkloadKeyedWriterProducer>& producer) {
            return producer->HasContinuationTokens();
        },
        [this]() {
            return GetCreateTimestampForNextMessage();
        },
        [this](TInstant& commitTime) {
            TryCommitTx(commitTime);
        },
        [this]() {
            return InflightMessagesSize();
        },
        [this](const TInstant& startTimestamp) {
            WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG, LogPrefix()
                << "WriterIdx: " << Params.WriterIdx
                << ": StartTimestamp " << startTimestamp);
        },
        [](const std::shared_ptr<TTopicWorkloadKeyedWriterProducer>&,
           const TInstant&,
           const TInstant&) {
            // No extra per-message logging for keyed writer.
        },
        SessionId,
        TDuration::Seconds(20)
    );
}

std::shared_ptr<TTopicWorkloadKeyedWriterProducer> TTopicWorkloadKeyedWriterWorker::CreateProducer()
{
    auto clock = NUnifiedAgent::TClock();
    auto producerId = TGUID::CreateTimebased().AsGuidString();

    auto producer = std::make_shared<TTopicWorkloadKeyedWriterProducer>(
        Params,
        StatsCollector,
        producerId,
        SessionId,
        std::move(clock)
    );

    auto topicClient = NYdb::NTopic::TTopicClient(Params.Driver);
    auto describeResult = topicClient.DescribeTopic(Params.TopicName).GetValueSync();
    auto autoPartitioningStrategy = describeResult.GetTopicDescription().GetPartitioningSettings().GetAutoPartitioningSettings().GetStrategy();
    auto isAutoPartitioningEnabled = autoPartitioningStrategy != NTopic::EAutoPartitioningStrategy::Unspecified && autoPartitioningStrategy != NTopic::EAutoPartitioningStrategy::Disabled;

    NYdb::NTopic::TKeyedWriteSessionSettings settings;
    settings.Codec((NYdb::NTopic::ECodec)Params.Codec);
    settings.Path(Params.TopicName);
    settings.SessionId(SessionId);
    settings.ProducerIdPrefix(producerId);
    settings.PartitionChooserStrategy(
        isAutoPartitioningEnabled ?
        NYdb::NTopic::TKeyedWriteSessionSettings::EPartitionChooserStrategy::Bound :
        NYdb::NTopic::TKeyedWriteSessionSettings::EPartitionChooserStrategy::Hash);
    if (Params.MaxMemoryUsageBytes.has_value()) {
        settings.MaxMemoryUsage(Params.MaxMemoryUsageBytes.value());
    }

    NYdb::NTopic::TWriteSessionSettings::TEventHandlers eventHandlers;
    eventHandlers.AcksHandler(
        std::bind(&TTopicWorkloadKeyedWriterProducer::HandleAckEvent, producer, std::placeholders::_1));
    eventHandlers.SessionClosedHandler(
        std::bind(&TTopicWorkloadKeyedWriterProducer::HandleSessionClosed, producer, std::placeholders::_1));
    eventHandlers.ReadyToAcceptHandler(
        std::bind(&TTopicWorkloadKeyedWriterProducer::HandleReadyToAcceptEvent, producer, std::placeholders::_1));
    settings.EventHandlers(eventHandlers);

    settings.DirectWriteToPartition(Params.Direct);

    producer->SetWriteSession(topicClient.CreateKeyedWriteSession(settings));
    return producer;
}

size_t TTopicWorkloadKeyedWriterWorker::InflightMessagesSize()
{
    return NYdb::NConsoleClient::NTopicWorkloadWriterInternal::InflightMessagesSize(Producers);
}

bool TTopicWorkloadKeyedWriterWorker::InflightMessagesEmpty()
{
    return NYdb::NConsoleClient::NTopicWorkloadWriterInternal::InflightMessagesEmpty(Producers);
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
              writer.LogPrefix() << "Keyed writer started " << Now().ToStringUpToSeconds());

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
    NYdb::NConsoleClient::NTopicWorkloadWriterInternal::TryCommitTxCommon(
        Params,
        TxSupport,
        commitTime,
        WaitForCommitTx,
        [](const TInstant&, const TInstant&, size_t) {
            // Keyed writer: keep the previous behavior (no extra debug logging).
        }
    );
}

void TTopicWorkloadKeyedWriterWorker::TryCommitTableChanges()
{
    if (TxSupport->Rows.empty()) {
        return;
    }
    NYdb::NConsoleClient::NTopicWorkloadWriterInternal::CommitTableChangesCommon(Params, TxSupport);
}

TInstant TTopicWorkloadKeyedWriterWorker::GetCreateTimestampForNextMessage()
{
    const TInstant expectedTs = Params.BytesPerSec == 0 ? TInstant() : GetExpectedCurrMessageCreationTimestamp();
    return NYdb::NConsoleClient::NTopicWorkloadWriterInternal::GetCreateTimestampForNextMessage(Params, expectedTs);
}

} // namespace NYdb::NConsoleClient