#include "topic_workload_writer.h"
#include "topic_workload_writer_producer.h"
#include "topic_workload_writer_worker_common.h"
#include "topic_workload_configurator.h"
#include "topic_workload_describe.h"

#include <util/generic/overloaded.h>
#include <util/generic/guid.h>

using namespace NYdb::NConsoleClient;

TTopicWorkloadWriterWorker::TTopicWorkloadWriterWorker(const TTopicWorkloadWriterParams& params)
    : Params(params)
    , Closed(std::make_shared<std::atomic<bool>>(false))
    , StatsCollector(Params.StatsCollector)
{
    Producers = std::vector<std::shared_ptr<TTopicWorkloadWriterProducer>>();
    Producers.reserve(Params.PartitionCount);

    for (ui32 i = 0; i < Params.PartitionCount; ++i) {
        // write to random partition, cause workload CLI tool can be launched in several instances
        // and they need to load test different partitions of the topic
        ui32 partitionId = (Params.PartitionSeed + i) % Params.PartitionCount;

        Producers.push_back(CreateProducer(partitionId));
    }

    WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG, TStringBuilder()
            << "WriterId " << Params.WriterIdx
            << ": Initialized " << Params.PartitionCount << " producers");
}

TTopicWorkloadWriterWorker::~TTopicWorkloadWriterWorker()
{
    CloseProducers();
}

void TTopicWorkloadWriterWorker::Close()
{
    Closed->store(true);
    CloseProducers();
}

void TTopicWorkloadWriterWorker::CloseProducers()
{
    for (auto producer : Producers) {
        producer->Close();
    }
}

std::vector<TString> TTopicWorkloadWriterWorker::GenerateMessages(size_t messageSize) {
    std::vector<TString> generatedMessages;
    for (size_t i = 0; i < GENERATED_MESSAGES_COUNT; i++) {
        TStringBuilder stringBuilder;
        while (stringBuilder.size() < messageSize)
            stringBuilder << RandomNumber<ui64>(UINT64_MAX);
        stringBuilder.resize(messageSize);
        generatedMessages.push_back(std::move(stringBuilder));
    }
    return generatedMessages;
}

/*!
 * This method returns timestamp, when current message is expected to be generated.
 *
 * E.g. if we have 100 messages per second rate, then first message is expected at 0ms of first second, second after 10ms elapsed,
 * third after 20ms elapsed, 10th after 100ms elapsed and so on. This way 101 message is expected to be generated not earlier
 * then 1 second and 10 ms after test start has passed.
 * */
TInstant TTopicWorkloadWriterWorker::GetExpectedCurrMessageCreationTimestamp() const {
    return NYdb::NConsoleClient::NTopicWorkloadWriterInternal::GetExpectedCurrMessageCreationTimestamp(
        StartTimestamp,
        BytesWritten,
        Params
    );
}

void TTopicWorkloadWriterWorker::WaitTillNextMessageExpectedCreateTimeAndContinuationToken(
                            std::shared_ptr<TTopicWorkloadWriterProducer> producer) {
    auto now = Now();
    TDuration timeToNextMessage = Params.BytesPerSec == 0 ? TDuration::Zero() :
                                  GetExpectedCurrMessageCreationTimestamp() - now;

    if (timeToNextMessage > TDuration::Zero() || !producer->ContinuationTokenDefined())
    {
        producer->WaitForContinuationToken(timeToNextMessage);
    }
}

void TTopicWorkloadWriterWorker::Process(TInstant endTime) {
    NYdb::NConsoleClient::NTopicWorkloadWriterInternal::ProcessWriterLoopCommon(
        Params,
        Producers,
        BytesWritten,
        PartitionToWriteId,
        StartTimestamp,
        TxSupport,
        WaitForCommitTx,
        endTime,
        [](const std::shared_ptr<TTopicWorkloadWriterProducer>& producer) {
            return producer->ContinuationTokenDefined();
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
            WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG, TStringBuilder() << "WriterIdx: "
                                                                            << Params.WriterIdx
                                                                            << ": StartTimestamp " << startTimestamp);
        },
        [this](const std::shared_ptr<TTopicWorkloadWriterProducer>& producer,
               const TInstant& createTimestamp,
               const TInstant& now) {
            WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG, TStringBuilder()
                << "Written message " << producer->GetCurrentMessageId() - 1
                << " in writer " << Params.WriterIdx
                << " For partition " << producer->GetPartitionId()
                << " message create ts " << createTimestamp
                << " delta from now " << (Params.BytesPerSec == 0 ? TDuration() : now - createTimestamp));
        },
        ""
    );
}

std::shared_ptr<TTopicWorkloadWriterProducer> TTopicWorkloadWriterWorker::CreateProducer(ui64 partitionId) {
    auto clock = NUnifiedAgent::TClock();
    auto producerId = TGUID::CreateTimebased().AsGuidString();

    auto producer = std::make_shared<TTopicWorkloadWriterProducer>(
            Params,
            StatsCollector,
            producerId,
            partitionId,
            std::move(clock)
    );

    NYdb::NTopic::TWriteSessionSettings settings;
    settings.Codec((NYdb::NTopic::ECodec) Params.Codec);
    settings.Path(Params.TopicName);
    settings.ProducerId(producerId);
    if (Params.MaxMemoryUsageBytes.has_value()) {
        settings.MaxMemoryUsage(Params.MaxMemoryUsageBytes.value());
    }

    NYdb::NTopic::TWriteSessionSettings::TEventHandlers eventHandlers;
    eventHandlers.AcksHandler(
            std::bind(&TTopicWorkloadWriterProducer::HandleAckEvent, producer, std::placeholders::_1));
    eventHandlers.SessionClosedHandler(
            std::bind(&TTopicWorkloadWriterProducer::HandleSessionClosed, producer, std::placeholders::_1));
    settings.EventHandlers(eventHandlers);

    if (Params.UseAutoPartitioning) {
        settings.MessageGroupId(producerId);
    } else {
        settings.PartitionId(partitionId);
    }

    settings.DirectWriteToPartition(Params.Direct);

    producer->SetWriteSession(NYdb::NTopic::TTopicClient(Params.Driver).CreateWriteSession(settings));

    return producer;
}

size_t TTopicWorkloadWriterWorker::InflightMessagesSize() {
    return NYdb::NConsoleClient::NTopicWorkloadWriterInternal::InflightMessagesSize(Producers);
}

void TTopicWorkloadWriterWorker::RetryableWriterLoop(const TTopicWorkloadWriterParams& params) {
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

void TTopicWorkloadWriterWorker::WriterLoop(const TTopicWorkloadWriterParams& params, TInstant endTime) {
    TTopicWorkloadWriterWorker writer(params);

    if (params.UseTransactions) {
        writer.TxSupport.emplace(params.Driver, "", "");
    }

    (*writer.Params.StartedCount)++;

    WRITE_LOG(writer.Params.Log, ELogPriority::TLOG_INFO, TStringBuilder() << "Writer started " << Now().ToStringUpToSeconds());

    for (auto& producer : writer.Producers) {
        if (!producer->WaitForInitSeqNo())
            return;
    }

    try {
        writer.Process(endTime);
    } catch (const std::runtime_error& re) {
        WRITE_LOG(writer.Params.Log, ELogPriority::TLOG_ERR, TStringBuilder()
            << "Writer " << writer.Params.WriterIdx << " failed with error: " << re.what());
    } catch (...) {
        WRITE_LOG(writer.Params.Log, ELogPriority::TLOG_ERR, TStringBuilder()
            << "Writer " << writer.Params.WriterIdx << " caught unknown exception: " << CurrentExceptionMessage());
    }

    WRITE_LOG(writer.Params.Log, ELogPriority::TLOG_INFO, TStringBuilder() << "Writer finished " << Now().ToStringUpToSeconds());
}

void TTopicWorkloadWriterWorker::RetryableConfiguratorLoop(const TTopicWorkloadConfiguratorParams& params)
{
    auto errorFlag = params.ErrorFlag;

    const TInstant endTime = Now() + TDuration::Seconds(params.TotalSec + 3);

    while (!*errorFlag && Now() < endTime) {
        try {
            ConfiguratorLoop(params, endTime);
        } catch (const yexception& ex) {
            WRITE_LOG(params.Log, ELogPriority::TLOG_WARNING, TStringBuilder() << ex);
        }
    }
}

void TTopicWorkloadWriterWorker::ConfiguratorLoop(const TTopicWorkloadConfiguratorParams& params, TInstant endTime)
{
    WRITE_LOG(params.Log, ELogPriority::TLOG_INFO, TStringBuilder() << "Configurator started " << Now().ToStringUpToSeconds());

    try {
        TTopicWorkloadConfiguratorWorker configurator(params);
        configurator.Process(endTime);
    } catch (const std::runtime_error& re) {
        WRITE_LOG(params.Log, ELogPriority::TLOG_ERR, TStringBuilder()
            << "Configurator failed with error: " << re.what());
    } catch (...) {
        WRITE_LOG(params.Log, ELogPriority::TLOG_ERR, TStringBuilder()
            << "Configurator caught unknown exception: " << CurrentExceptionMessage());
    }

    WRITE_LOG(params.Log, ELogPriority::TLOG_INFO, TStringBuilder() << "Configurator finished " << Now().ToStringUpToSeconds());
}

void TTopicWorkloadWriterWorker::RetryableDescriberLoop(const TTopicWorkloadDescriberParams& params)
{
    auto errorFlag = params.ErrorFlag;

    const TInstant endTime = Now() + TDuration::Seconds(params.TotalSec + 3);

    while (!*errorFlag && Now() < endTime) {
        try {
            DescriberLoop(params, endTime);
        } catch (const yexception& ex) {
            WRITE_LOG(params.Log, ELogPriority::TLOG_WARNING, TStringBuilder() << ex);
        }
    }
}

void TTopicWorkloadWriterWorker::DescriberLoop(const TTopicWorkloadDescriberParams& params, TInstant endTime)
{
    WRITE_LOG(params.Log, ELogPriority::TLOG_INFO, TStringBuilder() << "Describer started " << Now().ToStringUpToSeconds());

    try {
        TTopicWorkloadDescriberWorker describer(params);
        describer.Process(endTime);
    } catch (const std::runtime_error& re) {
        WRITE_LOG(params.Log, ELogPriority::TLOG_ERR, TStringBuilder()
            << "Describer failed with error: " << re.what());
    } catch (...) {
        WRITE_LOG(params.Log, ELogPriority::TLOG_ERR, TStringBuilder()
            << "Describer caught unknown exception: " << CurrentExceptionMessage());
    }

    WRITE_LOG(params.Log, ELogPriority::TLOG_INFO, TStringBuilder() << "Describer finished " << Now().ToStringUpToSeconds());
}

void TTopicWorkloadWriterWorker::TryCommitTx(TInstant& commitTime)
{
    Y_ABORT_UNLESS(TxSupport);
    NYdb::NConsoleClient::NTopicWorkloadWriterInternal::TryCommitTxCommon(
        Params,
        TxSupport,
        commitTime,
        WaitForCommitTx,
        [this](const TInstant& now, const TInstant& commitAt, size_t rows) {
            WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG, TStringBuilder()
                << "Not committing: "
                << "commit time: " << commitAt
                << " now: " << now
                << ", messages needed for commit: " << Params.CommitMessages
                << " current rows in transactions: " << rows
            );
        }
    );
}

void TTopicWorkloadWriterWorker::TryCommitTableChanges()
{
    if (TxSupport->Rows.empty()) {
        return;
    }

    WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG, TStringBuilder()<< "Starting commit");
    NYdb::NConsoleClient::NTopicWorkloadWriterInternal::CommitTableChangesCommon(Params, TxSupport);
}

TInstant TTopicWorkloadWriterWorker::GetCreateTimestampForNextMessage() {
    const TInstant expectedTs = Params.BytesPerSec == 0 ? TInstant() : GetExpectedCurrMessageCreationTimestamp();
    return NYdb::NConsoleClient::NTopicWorkloadWriterInternal::GetCreateTimestampForNextMessage(Params, expectedTs);
}
