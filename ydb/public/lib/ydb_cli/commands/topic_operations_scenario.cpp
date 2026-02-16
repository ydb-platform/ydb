#include "topic_readwrite_scenario.h"

#include <ydb/public/lib/ydb_cli/commands/topic_workload/topic_workload_defines.h>
#include <ydb/public/lib/ydb_cli/commands/topic_workload/topic_workload_describe.h>
#include <ydb/public/lib/ydb_cli/commands/topic_workload/topic_workload_reader.h>
#include <ydb/public/lib/ydb_cli/commands/topic_workload/topic_workload_keyed_writer.h>
#include <ydb/public/lib/ydb_cli/commands/topic_workload/topic_workload_writer.h>
#include <ydb/public/lib/ydb_cli/commands/topic_workload/topic_workload_configurator.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_common.h>
#include <ydb/public/lib/ydb_cli/common/log.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/internal/logger/log.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <util/generic/guid.h>

namespace NYdb::NConsoleClient {

using TConfig = TClientCommand::TConfig;

TTopicOperationsScenario::TTopicOperationsScenario() :
    ErrorFlag(std::make_shared<std::atomic_bool>())
{
}

int TTopicOperationsScenario::Run(TConfig& config)
{
    InitLog(config);
    InitDriver(config);
    InitStatsCollector();

    return DoRun(config);
}

void TTopicOperationsScenario::EnsurePercentileIsValid() const
{
    if (Percentile > 100 || Percentile <= 0) {
        throw TMisuseException() << "--percentile should be in range (0,100].";
    }
}

void TTopicOperationsScenario::EnsureWarmupSecIsValid() const
{
    if (WarmupSec >= TotalSec) {
        throw TMisuseException() << "--warmup should be less than --seconds.";
    }
}

void TTopicOperationsScenario::EnsureRatesIsValid() const
{
    Y_ENSURE_EX(MessagesPerSec >= 0, TMisuseException() << "--messages-per-sec should be non negative.");
    Y_ENSURE_EX(BytesPerSec >= 0, TMisuseException() << "--bytes-per-sec should be non negative.");
}

TString TTopicOperationsScenario::GetReadOnlyTableName() const
{
    return TableName + "-ro";
}

TString TTopicOperationsScenario::GetWriteOnlyTableName() const
{
    return TableName;
}

ui32 TTopicOperationsScenario::GetTopicMaxPartitionCount() const
{
    return TopicMaxPartitionCount >= TopicPartitionCount ? TopicMaxPartitionCount : (TopicPartitionCount << 3);
}

THolder<TLogBackend> TTopicOperationsScenario::MakeLogBackend(ui32 level)
{
    return CreateLogBackend("cerr",
                            VerbosityLevelToELogPriority(level));
}

void TTopicOperationsScenario::InitLog(TConfig& config)
{
    Log = std::make_shared<TLog>(MakeLogBackend(config.VerbosityLevel));
    Log->SetFormatter(GetPrefixLogFormatter(""));
}

void TTopicOperationsScenario::InitDriver(TConfig& config)
{
    Driver =
        std::make_unique<NYdb::TDriver>(TYdbCommand::CreateDriver(config,
                                                                  std::unique_ptr<TLogBackend>(MakeLogBackend(config.VerbosityLevel).Release())));
}

void TTopicOperationsScenario::InitStatsCollector()
{
    StatsCollector =
        std::make_shared<TTopicWorkloadStatsCollector>(ProducerThreadCount,
                                                       ConsumerCount * ConsumerThreadCount,
                                                       Quiet,
                                                       PrintTimestamp,
                                                       WindowSec.Seconds(),
                                                       TotalSec.Seconds(),
                                                       WarmupSec.Seconds(),
                                                       Percentile,
                                                       ErrorFlag,
                                                       UseTransactions);
}

void TTopicOperationsScenario::CreateTopic(const TString& database,
                                           const TString& topic,
                                           ui32 partitionCount,
                                           ui32 consumerCount,
                                           bool autoscaling,
                                           ui32 maxPartitionCount,
                                           ui32 stabilizationWindowSeconds,
                                           ui32 upUtilizationPercent,
                                           ui32 downUtilizationPercent,
                                           bool cleanupPopicyCompact)
{
    auto topicPath =
        TCommandWorkloadTopicDescribe::GenerateFullTopicName(database, topic);

    EnsureTopicNotExist(topicPath);
    CreateTopic(topicPath, partitionCount, consumerCount, autoscaling, maxPartitionCount, stabilizationWindowSeconds, upUtilizationPercent, downUtilizationPercent, cleanupPopicyCompact);
}

void TTopicOperationsScenario::DropTopic(const TString& database,
                                         const TString& topic)
{
    Y_ABORT_UNLESS(Driver);

    NTopic::TTopicClient client(*Driver);
    auto topicPath =
        TCommandWorkloadTopicDescribe::GenerateFullTopicName(database, topic);

    auto result = client.DropTopic(topicPath).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);
}

void TTopicOperationsScenario::DropTable(const TString& database, const TString& table)
{
    NTable::TTableClient client(*Driver);
    auto session = GetSession(client);
    auto result = session.DropTable(database + "/" + table).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);
}

void TTopicOperationsScenario::ExecSchemeQuery(const TString& query)
{
    NTable::TTableClient client(*Driver);
    auto session = GetSession(client);
    auto result = session.ExecuteSchemeQuery(query).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);
}

void TTopicOperationsScenario::ExecDataQuery(const TString& query,
                                             const NYdb::TParams& params)
{
    NTable::TTableClient client(*Driver);
    auto session = GetSession(client);
    auto result = session.ExecuteDataQuery(query,
                                           NTable::TTxControl::BeginTx(NTable::TTxSettings::SerializableRW()).CommitTx(),
                                           params).ExtractValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);
}

void TTopicOperationsScenario::EnsureTopicNotExist(const TString& topic)
{
    Y_ABORT_UNLESS(Driver);

    NTopic::TTopicClient client(*Driver);

    auto result = client.DescribeTopic(topic, {}).GetValueSync();

    if (result.GetTopicDescription().GetTotalPartitionsCount() != 0) {
        ythrow yexception() << "Topic '" << topic << "' already exists.";
    }
}

void TTopicOperationsScenario::CreateTopic(const TString& topic,
                                           ui32 partitionCount,
                                           ui32 consumerCount,
                                           bool autoscaling,
                                           ui32 maxPartitionCount,
                                           ui32 stabilizationWindowSeconds,
                                           ui32 upUtilizationPercent,
                                           ui32 downUtilizationPercent,
                                           bool cleanupPolicyCompact)
{
    Y_ABORT_UNLESS(Driver);

    NTopic::TTopicClient client(*Driver);

    NTopic::TCreateTopicSettings settings;
    if (autoscaling) {
        settings.BeginConfigurePartitioningSettings()
            .MinActivePartitions(partitionCount)
            .MaxActivePartitions(maxPartitionCount)
            .BeginConfigureAutoPartitioningSettings()
                .Strategy(NTopic::EAutoPartitioningStrategy::ScaleUpAndDown)
                .StabilizationWindow(TDuration::Seconds(stabilizationWindowSeconds))
                .UpUtilizationPercent(upUtilizationPercent)
                .DownUtilizationPercent(downUtilizationPercent)
            .EndConfigureAutoPartitioningSettings()
            .EndConfigurePartitioningSettings();
    } else {
        settings.PartitioningSettings(partitionCount, partitionCount);
    }
    if (cleanupPolicyCompact) {
        settings.AddAttribute("_cleanup_policy", "compact");
    }

    for (unsigned consumerIdx = 0; consumerIdx < consumerCount; ++consumerIdx) {
        settings
            .BeginAddConsumer(TCommandWorkloadTopicDescribe::GenerateConsumerName(ConsumerPrefix, consumerIdx))
            .EndAddConsumer();
    }

    auto result = client.CreateTopic(topic, settings).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);
}

NTable::TSession TTopicOperationsScenario::GetSession(NTable::TTableClient& client)
{
    auto result = client.GetSession({}).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);
    return result.GetSession();
}

void TTopicOperationsScenario::StartConsumerThreads(std::vector<std::future<void>>& threads,
                                                    const TString& database)
{
    auto count = std::make_shared<std::atomic_uint>();

    for (ui32 consumerIdx = 0, readerIdx = 0; consumerIdx < ConsumerCount; ++consumerIdx) {
        for (ui32 threadIdx = 0; threadIdx < ConsumerThreadCount; ++threadIdx, ++readerIdx) {
            TTopicWorkloadReaderParams readerParams{
                .TotalSec = TotalSec.Seconds(),
                .Driver = *Driver,
                .Log = Log,
                .StatsCollector = StatsCollector,
                .ErrorFlag = ErrorFlag,
                .StartedCount = count,
                .Database = database,
                .TopicName = TopicName,
                .TableName = GetWriteOnlyTableName(),
                .ReadOnlyTableName = GetReadOnlyTableName(),
                .ConsumerIdx = consumerIdx,
                .ConsumerPrefix = ConsumerPrefix,
                .ReaderIdx = readerIdx,
                .UseTransactions = UseTransactions,
                .UseTopicCommit = OnlyTopicInTx,
                .UseTableSelect = UseTableSelect && !OnlyTopicInTx,
                .UseTableUpsert = !OnlyTopicInTx,
                .RestartInterval = RestartInterval,
                .ReadWithoutCommit = ReadWithoutCommit,
                .ReadWithoutConsumer = ReadWithoutConsumer,
                .CommitPeriodMs = TxCommitIntervalMs != 0 ? TxCommitIntervalMs : CommitPeriodSeconds * 1000, // seconds to ms conversion,
                .CommitMessages = CommitMessages,
                .MaxMemoryUsageBytes = ConsumerMaxMemoryUsageBytes,
            };

            threads.push_back(std::async([readerParams = std::move(readerParams)]() { TTopicWorkloadReader::RetryableReaderLoop(readerParams); }));
        }
    }

    while (*count != ConsumerThreadCount * ConsumerCount) {
        Sleep(TDuration::MilliSeconds(10));
    }
}

/*!
 * This method starts producers threads specified in -t option, that will write to topic in parallel. Every producer thread will create
 * WriteSession for every partition in the topic and will write in partitions in round robin manner.
 * */
void TTopicOperationsScenario::StartProducerThreads(std::vector<std::future<void>>& threads,
                                                    ui32 partitionCount,
                                                    ui32 partitionSeed,
                                                    const std::vector<TString>& generatedMessages,
                                                    const TString& database)
{
    auto describeTopicResult = TCommandWorkloadTopicDescribe::DescribeTopic(database, TopicName, *Driver);
    bool useAutoPartitioning = NYdb::NTopic::EAutoPartitioningStrategy::Disabled != describeTopicResult.GetPartitioningSettings().GetAutoPartitioningSettings().GetStrategy();

    auto count = std::make_shared<std::atomic_uint>();
    for (ui32 writerIdx = 0; writerIdx < ProducerThreadCount; ++writerIdx) {
        TTopicWorkloadWriterParams writerParams{
            .TotalSec = TotalSec.Seconds(),
            .WarmupSec = WarmupSec.Seconds(),
            .Driver = *Driver,
            .Log = Log,
            .StatsCollector = StatsCollector,
            .ErrorFlag = ErrorFlag,
            .StartedCount = count,
            .GeneratedMessages = generatedMessages,
            .Database = database,
            .TopicName = TopicName,
            .BytesPerSec = MessagesPerSec != 0 ? MessagesPerSec * MessageSizeBytes : BytesPerSec,
            .MessageSize = MessageSizeBytes,
            .ProducerThreadCount = ProducerThreadCount,
            .WriterIdx = writerIdx,
            .PartitionCount = partitionCount,
            .PartitionSeed = partitionSeed,
            .Direct = Direct,
            .Codec = Codec,
            .UseTransactions = UseTransactions,
            .UseAutoPartitioning = useAutoPartitioning,
            .CommitIntervalMs = TxCommitIntervalMs != 0 ? TxCommitIntervalMs : CommitPeriodSeconds * 1000, // seconds to ms conversion
            .CommitMessages = CommitMessages,
            .UseCpuTimestamp = UseCpuTimestamp,
            .KeyPrefix = KeyPrefix,
            .KeyCount = KeyCount,
            .KeySeed = writerIdx,
            .MaxMemoryUsageBytes = ProducerMaxMemoryUsageBytes,
        };

        if (KeyedWrites) {
            TTopicWorkloadKeyedWriterParams keyedWriterParams(writerParams);
            keyedWriterParams.ProducerKeysCount = ProducerKeysCount;

            threads.push_back(std::async([keyedWriterParams = std::move(keyedWriterParams)]() {
                TTopicWorkloadKeyedWriterWorker::RetryableWriterLoop(keyedWriterParams);
            }));
        } else {
            threads.push_back(std::async([writerParams = std::move(writerParams)]() {
                TTopicWorkloadWriterWorker::RetryableWriterLoop(writerParams);
            }));
        }
    }

    while (*count != ProducerThreadCount) {
        Sleep(TDuration::MilliSeconds(10));
    }
}

void TTopicOperationsScenario::StartConfiguratorThread(std::vector<std::future<void>>& threads,
                                                       const TString& database)
{
    if (!ConfigConsumerCount) {
        return;
    }

    TTopicWorkloadConfiguratorParams params{
        .TotalSec = TotalSec.Seconds(),
        .WarmupSec = WarmupSec.Seconds(),
        .Driver = *Driver,
        .Log = Log,
        .ErrorFlag = ErrorFlag,
        .Database = database,
        .TopicName = TopicName,
        .ConsumerCount = ConfigConsumerCount
    };
    threads.push_back(std::async([params = std::move(params)]() { TTopicWorkloadWriterWorker::RetryableConfiguratorLoop(params); }));
}

void TTopicOperationsScenario::StartDescriberThread(std::vector<std::future<void>>& threads,
                                                    const TString& database)
{
    if (!NeedDescribeTopic && DescribeConsumerName.empty()) {
        return;
    }

    TTopicWorkloadDescriberParams params{
        .TotalSec = TotalSec.Seconds(),
        .WarmupSec = WarmupSec.Seconds(),
        .Driver = *Driver,
        .Log = Log,
        .ErrorFlag = ErrorFlag,
        .Database = database,
        .TopicName = TopicName,
        .ConsumerName = DescribeConsumerName,
        .NeedDescribeTopic = NeedDescribeTopic,
        .NeedDescribeConsumer = !DescribeConsumerName.empty()
    };
    threads.push_back(std::async([params = std::move(params)]() { TTopicWorkloadWriterWorker::RetryableDescriberLoop(params); }));
}

void TTopicOperationsScenario::JoinThreads(const std::vector<std::future<void>>& threads)
{
    for (auto& future : threads) {
        future.wait();
    }

    WRITE_LOG(Log, ELogPriority::TLOG_INFO, "All thread joined.");
}

bool TTopicOperationsScenario::AnyErrors() const
{
    if (!*ErrorFlag) {
        return false;
    }

    WRITE_LOG(Log, ELogPriority::TLOG_EMERG, "Problems occured while processing messages.");

    return true;
}

bool TTopicOperationsScenario::AnyIncomingMessages() const
{
    if (StatsCollector->GetTotalReadMessages()) {
        return true;
    }

    WRITE_LOG(Log, ELogPriority::TLOG_EMERG, "No messages were read.");

    return false;
}

bool TTopicOperationsScenario::AnyOutgoingMessages() const
{
    if (StatsCollector->GetTotalWriteMessages()) {
        return true;
    }

    WRITE_LOG(Log, ELogPriority::TLOG_EMERG, "No messages were written.");

    return false;
}

}
