#include "topic_workload_run_read.h"

#include "topic_workload_defines.h"
#include "topic_workload_params.h"
#include "topic_workload_reader.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_common.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/logger/log.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <util/generic/guid.h>

#include <sstream>
#include <future>
#include <thread>
#include <iomanip>

using namespace NYdb::NConsoleClient;

TCommandWorkloadTopicRunRead::TCommandWorkloadTopicRunRead()
    : TWorkloadCommand("read", {}, "Read workload")
    , ErrorFlag(std::make_shared<std::atomic_bool>())
{
}

void TCommandWorkloadTopicRunRead::Config(TConfig& config) {
    TYdbCommand::Config(config);

    config.SetFreeArgsNum(0);

    // Common params
    config.Opts->AddLongOption('s', "seconds", "Seconds to run workload.")
        .DefaultValue(60)
        .StoreResult(&TotalSec);
    config.Opts->AddLongOption('w', "window", "Output window duration in seconds.")
        .DefaultValue(1)
        .StoreResult(&WindowSec);
    config.Opts->AddLongOption('q', "quiet", "Quiet mode. Doesn't print statistics each second.")
        .StoreTrue(&Quiet);
    config.Opts->AddLongOption("print-timestamp", "Print timestamp each second with statistics.")
        .StoreTrue(&PrintTimestamp);
    config.Opts->AddLongOption("percentile", "Percentile for output statistics.")
        .DefaultValue(50)
        .StoreResult(&Percentile);
    config.Opts->AddLongOption("warmup", "Warm-up time in seconds.")
        .DefaultValue(5)
        .StoreResult(&WarmupSec);
    config.Opts->AddLongOption("topic", "Topic name.")
        .DefaultValue(TOPIC)
        .StoreResult(&TopicName);

    // Specific params
    config.Opts->AddLongOption('c', "consumers", "Number of consumers in a topic.")
        .DefaultValue(1)
        .StoreResult(&ConsumerCount);
    config.Opts->AddLongOption('t', "threads", "Number of consumer threads.")
        .DefaultValue(1)
        .StoreResult(&ConsumerThreadCount);

    config.IsNetworkIntensive = true;
}

void TCommandWorkloadTopicRunRead::Parse(TConfig& config) 
{
    TClientCommand::Parse(config);

    if (Percentile > 100 || Percentile <= 0) {
        throw TMisuseException() << "--percentile should be in range (0,100].";
    }
    if (WarmupSec >= TotalSec) {
        throw TMisuseException() << "--warmup should be less than --seconds.";
    }
}

int TCommandWorkloadTopicRunRead::Run(TConfig& config) {
    Log = std::make_shared<TLog>(CreateLogBackend("cerr", TClientCommand::TConfig::VerbosityLevelToELogPriority(config.VerbosityLevel)));
    Log->SetFormatter(GetPrefixLogFormatter(""));
    Driver = std::make_unique<NYdb::TDriver>(CreateDriver(config, CreateLogBackend("cerr", TClientCommand::TConfig::VerbosityLevelToELogPriority(config.VerbosityLevel))));

    StatsCollector = std::make_shared<TTopicWorkloadStatsCollector>(0, ConsumerCount * ConsumerThreadCount, Quiet, PrintTimestamp, WindowSec, TotalSec, WarmupSec, Percentile, ErrorFlag);
    StatsCollector->PrintHeader();

    std::vector<std::future<void>> threads;

    auto consumerStartedCount = std::make_shared<std::atomic_uint>();
    for (ui32 consumerIdx = 0; consumerIdx < ConsumerCount; ++consumerIdx) {
        for (ui32 consumerThreadIdx = 0; consumerThreadIdx < ConsumerThreadCount; ++consumerThreadIdx) {
            TTopicWorkloadReaderParams readerParams{
                .TotalSec = TotalSec,
                .Driver = Driver.get(),
                .Log = Log,
                .StatsCollector = StatsCollector,
                .ErrorFlag = ErrorFlag,
                .StartedCount = consumerStartedCount,
                .Database = config.Database,
                .TopicName = TopicName,
                .ConsumerIdx = consumerIdx,
                .ReaderIdx = consumerIdx * ConsumerCount + consumerThreadIdx};

            threads.push_back(std::async([readerParams = std::move(readerParams)]() mutable { TTopicWorkloadReader::ReaderLoop(std::move(readerParams)); }));
        }
    }
    while (*consumerStartedCount != ConsumerThreadCount * ConsumerCount)
        Sleep(TDuration::MilliSeconds(10));

    StatsCollector->PrintWindowStatsLoop();

    for (auto& future : threads) {
        future.wait();
        WRITE_LOG(Log, ELogPriority::TLOG_INFO, "All thread joined.");
    }

    StatsCollector->PrintTotalStats();

    if (*ErrorFlag) {
        WRITE_LOG(Log, ELogPriority::TLOG_EMERG, "Problems occured while reading messages.");
        return EXIT_FAILURE;
    }

    if (StatsCollector->GetTotalReadMessages() == 0) {
        WRITE_LOG(Log, ELogPriority::TLOG_EMERG, "No messages were read.");
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}
