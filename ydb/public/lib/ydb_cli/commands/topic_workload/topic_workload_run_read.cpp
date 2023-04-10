#include "topic_workload_run_read.h"

#include "topic_workload_defines.h"
#include "topic_workload_params.h"
#include "topic_workload_reader.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_common.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>

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
        .DefaultValue(10)
        .StoreResult(&Seconds);
    config.Opts->AddLongOption('w', "window", "Output window duration in seconds.")
        .DefaultValue(1)
        .StoreResult(&WindowDurationSec);
    config.Opts->AddLongOption('q', "quiet", "Quiet mode. Doesn't print statistics each second.")
        .StoreTrue(&Quiet);
    config.Opts->AddLongOption("print-timestamp", "Print timestamp each second with statistics.")
        .StoreTrue(&PrintTimestamp);

    // Specific params
    config.Opts->AddLongOption('c', "consumers", "Number of consumers in a topic.")
        .DefaultValue(1)
        .StoreResult(&ConsumerCount);
    config.Opts->AddLongOption('t', "threads", "Number of consumer threads.")
        .DefaultValue(1)
        .StoreResult(&ConsumerThreadCount);
}

void TCommandWorkloadTopicRunRead::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandWorkloadTopicRunRead::Run(TConfig& config) {
    Log = std::make_shared<TLog>(CreateLogBackend("cerr", TClientCommand::TConfig::VerbosityLevelToELogPriority(config.VerbosityLevel)));
    Driver = std::make_unique<NYdb::TDriver>(CreateDriver(config, CreateLogBackend("cerr", TClientCommand::TConfig::VerbosityLevelToELogPriority(config.VerbosityLevel))));

    StatsCollector = std::make_shared<TTopicWorkloadStatsCollector>(false, true, Quiet, PrintTimestamp, WindowDurationSec, Seconds, ErrorFlag);
    StatsCollector->PrintHeader();

    std::vector<std::future<void>> threads;

    auto consumerStartedCount = std::make_shared<std::atomic_uint>();
    for (ui32 readerIdx = 0; readerIdx < ConsumerCount; ++readerIdx) {
        for (ui32 readerThreadIdx = 0; readerThreadIdx < ConsumerThreadCount; ++readerThreadIdx) {
            TTopicWorkloadReaderParams readerParams{
                .Seconds = Seconds,
                .Driver = Driver.get(),
                .Log = Log,
                .StatsCollector = StatsCollector,
                .ErrorFlag = ErrorFlag,
                .StartedCount = consumerStartedCount,

                .ConsumerIdx = readerIdx};

            threads.push_back(std::async([readerParams = std::move(readerParams)]() mutable { TTopicWorkloadReader::ReaderLoop(std::move(readerParams)); }));
        }
    }
    while (*consumerStartedCount != ConsumerThreadCount * ConsumerCount)
        Sleep(TDuration::MilliSeconds(10));

    StatsCollector->PrintWindowStatsLoop();

    for (auto& future : threads) {
        future.wait();
        WRITE_LOG(Log,ELogPriority::TLOG_INFO, "All thread joined.\n");
    }

    StatsCollector->PrintTotalStats();

    if (*ErrorFlag) {
        WRITE_LOG(Log,ELogPriority::TLOG_EMERG, "Problems occured while reading messages.\n");
        return EXIT_FAILURE;
    }

    if (StatsCollector->GetTotalReadMessages() == 0) {
        WRITE_LOG(Log,ELogPriority::TLOG_EMERG, "No messages were read.\n");
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}
