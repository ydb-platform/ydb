#include "topic_workload_run_write.h"

#include "topic_workload_defines.h"
#include "topic_workload_describe.h"
#include "topic_workload_params.h"
#include "topic_workload_writer.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_service_topic.h>
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

TCommandWorkloadTopicRunWrite::TCommandWorkloadTopicRunWrite()
    : TWorkloadCommand("write", {}, "Write workload")
    , ErrorFlag(std::make_shared<std::atomic_bool>())
{
}

void TCommandWorkloadTopicRunWrite::Config(TConfig& config) {
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
    config.Opts->AddLongOption("percentile", "Percentile for output statistics.")
        .DefaultValue(50)
        .StoreResult(&Percentile);
    config.Opts->AddLongOption("warmup", "Warm-up time in seconds.")
        .DefaultValue(1)
        .StoreResult(&Warmup);
    config.Opts->AddLongOption("topic", "Topic name.")
        .DefaultValue(TOPIC)
        .StoreResult(&TopicName);

    // Specific params
    config.Opts->AddLongOption('t', "threads", "Number of producer threads.")
        .DefaultValue(1)
        .StoreResult(&ProducerThreadCount);
    config.Opts->AddLongOption('m', "message-size", "Message size.")
        .DefaultValue(10_KB)
        .StoreMappedResultT<TString>(&MessageSize, &TCommandWorkloadTopicParams::StrToBytes);
    config.Opts->AddLongOption("message-rate", "Total message rate for all producer threads (messages per second). Exclusive with --byte-rate.")
        .DefaultValue(0)
        .StoreResult(&MessageRate);
    config.Opts->AddLongOption("byte-rate", "Total message rate for all producer threads (bytes per second). Exclusive with --message-rate.")
        .DefaultValue(0)
        .StoreMappedResultT<TString>(&ByteRate, &TCommandWorkloadTopicParams::StrToBytes);
    config.Opts->AddLongOption("codec", PrepareAllowedCodecsDescription("Client-side compression algorithm. When read, data will be uncompressed transparently with a codec used on write", InitAllowedCodecs()))
        .Optional()
        .DefaultValue((TStringBuilder() << NTopic::ECodec::RAW))
        .StoreMappedResultT<TString>(&Codec, &TCommandWorkloadTopicParams::StrToCodec);


    config.Opts->MutuallyExclusive("message-rate", "byte-rate");

    config.IsNetworkIntensive = true;
}

void TCommandWorkloadTopicRunWrite::Parse(TConfig& config) 
{
    TClientCommand::Parse(config);

    if (Percentile >= 100) {
        throw TMisuseException() << "--percentile should be less than 100.";
    }
    if (Warmup >= Seconds) {
        throw TMisuseException() << "--warmup should be less than --seconds.";
    }
}

int TCommandWorkloadTopicRunWrite::Run(TConfig& config) {
    Log = std::make_shared<TLog>(CreateLogBackend("cerr", TClientCommand::TConfig::VerbosityLevelToELogPriority(config.VerbosityLevel)));
    Log->SetFormatter(GetPrefixLogFormatter(""));
    Driver = std::make_unique<NYdb::TDriver>(CreateDriver(config, CreateLogBackend("cerr", TClientCommand::TConfig::VerbosityLevelToELogPriority(config.VerbosityLevel))));
    StatsCollector = std::make_shared<TTopicWorkloadStatsCollector>(ProducerThreadCount, 0, Quiet, PrintTimestamp, WindowDurationSec, Seconds, Warmup, Percentile, ErrorFlag);
    StatsCollector->PrintHeader();

    auto describeTopicResult = TCommandWorkloadTopicDescribe::DescribeTopic(config.Database, TopicName, *Driver);
    ui32 partitionCount = describeTopicResult.GetTotalPartitionsCount();
    ui32 partitionSeed = RandomNumber<ui32>(partitionCount);

    std::vector<std::future<void>> threads;

    auto producerStartedCount = std::make_shared<std::atomic_uint>();
    for (ui32 writerIdx = 0; writerIdx < ProducerThreadCount; ++writerIdx) {
        TTopicWorkloadWriterParams writerParams{
            .Seconds = Seconds,
            .Driver = Driver.get(),
            .Log = Log,
            .StatsCollector = StatsCollector,
            .ErrorFlag = ErrorFlag,
            .StartedCount = producerStartedCount,
            .TopicName = TopicName,
            .ByteRate = MessageRate != 0 ? MessageRate * MessageSize : ByteRate,
            .ProducerThreadCount = ProducerThreadCount,
            .WriterIdx = writerIdx,
            .ProducerId = TGUID::CreateTimebased().AsGuidString(),
            .PartitionId = (partitionSeed + writerIdx) % partitionCount,
            .MessageSize = MessageSize,
            .Codec = Codec};

        threads.push_back(std::async([writerParams = std::move(writerParams)]() mutable { TTopicWorkloadWriterWorker::WriterLoop(std::move(writerParams)); }));
    }

    while (*producerStartedCount != ProducerThreadCount)
        Sleep(TDuration::MilliSeconds(10));

    StatsCollector->PrintWindowStatsLoop();

    for (auto& future : threads)
        future.wait();
    WRITE_LOG(Log, ELogPriority::TLOG_INFO, "All thread joined.");

    StatsCollector->PrintTotalStats();

    if (*ErrorFlag) {
        WRITE_LOG(Log, ELogPriority::TLOG_EMERG, "Problems occured while writing messages.")
        return EXIT_FAILURE;
    }

    if (StatsCollector->GetTotalWriteMessages() == 0) {
        WRITE_LOG(Log, ELogPriority::TLOG_EMERG, "No messages were written.");
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}
