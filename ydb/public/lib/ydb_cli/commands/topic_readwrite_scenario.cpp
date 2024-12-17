#include "topic_readwrite_scenario.h"

#include <ydb/public/lib/ydb_cli/commands/topic_workload/topic_workload_defines.h>
#include <ydb/public/lib/ydb_cli/commands/topic_workload/topic_workload_describe.h>
#include <ydb/public/lib/ydb_cli/commands/topic_workload/topic_workload_stats_collector.h>
#include <ydb/public/lib/ydb_cli/commands/topic_workload/topic_workload_writer.h>

#include <util/random/random.h>

namespace NYdb::NConsoleClient {

int TTopicReadWriteScenario::DoRun(const TClientCommand::TConfig& config)
{
    auto describeTopicResult = TCommandWorkloadTopicDescribe::DescribeTopic(config.Database, TopicName, *Driver);
    ui32 partitionCount = describeTopicResult.GetTotalPartitionsCount();
    ui32 partitionSeed = RandomNumber<ui32>(partitionCount);

    std::vector<TString> generatedMessages =
        TTopicWorkloadWriterWorker::GenerateMessages(MessageSize);

    std::vector<std::future<void>> threads;

    StartConsumerThreads(threads, config.Database);
    StartProducerThreads(threads, partitionCount, partitionSeed, generatedMessages, config.Database);

    StatsCollector->PrintWindowStatsLoop();

    JoinThreads(threads);

    StatsCollector->PrintTotalStats();

    if (AnyErrors() || !AnyOutgoingMessages() || !AnyIncomingMessages()) {
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

}
