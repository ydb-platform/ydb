#include "topic_read_scenario.h"

#include <ydb/public/lib/ydb_cli/commands/topic_workload/topic_workload_defines.h>
#include <ydb/public/lib/ydb_cli/commands/topic_workload/topic_workload_stats_collector.h>

#include <util/random/random.h>

namespace NYdb::NConsoleClient {

int TTopicReadScenario::DoRun(const TClientCommand::TConfig& config)
{
    std::vector<std::future<void>> threads;

    StartConsumerThreads(threads, config.Database);

    StatsCollector->PrintWindowStatsLoop();

    JoinThreads(threads);

    StatsCollector->PrintTotalStats();

    if (AnyErrors() || !AnyIncomingMessages()) {
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

}
