#pragma once

#include "topic_workload_stats_collector.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_workload.h>
#include <ydb/public/lib/ydb_cli/commands/topic_write_scenario.h>

namespace NYdb::NConsoleClient {

class TCommandWorkloadTopicRunWrite : public TWorkloadCommand {
public:
    TCommandWorkloadTopicRunWrite();

    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    TTopicWriteScenario Scenario;
};

}
