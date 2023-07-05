#pragma once

#include <ydb/public/lib/ydb_cli/commands/ydb_workload.h>
#include <ydb/public/lib/ydb_cli/commands/topic_readwrite_scenario.h>

namespace NYdb::NConsoleClient {

class TCommandWorkloadTopicRunFull : public TWorkloadCommand {
public:
    TCommandWorkloadTopicRunFull();

    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    TTopicReadWriteScenario Scenario;
};

}
