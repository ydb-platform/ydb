#pragma once

#include <ydb/public/lib/ydb_cli/commands/ydb_workload.h>
#include <ydb/public/lib/ydb_cli/commands/topic_read_scenario.h>

namespace NYdb::NConsoleClient {

class TCommandWorkloadTopicRunRead : public TWorkloadCommand {
public:
    TCommandWorkloadTopicRunRead();

    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    TTopicReadScenario Scenario;
};

}
