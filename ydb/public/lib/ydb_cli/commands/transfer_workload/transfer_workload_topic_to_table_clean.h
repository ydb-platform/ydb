#pragma once

#include <ydb/public/lib/ydb_cli/commands/ydb_workload.h>
#include <ydb/public/lib/ydb_cli/commands/topic_operations_scenario.h>

namespace NYdb::NConsoleClient {

class TCommandWorkloadTransferTopicToTableClean : public TWorkloadCommand {
public:
    TCommandWorkloadTransferTopicToTableClean();

    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    class TScenario : public TTopicOperationsScenario {
        int DoRun(const TConfig& config) override;
    };

    TScenario Scenario;
};

}
