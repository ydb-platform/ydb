#pragma once

#include <ydb/public/lib/ydb_cli/commands/ydb_workload.h>
#include <ydb/public/lib/ydb_cli/commands/sqs_workload/sqs_workload_read_scenario.h>

namespace NYdb::NConsoleClient {

class TCommandWorkloadSqsRunRead : public TWorkloadCommand {
public:
    TCommandWorkloadSqsRunRead();

    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    TSqsWorkloadReadScenario Scenario;
};

}

