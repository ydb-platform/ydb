#pragma once

#include "sqs_workload_init_scenario.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_workload.h>
#include <ydb/public/lib/ydb_cli/common/command.h>

namespace NYdb::NConsoleClient {

    class TCommandWorkloadSqsInit: public TWorkloadCommand {
    public:
        TCommandWorkloadSqsInit();
        void Config(TConfig& config) override;
        void Parse(TConfig& config) override;
        int Run(TConfig& config) override;

    private:
        TSqsWorkloadInitScenario Scenario;
    };

} // namespace NYdb::NConsoleClient
