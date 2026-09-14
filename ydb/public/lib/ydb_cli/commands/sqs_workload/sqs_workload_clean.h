#pragma once

#include "sqs_workload_clean_scenario.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_workload.h>
#include <ydb/public/lib/ydb_cli/common/command.h>

namespace NYdb::NConsoleClient {

    class TCommandWorkloadSqsClean: public TWorkloadCommand {
    public:
        TCommandWorkloadSqsClean();
        void Config(TConfig& config) override;
        void Parse(TConfig& config) override;
        int Run(TConfig& config) override;

    private:
        TSqsWorkloadCleanScenario Scenario;
    };

} // namespace NYdb::NConsoleClient
