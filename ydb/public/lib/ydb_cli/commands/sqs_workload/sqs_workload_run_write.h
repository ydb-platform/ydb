#pragma once

#include <ydb/public/lib/ydb_cli/commands/sqs_workload/sqs_workload_write_scenario.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_workload.h>

namespace NYdb::NConsoleClient {

    class TCommandWorkloadSqsRunWrite: public TWorkloadCommand {
    public:
        TCommandWorkloadSqsRunWrite();

        void Config(TConfig& config) override;
        void Parse(TConfig& config) override;
        int Run(TConfig& config) override;

    private:
        TSqsWorkloadWriteScenario Scenario;
    };

} // namespace NYdb::NConsoleClient
