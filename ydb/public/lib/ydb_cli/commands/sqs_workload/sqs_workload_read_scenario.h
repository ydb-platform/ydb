#pragma once

#include <ydb/public/lib/ydb_cli/common/command.h>
#include <ydb/public/lib/ydb_cli/commands/sqs_workload/sqs_workload_scenario.h>

namespace NYdb::NConsoleClient {

class TSqsWorkloadReadScenario : public TSqsWorkloadScenario {
public:
    int Run(const TClientCommand::TConfig&);

    std::optional<TString> DlqQueueName = std::nullopt;
    std::optional<TString> DlqEndPoint = std::nullopt;
    std::optional<ui32> ErrorMessagesRate = std::nullopt;
    ui64 VisibilityTimeoutMs;
    ui64 HandleMessageDelayMs;
};

} // namespace NYdb::NConsoleClient
