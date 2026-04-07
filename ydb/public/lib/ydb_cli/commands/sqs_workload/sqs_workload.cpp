#include "sqs_workload.h"
#include "sqs_workload_clean.h"
#include "sqs_workload_init.h"
#include "sqs_workload_run.h"

namespace NYdb::NConsoleClient {

    TCommandWorkloadSqs::TCommandWorkloadSqs()
        : TClientCommandTree("sqs", {}, "YDB sqs workload")
    {
        AddCommand(std::make_unique<TCommandWorkloadSqsRun>());
        AddCommand(std::make_unique<TCommandWorkloadSqsInit>());
        AddCommand(std::make_unique<TCommandWorkloadSqsClean>());
    }

} // namespace NYdb::NConsoleClient
