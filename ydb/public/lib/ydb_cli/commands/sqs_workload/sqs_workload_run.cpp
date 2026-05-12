#include "sqs_workload_run.h"
#include "sqs_workload_run_read.h"
#include "sqs_workload_run_write.h"

namespace NYdb::NConsoleClient {

    TCommandWorkloadSqsRun::TCommandWorkloadSqsRun()
        : TClientCommandTree("run", {}, "Run SQS workload")
    {
        AddCommand(std::make_unique<TCommandWorkloadSqsRunWrite>());
        AddCommand(std::make_unique<TCommandWorkloadSqsRunRead>());
    }

} // namespace NYdb::NConsoleClient
