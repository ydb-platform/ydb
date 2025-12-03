#include "sqs_workload.h"
#include "sqs_workload_run.h"
#include "sqs_workload_init.h"

namespace NYdb::NConsoleClient {

TCommandWorkloadSqs::TCommandWorkloadSqs() : TClientCommandTree("sqs", {}, "YDB sqs workload") {
    AddCommand(std::make_unique<TCommandWorkloadSqsRun>());
    AddCommand(std::make_unique<TCommandWorkloadSqsInit>());
}

}  // namespace NYdb::NConsoleClient
