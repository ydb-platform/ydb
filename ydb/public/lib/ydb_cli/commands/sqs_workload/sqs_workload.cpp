#include "sqs_workload.h"
#include "sqs_workload_run.h"

namespace NYdb::NConsoleClient {

TCommandWorkloadSqs::TCommandWorkloadSqs() : TClientCommandTree("sqs", {}, "YDB sqs workload") {
    AddCommand(std::make_unique<TCommandWorkloadSqsRun>());
}

}  // namespace NYdb::NConsoleClient
