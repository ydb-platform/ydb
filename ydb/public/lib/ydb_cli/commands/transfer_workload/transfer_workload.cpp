#include "transfer_workload.h"
#include "transfer_workload_topic_to_table.h"

using namespace NYdb::NConsoleClient;

TCommandWorkloadTransfer::TCommandWorkloadTransfer()
    : TClientCommandTree("transfer", {}, "YDB transfer workload")
{
    AddCommand(std::make_unique<TCommandWorkloadTransferTopicToTable>());
}
