#include "transfer_workload_topic_to_table.h"
#include "transfer_workload_topic_to_table_init.h"
#include "transfer_workload_topic_to_table_clean.h"
#include "transfer_workload_topic_to_table_run.h"
#include "transfer_workload_defines.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_common.h>

using namespace NYdb::NConsoleClient;

TCommandWorkloadTransferTopicToTable::TCommandWorkloadTransferTopicToTable() :
    TClientCommandTree("topic-to-table", {}, "Transfer from topic to table")
{
    AddCommand(std::make_unique<TCommandWorkloadTransferTopicToTableInit>());
    AddCommand(std::make_unique<TCommandWorkloadTransferTopicToTableClean>());
    AddCommand(std::make_unique<TCommandWorkloadTransferTopicToTableRun>());
}
