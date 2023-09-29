#include "transfer_workload_topic_to_table_clean.h"
#include "transfer_workload_defines.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_common.h>

using namespace NYdb::NConsoleClient;

int TCommandWorkloadTransferTopicToTableClean::TScenario::DoRun(const TConfig& config)
{
    DropTopic(config.Database, TopicName);
    DropTable(config.Database, GetWriteOnlyTableName());
    DropTable(config.Database, GetReadOnlyTableName());

    return EXIT_SUCCESS;
}

TCommandWorkloadTransferTopicToTableClean::TCommandWorkloadTransferTopicToTableClean() :
    TWorkloadCommand("clean", {}, "Deletes objects created at the initialization stage")
{
}

void TCommandWorkloadTransferTopicToTableClean::Config(TConfig& config)
{
    TYdbCommand::Config(config);

    config.SetFreeArgsNum(0);

    config.Opts->AddLongOption("topic", "Topic name.")
        .DefaultValue(NWorkloadTransfer::TOPIC)
        .StoreResult(&Scenario.TopicName);
    config.Opts->AddLongOption("table", "Table name.")
        .DefaultValue(NWorkloadTransfer::TABLE)
        .StoreResult(&Scenario.TableName);
}

void TCommandWorkloadTransferTopicToTableClean::Parse(TConfig& config)
{
    TClientCommand::Parse(config);
}

int TCommandWorkloadTransferTopicToTableClean::Run(TConfig& config)
{
    return Scenario.Run(config);
}
