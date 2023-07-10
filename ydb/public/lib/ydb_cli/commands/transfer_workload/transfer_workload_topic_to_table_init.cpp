#include "transfer_workload_topic_to_table_init.h"
#include "transfer_workload_defines.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_common.h>

using namespace NYdb::NConsoleClient;

int TCommandWorkloadTransferTopicToTableInit::TScenario::DoRun(const TConfig& config)
{
    CreateTopic(config.Database, TopicName, TopicPartitionCount, ConsumerCount);
    CreateTable(TableName, TablePartitionCount);

    return EXIT_SUCCESS;
}

void TCommandWorkloadTransferTopicToTableInit::TScenario::CreateTable(const TString& name,
                                                                      ui32 partitionCount)
{
    TStringBuilder query;
    query << "CREATE TABLE `";
    query << name;
    query << "` (id Uint64, value String, PRIMARY KEY (id)) WITH (UNIFORM_PARTITIONS = ";
    query << partitionCount;
    query << ", AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = ";
    query << partitionCount;
    query << ")";

    ExecSchemeQuery(query);
}

TCommandWorkloadTransferTopicToTableInit::TCommandWorkloadTransferTopicToTableInit() :
    TWorkloadCommand("init", {}, "Creates and initializes objects")
{
}

void TCommandWorkloadTransferTopicToTableInit::Config(TConfig& config)
{
    TYdbCommand::Config(config);

    config.SetFreeArgsNum(0);

    config.Opts->AddLongOption("topic", "Topic name.")
        .DefaultValue(NWorkloadTransfer::TOPIC)
        .StoreResult(&Scenario.TopicName);
    config.Opts->AddLongOption("table", "Table name.")
        .DefaultValue(NWorkloadTransfer::TABLE)
        .StoreResult(&Scenario.TableName);

    config.Opts->AddLongOption("consumers", "Number of consumers in the topic.")
        .DefaultValue(1)
        .StoreResult(&Scenario.ConsumerCount);

    config.Opts->AddLongOption("topic-partitions", "Number of partitions in the source topic.")
        .DefaultValue(128)
        .StoreResult(&Scenario.TopicPartitionCount);
    config.Opts->AddLongOption("table-partitions", "Number of partitons in table.")
        .DefaultValue(128)
        .StoreResult(&Scenario.TablePartitionCount);
}

void TCommandWorkloadTransferTopicToTableInit::Parse(TConfig& config)
{
    TClientCommand::Parse(config);
}

int TCommandWorkloadTransferTopicToTableInit::Run(TConfig& config)
{
    return Scenario.Run(config);
}
