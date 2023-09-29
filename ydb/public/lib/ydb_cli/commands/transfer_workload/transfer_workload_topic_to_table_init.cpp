#include "transfer_workload_topic_to_table_init.h"
#include "transfer_workload_defines.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_common.h>

#include <util/random/random.h>

using namespace NYdb::NConsoleClient;

int TCommandWorkloadTransferTopicToTableInit::TScenario::DoRun(const TConfig& config)
{
    CreateTopic(config.Database, TopicName, TopicPartitionCount, ConsumerCount);
    CreateWriteOnlyTable(GetWriteOnlyTableName(), TablePartitionCount);
    CreateReadOnlyTable(GetReadOnlyTableName(), TablePartitionCount);

    return EXIT_SUCCESS;
}

void TCommandWorkloadTransferTopicToTableInit::TScenario::CreateWriteOnlyTable(const TString& name,
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

void TCommandWorkloadTransferTopicToTableInit::TScenario::CreateReadOnlyTable(const TString& name,
                                                                              ui32 partitionCount)
{
    TStringBuilder query;
    query << "CREATE TABLE `";
    query << name;
    query << "` (id Uint64, PRIMARY KEY (id)) WITH (UNIFORM_PARTITIONS = ";
    query << partitionCount;
    query << ", AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = ";
    query << partitionCount;
    query << ")";

    ExecSchemeQuery(query);

    for (int i = 0; i < 10; ++i) {
        UpsertRandomKeyBlock();
    }
}

void TCommandWorkloadTransferTopicToTableInit::TScenario::UpsertRandomKeyBlock()
{
    TString query = R"(
        DECLARE $rows AS List<Struct<
            id: Uint64
        >>;

        UPSERT INTO `)" + GetReadOnlyTableName() + R"(` (SELECT id FROM AS_TABLE($rows));
    )";

    NYdb::TParamsBuilder builder;

    auto& rows = builder.AddParam("$rows");
    rows.BeginList();
    for (int i = 0; i < 100'000; ++i) {
        rows.AddListItem()
            .BeginStruct()
            .AddMember("id").Uint64(RandomNumber<ui64>())
            .EndStruct();
    }
    rows.EndList();
    rows.Build();

    auto params = builder.Build();

    ExecDataQuery(query, params);
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
    config.Opts->AddLongOption("consumer-prefix", "Use consumers with names '<consumer-prefix>-0' ... '<consumer-prefix>-<n-1>' where n is set in the '--consumers' option.")
        .DefaultValue(NWorkloadTransfer::CONSUMER_PREFIX)
        .StoreResult(&Scenario.ConsumerPrefix);
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
