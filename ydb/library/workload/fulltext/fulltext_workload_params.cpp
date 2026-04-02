#include "fulltext_workload_params.h"
#include "fulltext_workload_generator.h"
#include "fulltext_data_generator.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

namespace NYdbWorkload {

    void TFulltextWorkloadParams::ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) {
        switch (commandType) {
            case TWorkloadParams::ECommandType::Root:
                opts.AddLongOption("path", "Path to workload table.")
                    .DefaultValue(TableName)
                    .StoreResult(&TableName);
                break;
            case TWorkloadParams::ECommandType::Init:
                opts.AddLongOption("min-partitions", "Minimum number of partitions.")
                    .DefaultValue(MinPartitions)
                    .StoreResult(&MinPartitions);
                opts.AddLongOption("partition-size", "Partition size in MB.")
                    .DefaultValue(PartitionSizeMb)
                    .StoreResult(&PartitionSizeMb);
                opts.AddLongOption("auto-partition", "Enable auto partitioning by load.")
                    .DefaultValue(AutoPartitioningByLoad)
                    .StoreResult(&AutoPartitioningByLoad);
                break;
            case TWorkloadParams::ECommandType::Import:
                opts.AddLongOption("index", "Fulltext index name.")
                    .DefaultValue(IndexName)
                    .StoreResult(&IndexName);
                opts.AddLongOption("index-type", "Fulltext index type (fulltext_plain, fulltext_relevance).")
                    .DefaultValue(IndexType)
                    .StoreResult(&IndexType);
                opts.AddLongOption("index-param", "Fulltext index param. Can be specified multiple times. Format: `--index-param=\"<name>=<value>\" --index-param=...`.")
                    .InsertTo(&IndexParams)
                    .DefaultValue("tokenizer=standard");
                break;
            case TWorkloadParams::ECommandType::Run:
                RunWorkloadType = workloadType;
                opts.AddLongOption("index-name", "Fulltext index name.")
                    .DefaultValue(IndexName)
                    .StoreResult(&IndexName);
                switch (static_cast<EFulltextWorkloadType>(workloadType)) {
                    case EFulltextWorkloadType::Select:
                        opts.AddLongOption("query-table", "Name of the table with predefined queries. The table must have a 'query' column.")
                            .DefaultValue("")
                            .StoreResult(&QueryTable);
                        opts.AddLongOption("top-size", "Number of rows to sample from the table to build the query word set")
                            .DefaultValue(TopSize)
                            .StoreResult(&TopSize);
                        opts.AddLongOption("limit", "Limit rows in result set")
                            .DefaultValue(Limit)
                            .StoreResult(&Limit);
                        opts.AddLongOption('m', "model", "Path to Markov chain model file (.tsv.gz) for generating queries")
                            .RequiredArgument("PATH")
                            .StoreResult(&ModelPath);
                        opts.AddLongOption("min-query-len", "Minimum number of words in a generated query")
                            .DefaultValue(SelectMinQueryLen)
                            .StoreResult(&SelectMinQueryLen);
                        opts.AddLongOption("max-query-len", "Maximum number of words in a generated query")
                            .DefaultValue(SelectMaxQueryLen)
                            .StoreResult(&SelectMaxQueryLen);
                        break;
                    case EFulltextWorkloadType::Upsert:
                        opts.AddLongOption("bulk-size", "Number of rows in a upsert batch")
                            .DefaultValue(UpsertBulkSize)
                            .StoreResult(&UpsertBulkSize);
                        opts.AddLongOption('m', "model", "Path to Markov chain model file (.tsv.gz)")
                            .RequiredArgument("PATH")
                            .Required()
                            .StoreResult(&ModelPath);
                        opts.AddLongOption("min-sentence-len", "Minimum number of words in a generated sentence")
                            .DefaultValue(UpsertMinSentenceLen)
                            .StoreResult(&UpsertMinSentenceLen);
                        opts.AddLongOption("max-sentence-len", "Maximum number of words in a generated sentence")
                            .DefaultValue(UpsertMaxSentenceLen)
                            .StoreResult(&UpsertMaxSentenceLen);
                        break;
                }
                break;
            default:
                break;
        }
    }

    THolder<IWorkloadQueryGenerator> TFulltextWorkloadParams::CreateGenerator() const {
        return MakeHolder<TFulltextWorkloadGenerator>(this);
    }

    TWorkloadDataInitializer::TList TFulltextWorkloadParams::CreateDataInitializers() const {
        return {
            std::make_shared<TFulltextFilesDataInitializer>(*this),
            std::make_shared<TFulltextGeneratorDataInitializer>(*this),
        };
    }

    TString TFulltextWorkloadParams::GetWorkloadName() const {
        return "fulltext";
    }

    void TFulltextWorkloadParams::Validate(const ECommandType commandType, int workloadType) {
        // Validation logic if needed
        Y_UNUSED(commandType);
        Y_UNUSED(workloadType);
    }

    void TFulltextWorkloadParams::Init() {
        if (RunWorkloadType != static_cast<int>(EFulltextWorkloadType::Select)) {
            return;
        }

        const TString tablePath = GetFullTableName(TableName.c_str());
        auto session = TableClient->GetSession().ExtractValueSync().GetSession();
        auto describeResult = session.DescribeTable(tablePath).GetValueSync();
        Y_ABORT_UNLESS(describeResult.IsSuccess(), "DescribeTable failed: %s", describeResult.GetIssues().ToString().c_str());

        for (const auto& index : describeResult.GetTableDescription().GetIndexDescriptions()) {
            if (index.GetIndexName() == IndexName) {
                IndexIsRelevance = (index.GetIndexType() == NYdb::NTable::EIndexType::GlobalFulltextRelevance);
                break;
            }
        }
    }

} // namespace NYdbWorkload
