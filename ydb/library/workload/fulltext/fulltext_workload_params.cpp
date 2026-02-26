#include "fulltext_workload_params.h"
#include "fulltext_workload_generator.h"
#include "fulltext_data_generator.h"

namespace NYdbWorkload {

    void TFulltextWorkloadParams::ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) {
        Y_UNUSED(workloadType);

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
                opts.AddLongOption("index-name", "Fulltext index name.")
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
                opts.AddLongOption("index-name", "Fulltext index name.")
                    .DefaultValue(IndexName)
                    .StoreResult(&IndexName);
                opts.AddLongOption("query-table", "Name of the table that contains queries to use for select queries. The table must have a 'query' column.")
                    .Required()
                    .StoreResult(&QueryTable);
                opts.AddLongOption("limit", "Limit rows in result set.")
                    .DefaultValue(Limit)
                    .StoreResult(&Limit);
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
            std::make_shared<TFulltextWorkloadDataInitializer>(*this),
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
        // Initialization logic if needed
    }

} // namespace NYdbWorkload
