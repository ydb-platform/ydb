#include "json_workload_params.h"
#include "json_workload_generator.h"
#include "json_data_generator.h"

namespace NYdbWorkload {

TString TJsonWorkloadParams::GetJsonTypeName() const {
    return Binary ? "JsonDocument" : "Json";
}

void TJsonWorkloadParams::ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) {
    switch (commandType) {
        case ECommandType::Root:
            opts.AddLongOption("path", "Path to workload table.")
                .DefaultValue(TableName)
                .StoreResult(&TableName);
            opts.AddLongOption("binary", "Use JsonDocument column type instead of Json.")
                .NoArgument()
                .SetFlag(&Binary);
            break;
        case ECommandType::Init:
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
        case ECommandType::Import:
            if (!ImportOptsRegistered) {
                ImportOptsRegistered = true;
                opts.AddLongOption("index", "JSON index name.")
                    .DefaultValue(IndexName)
                    .StoreResult(&IndexName);
            }
            break;
        case ECommandType::Run:
            if (workloadType < 0) {
                opts.AddLongOption("index", "JSON index name.")
                    .DefaultValue(IndexName)
                    .StoreResult(&IndexName);
                break;
            }
            RunWorkloadType = workloadType;
            switch (static_cast<EJsonWorkloadType>(workloadType)) {
                case EJsonWorkloadType::Select:
                    opts.AddLongOption("strict", "Use strict JSON path mode in predicates.")
                        .NoArgument()
                        .SetFlag(&IsStrict);
                    opts.AddLongOption("rows", "Corpus size used to build predicates.")
                        .DefaultValue(RowCount)
                        .StoreResult(&RowCount);
                    opts.AddLongOption("seed", "Random seed for predicate generation.")
                        .DefaultValue(Seed)
                        .StoreResult(&Seed);
                    opts.AddLongOption("max-predicates", "Maximum number of predicates to prepare.")
                        .DefaultValue(MaxPredicates)
                        .StoreResult(&MaxPredicates);
                    opts.AddLongOption("limit", "Limit rows in result set.")
                        .DefaultValue(Limit)
                        .StoreResult(&Limit);
                    break;
                case EJsonWorkloadType::Upsert:
                    opts.AddLongOption("bulk-size", "Number of rows in an upsert batch.")
                        .DefaultValue(UpsertBulkSize)
                        .StoreResult(&UpsertBulkSize);
                    opts.AddLongOption("seed", "Random seed for JSON value generation.")
                        .DefaultValue(Seed)
                        .StoreResult(&Seed);
                    break;
            }
            break;
        default:
            break;
    }
}

THolder<IWorkloadQueryGenerator> TJsonWorkloadParams::CreateGenerator() const {
    return MakeHolder<TJsonWorkloadGenerator>(this);
}

TWorkloadDataInitializer::TList TJsonWorkloadParams::CreateDataInitializers() const {
    return {std::make_shared<TJsonDataInitializer>(*this)};
}

TString TJsonWorkloadParams::GetWorkloadName() const {
    return "json";
}

void TJsonWorkloadParams::Validate(const ECommandType commandType, int workloadType) {
    Y_UNUSED(commandType);
    Y_UNUSED(workloadType);
}

} // namespace NYdbWorkload
