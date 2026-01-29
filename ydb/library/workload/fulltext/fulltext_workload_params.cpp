#include "fulltext_workload_params.h"
#include "fulltext_workload_generator.h"

#include <util/string/builder.h>

namespace NYdbWorkload {

void TFulltextWorkloadParams::ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) {
    switch (commandType) {
    case TWorkloadParams::ECommandType::Init:
        opts.AddLongOption("table", "Table name")
            .DefaultValue(TableName).StoreResult(&TableName);
        opts.AddLongOption("min-partitions", "Minimum number of partitions")
            .DefaultValue(MinPartitions).StoreResult(&MinPartitions);
        opts.AddLongOption("partition-size", "Partition size in MB")
            .DefaultValue(PartitionSizeMb).StoreResult(&PartitionSizeMb);
        opts.AddLongOption("auto-partition", "Enable auto partitioning by load")
            .DefaultValue(AutoPartitioningByLoad).StoreResult(&AutoPartitioningByLoad);
        break;
    case TWorkloadParams::ECommandType::Import:
    case TWorkloadParams::ECommandType::Run:
        opts.AddLongOption("table", "Table name")
            .DefaultValue(TableName).StoreResult(&TableName);
        break;
    default:
        break;
    }
}

THolder<IWorkloadQueryGenerator> TFulltextWorkloadParams::CreateGenerator() const {
    return MakeHolder<TFulltextWorkloadGenerator>(this);
}

TWorkloadDataInitializer::TList TFulltextWorkloadParams::CreateDataInitializers() const {
    return {};
}

TString TFulltextWorkloadParams::GetWorkloadName() const {
    return "fulltext";
}

void TFulltextWorkloadParams::Validate(const ECommandType commandType, int workloadType) {
    // Validation logic if needed
}

void TFulltextWorkloadParams::Init() {
    // Initialization logic if needed
}

} // namespace NYdbWorkload
