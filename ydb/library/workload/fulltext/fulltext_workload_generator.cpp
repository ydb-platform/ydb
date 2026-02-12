#include "fulltext_workload_generator.h"
#include "fulltext_workload_params.h"

#include <util/random/random.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/generic/vector.h>

#include <format>

namespace NYdbWorkload {

TFulltextWorkloadGenerator::TFulltextWorkloadGenerator(const TFulltextWorkloadParams* params)
    : TBase(params)
{}

std::string TFulltextWorkloadGenerator::GetDDLQueries() const {
    return std::format(
        R"sql(
            CREATE TABLE `{}/{}` (
                `id` Uint64,
                `text` String,
                PRIMARY KEY (`id`)
            ) WITH (
                AUTO_PARTITIONING_BY_LOAD = {},
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = {}
            );
        )sql",
        Params.DbPath.c_str(),
        Params.TableName.c_str(),
        Params.AutoPartitioningByLoad ? "ENABLED" : "DISABLED",
        Params.MinPartitions
    );
}

TQueryInfoList TFulltextWorkloadGenerator::GetInitialData() {
    return {};
}

TVector<std::string> TFulltextWorkloadGenerator::GetCleanPaths() const {
    return {Params.TableName};
}

TQueryInfoList TFulltextWorkloadGenerator::GetWorkload(int type) {
    Y_UNUSED(type);

    return {};
}

TVector<IWorkloadQueryGenerator::TWorkloadType> TFulltextWorkloadGenerator::GetSupportedWorkloadTypes() const {
    return {};
}

} // namespace NYdbWorkload
