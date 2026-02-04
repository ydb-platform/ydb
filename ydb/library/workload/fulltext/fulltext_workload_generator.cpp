#include "fulltext_workload_generator.h"
#include "fulltext_workload_params.h"

#include <util/random/random.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/generic/vector.h>

namespace NYdbWorkload {

TFulltextWorkloadGenerator::TFulltextWorkloadGenerator(const TFulltextWorkloadParams* params)
    : TBase(params)
{}

void TFulltextWorkloadGenerator::Init() {
}

std::string TFulltextWorkloadGenerator::GetDDLQueries() const {
    return TStringBuilder() << "CREATE TABLE `" << Params.DbPath << "/" << Params.TableName << "` ("
        << "id Uint64, "
        << "text String, "
        << "PRIMARY KEY (id)"
        << ") WITH ("
        << "AUTO_PARTITIONING_BY_LOAD = " << (Params.AutoPartitioningByLoad ? "ENABLED" : "DISABLED") << ", "
        << "AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = " << Params.MinPartitions
        << ");";
}

TQueryInfoList TFulltextWorkloadGenerator::GetInitialData() {
    return {};
}

TVector<std::string> TFulltextWorkloadGenerator::GetCleanPaths() const {
    return {Params.TableName};
}

TQueryInfoList TFulltextWorkloadGenerator::GetWorkload(int type) {
    switch (type) {
        case 0:
            return Select();
        default:
            return {};
    }
}

TVector<IWorkloadQueryGenerator::TWorkloadType> TFulltextWorkloadGenerator::GetSupportedWorkloadTypes() const {
    TVector<TWorkloadType> result;
    result.emplace_back(0, "select", "Select");
    return result;

}

TQueryInfoList TFulltextWorkloadGenerator::Upsert() {
    TQueryInfo queryInfo;
    queryInfo.Query = TStringBuilder() << "UPSERT INTO `" << Params.TableName << "` (id, text) VALUES ($id, $text);";
    queryInfo.Params = NYdb::TParamsBuilder()
        .AddParam("$id").Uint64(RandomNumber<ui64>()).Build()
        .AddParam("$text").String("Random text " + ToString(RandomNumber<ui64>())).Build()
        .Build();
    return {queryInfo};
}

TQueryInfoList TFulltextWorkloadGenerator::Select() {
    TQueryInfo queryInfo;
    queryInfo.Query = TStringBuilder() << "SELECT * FROM `" << Params.TableName << "` WHERE text LIKE $text;";
    queryInfo.Params = NYdb::TParamsBuilder()
        .AddParam("$text").String("%Random%").Build()
        .Build();
    return {queryInfo};
}

} // namespace NYdbWorkload
