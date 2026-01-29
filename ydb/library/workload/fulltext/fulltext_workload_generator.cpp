#include "fulltext_workload_generator.h"
#include "fulltext_workload_params.h"

#include <util/random/random.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

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
        case 0: return Upsert();
        case 1: return Select();
        default: return {};
    }
}

TVector<IWorkloadQueryGenerator::TWorkloadType> TFulltextWorkloadGenerator::GetSupportedWorkloadTypes() const {
    return {
        {0, "upsert", "Upsert random text"},
        {1, "select", "Select random text"}
    };
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
