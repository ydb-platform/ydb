#include "vector_enums.h"
#include "vector_sql.h"
#include "vector_workload_generator.h"
#include "vector_workload_params.h"

#include <util/datetime/base.h>
#include <util/generic/serialized_enum.h>

#include <format>
#include <string>

#include <algorithm>


namespace NYdbWorkload {

TVectorWorkloadGenerator::TVectorWorkloadGenerator(const TVectorWorkloadParams* params)
    : TBase(params)
{
}

void TVectorWorkloadGenerator::Init() {
    VectorSampler = MakeHolder<TVectorSampler>(Params);
    if (Params.QueryTableName.empty()) {
        VectorSampler->SampleExistingVectors();
    } else {
        VectorSampler->SelectPredefinedVectors();
    }

    if (Params.Recall) {
        TVectorRecallEvaluator vectorRecallEvaluator(Params);
        vectorRecallEvaluator.MeasureRecall(*VectorSampler);
    }
}

std::string TVectorWorkloadGenerator::GetDDLQueries() const {
    return std::format(R"_(--!syntax_v1
        CREATE TABLE `{0}/{1}`(
            id Uint64,
            embedding Utf8,
            PRIMARY KEY(id))
        WITH (
            AUTO_PARTITIONING_BY_SIZE = ENABLED,
            AUTO_PARTITIONING_PARTITION_SIZE_MB = 500,
            AUTO_PARTITIONING_BY_LOAD = ENABLED
        )
    )_", Params.DbPath.c_str(), Params.TableName.c_str());
}

TQueryInfoList TVectorWorkloadGenerator::GetInitialData() {
    return {};
}

TVector<std::string> TVectorWorkloadGenerator::GetCleanPaths() const {
    return {"vector"};
}

TQueryInfoList TVectorWorkloadGenerator::GetWorkload(int type) {
    switch (static_cast<EWorkloadRunType>(type)) {
        case EWorkloadRunType::Upsert:
            return Upsert();
        case EWorkloadRunType::Select:
            return Select();
        default:
            return TQueryInfoList();
    }
}

TVector<IWorkloadQueryGenerator::TWorkloadType> TVectorWorkloadGenerator::GetSupportedWorkloadTypes() const {
    TVector<TWorkloadType> result;
    result.emplace_back(static_cast<int>(EWorkloadRunType::Upsert), "upsert", "Upsert vector rows in the table");
    result.emplace_back(static_cast<int>(EWorkloadRunType::Select), "select", "Retrieve top-K vectors");
    return result;
}

TQueryInfoList TVectorWorkloadGenerator::Upsert() {
    // Not implemented yet
    return {};
}

TQueryInfoList TVectorWorkloadGenerator::Select() {
    CurrentIndex = (CurrentIndex + 1) % VectorSampler->GetTargetCount();

    // Create the query string
    std::string query = MakeSelect(Params, Params.IndexName);

    // Get the embedding for the specified target
    const auto& targetEmbedding = VectorSampler->GetTargetEmbedding(CurrentIndex);

    // Get the prefix value if needed
    std::optional<NYdb::TValue> prefixValue;
    if (Params.PrefixColumn.has_value()) {
        prefixValue = VectorSampler->GetPrefixValue(CurrentIndex);
    }

    NYdb::TParams params = MakeSelectParams(targetEmbedding, prefixValue, Params.Limit);

    // Create the query info with a callback that captures the target index
    TQueryInfo queryInfo(query, std::move(params));
    queryInfo.UseStaleRO = Params.StaleRO;

    return TQueryInfoList(1, queryInfo);
}

} // namespace NYdbWorkload
