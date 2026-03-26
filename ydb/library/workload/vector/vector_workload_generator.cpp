#include "vector_enums.h"
#include "vector_sql.h"
#include "vector_workload_generator.h"
#include "vector_workload_params.h"

#include <ydb/library/yql/udfs/common/knn/knn-serializer-shared.h>

#include <util/datetime/base.h>
#include <util/generic/serialized_enum.h>
#include <util/random/random.h>

#include <format>
#include <string>

#include <algorithm>
#include <random>


namespace NYdbWorkload {

TVectorWorkloadGenerator::TVectorWorkloadGenerator(const TVectorWorkloadParams* params)
    : TBase(params)
{
}

void TVectorWorkloadGenerator::Init() {
    if (Params.RunWorkloadType == static_cast<int>(EWorkloadRunType::Upsert)) {
        return;
    }

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
                id Uint64 NOT NULL,
                embedding String,
                PRIMARY KEY(id))
            WITH (
                AUTO_PARTITIONING_BY_SIZE = ENABLED,
                AUTO_PARTITIONING_BY_LOAD = {2},
                AUTO_PARTITIONING_PARTITION_SIZE_MB = {3},
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = {4}
            )
        )_",
        Params.DbPath.c_str(),
        Params.TableOpts.Name.c_str(),
        Params.TablePartitioningOpts.AutoPartitioningByLoad ? "ENABLED" : "DISABLED",
        Params.TablePartitioningOpts.PartitionSize,
        Params.TablePartitioningOpts.MinPartitions
    );
}

TQueryInfoList TVectorWorkloadGenerator::GetInitialData() {
    return {};
}

TVector<std::string> TVectorWorkloadGenerator::GetCleanPaths() const {
    return {Params.TableOpts.Name};
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
    static thread_local std::mt19937_64 rng(std::random_device{}());
    static thread_local std::uniform_real_distribution<float> dist(-1.0f, 1.0f);
    static thread_local std::uniform_int_distribution<ui64> idDist;

    const size_t dimension = Params.VectorOpts.VectorDimension;
    const size_t batchSize = Params.UpsertBulkSize;

    TStringBuilder query;
    query << "--!syntax_v1\n";
    query << "DECLARE $rows AS List<Struct<id:Uint64, embedding:String";
    if (Params.UpsertPrefixed) {
        query << ", prefix:Uint64";
    }
    query << ">>;\n";
    query << "UPSERT INTO `" << Params.TableOpts.Name << "` SELECT * FROM AS_TABLE($rows);\n";

    NYdb::TParamsBuilder paramsBuilder;
    auto& listBuilder = paramsBuilder.AddParam("$rows").BeginList();

    for (size_t i = 0; i < batchSize; ++i) {
        TStringBuilder embeddingBuffer;
        NKnnVectorSerialization::TSerializer<float> serializer(&embeddingBuffer.Out);
        for (size_t j = 0; j < dimension; ++j) {
            serializer.HandleElement(dist(rng));
        }
        serializer.Finish();

        auto& item = listBuilder.AddListItem().BeginStruct();
        item.AddMember("id").Uint64(idDist(rng));
        item.AddMember("embedding").String(embeddingBuffer);
        if (Params.UpsertPrefixed) {
            item.AddMember("prefix").Uint64(idDist(rng) % Params.UpsertPrefixCount);
        }
        item.EndStruct();
    }
    listBuilder.EndList().Build();

    return TQueryInfoList(1, TQueryInfo(query, paramsBuilder.Build()));
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
