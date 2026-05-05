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
    TStringBuilder extraColumns;
    if (Params.KmeansTreePrefixed) {
        extraColumns << "prefix Uint64 NOT NULL,\n";
    }
    return std::format(R"_(--!syntax_v1
            CREATE TABLE `{0}/{1}`(
                id Uint64 NOT NULL,
                embedding String,
                {2}
                PRIMARY KEY(id))
            WITH (
                AUTO_PARTITIONING_BY_SIZE = ENABLED,
                AUTO_PARTITIONING_BY_LOAD = {3},
                AUTO_PARTITIONING_PARTITION_SIZE_MB = {4},
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = {5}
            )
        )_",
        Params.DbPath.c_str(),
        Params.TableOpts.Name.c_str(),
        extraColumns.c_str(),
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

template<typename T>
static TString GenerateEmbedding(size_t dimension, std::mt19937_64& rng, std::uniform_real_distribution<float>& dist) {
    TStringBuilder buffer;
    NKnnVectorSerialization::TSerializer<T> serializer(&buffer.Out);
    for (size_t j = 0; j < dimension; ++j) {
        if constexpr (std::is_same<T, float>::value) {
            serializer.HandleElement(dist(rng));
        } else if constexpr (std::is_same<T, uint8_t>::value) {
            serializer.HandleElement(static_cast<uint8_t>(dist(rng) * (UINT8_MAX + 1)));
        } else if constexpr (std::is_same<T, int8_t>::value) {
            serializer.HandleElement(static_cast<int8_t>(dist(rng) * (INT8_MAX - INT8_MIN + 1) + INT8_MIN));
        } else if constexpr (std::is_same<T, bool>::value) {
            serializer.HandleElement(dist(rng) >= 0.5f);
        } else {
            static_assert(false, "Unsupported type");
        }
    }
    serializer.Finish();
    return buffer;
}

TQueryInfoList TVectorWorkloadGenerator::Upsert() {
    static thread_local std::mt19937_64 rng(std::random_device{}());
    static thread_local std::uniform_real_distribution<float> dist(-1.0f, 1.0f);
    static thread_local std::uniform_int_distribution<ui64> idDist;

    const size_t dimension = Params.VectorOpts.VectorDimension;
    const size_t batchSize = Params.UpsertBulkSize;
    const TString& vectorType = Params.VectorOpts.VectorType;

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
        TString embeddingBuffer;
        if (vectorType == "uint8") {
            embeddingBuffer = GenerateEmbedding<uint8_t>(dimension, rng, dist);
        } else if (vectorType == "int8") {
            embeddingBuffer = GenerateEmbedding<int8_t>(dimension, rng, dist);
        } else if (vectorType == "bit") {
            embeddingBuffer = GenerateEmbedding<bool>(dimension, rng, dist);
        } else {
            embeddingBuffer = GenerateEmbedding<float>(dimension, rng, dist);
        }

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
