#include "vector.h"

#include <util/datetime/base.h>
#include <util/generic/serialized_enum.h>

#include <format>
#include <string>
#include <random>

namespace NYdbWorkload {

TVectorWorkloadGenerator::TVectorWorkloadGenerator(const TVectorWorkloadParams* params)
    : TBase(params)
    , Rd()
    , Gen(Rd())
    , RandExpDistrib(1.6)
    , VectorValueGenerator(0.0, 1.0)
    , PregeneratedIndexGenerator(0, params->VectorSelectCount - 1)
{
    Gen.seed(Now().MicroSeconds());

    PregenerateSelectEmbeddings();
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
    TQueryInfoList res;
    return res;
}

TVector<std::string> TVectorWorkloadGenerator::GetCleanPaths() const {
    return {"vector"};
}


TQueryInfoList TVectorWorkloadGenerator::GetWorkload(int type) {
    switch (static_cast<EType>(type)) {
        case EType::SelectScan:
            return SelectScan();
        case EType::SelectIndex:
            return SelectIndex();
        case EType::SelectPrefixIndex:
            return SelectPrefixIndex();
        default:
            return TQueryInfoList();
    }
}

TVector<IWorkloadQueryGenerator::TWorkloadType> TVectorWorkloadGenerator::GetSupportedWorkloadTypes() const {
    TVector<TWorkloadType> result;
    result.emplace_back(static_cast<int>(EType::SelectScan), "select-scan", "Select top-K vector using brute force scan");
    result.emplace_back(static_cast<int>(EType::SelectIndex), "select-index", "Select top-K vector using index");
    result.emplace_back(static_cast<int>(EType::SelectPrefixIndex), "select-prefix-index", "Select top-K vector using prefix index");
    return result;
}

TQueryInfoList TVectorWorkloadGenerator::SelectScan() {
    return TQueryInfoList(1, SelectScanImpl());
}

TQueryInfoList TVectorWorkloadGenerator::SelectIndex() {
    return TQueryInfoList(1, SelectIndexImpl());
}

TQueryInfoList TVectorWorkloadGenerator::SelectPrefixIndex() {
    return TQueryInfoList(1, SelectPrefixIndexImpl());
}

TQueryInfo TVectorWorkloadGenerator::SelectScanImpl() {
    std::string query = std::format(R"_(--!syntax_v1
        DECLARE $EmbeddingList as List<Float>;
        $EmbeddingString = Knn::ToBinaryStringFloat($EmbeddingList);

        SELECT id, Knn::CosineDistance(embedding, $EmbeddingString) AS similarity
        FROM {0}
        WHERE id < $MaxId
        ORDER BY similarity
        LIMIT $K;
    )_", Params.TableName.c_str());

    NYdb::TParamsBuilder paramsBuilder;
    
    auto& paramsList = paramsBuilder.AddParam("$EmbeddingList").BeginList();
    for(float val: SelectEmbeddings[PregeneratedIndexGenerator(Gen)])
        paramsList.AddListItem().Float(val);
    paramsList.EndList().Build();

    paramsBuilder
        .AddParam("$MaxId").Uint64(Params.MaxId).Build()
        .AddParam("$K").Uint64(Params.Limit).Build();

    return TQueryInfo(query, paramsBuilder.Build());
}

TQueryInfo TVectorWorkloadGenerator::SelectIndexImpl() {
    std::string query = std::format(R"_(--!syntax_v1
        DECLARE $EmbeddingList as List<Float>;
        $EmbeddingString = Knn::ToBinaryStringFloat($EmbeddingList);

        SELECT id, Knn::CosineDistance(embedding, $EmbeddingString) AS similarity
        FROM {0}
        VIEW {1}
        ORDER BY similarity
        LIMIT $K;
    )_", Params.TableName.c_str(), Params.IndexName.c_str());

    NYdb::TParamsBuilder paramsBuilder;
    
    auto& paramsList = paramsBuilder.AddParam("$EmbeddingList").BeginList();
    for(float val: SelectEmbeddings[PregeneratedIndexGenerator(Gen)])
        paramsList.AddListItem().Float(val);
    paramsList.EndList().Build();

    paramsBuilder
        .AddParam("$MaxId").Uint64(Params.MaxId).Build()
        .AddParam("$K").Uint64(Params.Limit).Build();

    return TQueryInfo(query, paramsBuilder.Build());
}

TQueryInfo TVectorWorkloadGenerator::SelectPrefixIndexImpl() {
    std::string query = std::format(R"_(--!syntax_v1
        DECLARE $EmbeddingList as List<Float>;
        $EmbeddingString = Knn::ToBinaryStringFloat($EmbeddingList);

        SELECT id, Knn::CosineDistance(embedding, $EmbeddingString) AS similarity
        FROM {0}
        VIEW {1}
        WHERE intent = 'factoid'
        ORDER BY Knn::CosineDistance(embedding, $EmbeddingString)
        LIMIT $K;
    )_", Params.TableName.c_str(), Params.IndexName.c_str());

    NYdb::TParamsBuilder paramsBuilder;
    
    auto& paramsList = paramsBuilder.AddParam("$EmbeddingList").BeginList();
    for(float val: SelectEmbeddings[PregeneratedIndexGenerator(Gen)])
        paramsList.AddListItem().Float(val);
    paramsList.EndList().Build();

    paramsBuilder
        .AddParam("$MaxId").Uint64(Params.MaxId).Build()
        .AddParam("$K").Uint64(Params.Limit).Build();

    return TQueryInfo(query, paramsBuilder.Build());
}

void TVectorWorkloadGenerator::PregenerateSelectEmbeddings() {
    SelectEmbeddings.reserve(Params.VectorSelectCount);
    for (size_t i = 0; i < Params.VectorSelectCount; ++i) {
        std::vector<float> vector(Params.VectorDimension);
        for (size_t j = 0; j < Params.VectorDimension; ++j) {
            vector[j] = VectorValueGenerator(Gen);
        }
        SelectEmbeddings.emplace_back(vector);
    }
}

void TVectorWorkloadParams::ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) {
    auto addCommonParam = [&]() {
        opts.AddLongOption(0, "table", "Table name.")
            .Required().StoreResult(&TableName);
        opts.AddLongOption(0, "index", "Index name.")
            .Required().StoreResult(&IndexName);
    };

    auto addInitParam = [&]() {
        opts.AddLongOption(0, "vector-count", "Number of vectors to init the table.")
            .Required().StoreResult(&VectorInitCount);
        opts.AddLongOption(0, "distance", "Distance/similarity function")
            .Required().StoreResult(&Distance);
        opts.AddLongOption(0, "vector-type", "Type of vectors")
            .Required().StoreResult(&VectorType);
        opts.AddLongOption(0, "vector-dimension", "Vector dimension.")
            .Required().StoreResult(&VectorDimension);
        opts.AddLongOption(0, "kmeans-tree-levels", "Number of levels in the kmeans tree")
            .Required().StoreResult(&KmeansTreeLevels);
        opts.AddLongOption(0, "kmeans-tree-clusters", "Number of cluster in kmeans")
            .Required().StoreResult(&KmeansTreeClusters);
    };

    auto addSelectParam = [&]() {
        opts.AddLongOption(0, "top-k", "Number of top vector to return.")
            .DefaultValue(5).StoreResult(&Limit);
        opts.AddLongOption(0, "vector-dimension", "Vector dimension.")
            .Required().StoreResult(&VectorDimension);
        opts.AddLongOption(0, "vector-count", "Number of pregenerated vectors to search as targets.")
            .DefaultValue(1000).StoreResult(&VectorSelectCount);
    };

    switch (commandType) {
    case TWorkloadParams::ECommandType::Init:
        addCommonParam();
        addInitParam();
        break;
    case TWorkloadParams::ECommandType::Run:
        addCommonParam();
        switch (static_cast<TVectorWorkloadGenerator::EType>(workloadType)) {
        case TVectorWorkloadGenerator::EType::SelectScan:
            addSelectParam();
            opts.AddLongOption(0, "max-id", "Maximum id of vector to return. Only vectors lower than 'max-id' will be scanned.")
                .DefaultValue(10).StoreResult(&MaxId);
            break;
        case TVectorWorkloadGenerator::EType::SelectPrefixIndex:
            addSelectParam();
            break;
        case TVectorWorkloadGenerator::EType::SelectIndex:
            addSelectParam();
            break;
        }
        break;
    default:
        break;
    }
}

THolder<IWorkloadQueryGenerator> TVectorWorkloadParams::CreateGenerator() const {
    return MakeHolder<TVectorWorkloadGenerator>(this);
}

TString TVectorWorkloadParams::GetWorkloadName() const {
    return "vector";
}

}
