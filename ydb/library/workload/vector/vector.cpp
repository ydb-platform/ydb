#include "vector.h"

#include <util/datetime/base.h>
#include <util/generic/serialized_enum.h>

#include <format>
#include <string>
#include <random>

namespace NYdbWorkload {

TVectorGenerator::TVectorGenerator(size_t vectorDimension, size_t vectorCount) 
    : Rd()
    , Gen(Rd())
    , VectorValueGenerator(0.0, 1.0)
    , PregeneratedIndexGenerator(0, vectorCount - 1)
{
    Gen.seed(Now().MicroSeconds());

    PregenerateSelectEmbeddings(vectorDimension, vectorCount);
}

void TVectorGenerator::PregenerateSelectEmbeddings(size_t vectorDimension, size_t vectorCount) {
    SelectEmbeddings.reserve(vectorCount);
    for (size_t i = 0; i < vectorCount; ++i) {
        std::vector<float> vector(vectorDimension);
        for (size_t j = 0; j < vectorDimension; ++j) {
            vector[j] = VectorValueGenerator(Gen);
        }
        SelectEmbeddings.emplace_back(vector);
    }
}

const std::vector<float>& TVectorGenerator::GetRandomSelectEmbedding() {
    return SelectEmbeddings[PregeneratedIndexGenerator(Gen)];
}

TVectorWorkloadGenerator::TVectorWorkloadGenerator(const TVectorWorkloadParams* params)
    : TBase(params)
{
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
        case EType::Upsert:
            return Upsert();
        case EType::Select:
            return Select();
        default:
            return TQueryInfoList();
    }
}

TVector<IWorkloadQueryGenerator::TWorkloadType> TVectorWorkloadGenerator::GetSupportedWorkloadTypes() const {
    TVector<TWorkloadType> result;
    result.emplace_back(static_cast<int>(EType::Upsert), "upsert", "Upsert vector rows in the table");
    result.emplace_back(static_cast<int>(EType::Select), "select", "Retrieve top-K vectors");
    return result;
}

TQueryInfoList TVectorWorkloadGenerator::Upsert() {
    // Not implemented yet
    return {};
}

TQueryInfoList TVectorWorkloadGenerator::Select() {
    // TODO Detect index existance
    return TQueryInfoList(1, SelectIndexImpl());
}

TQueryInfo TVectorWorkloadGenerator::SelectImpl(const std::string& query) {
    NYdb::TParamsBuilder paramsBuilder;
    
    auto& paramsList = paramsBuilder.AddParam("$EmbeddingList").BeginList();
    for(float val: Params.GetRandomSelectEmbedding())
        paramsList.AddListItem().Float(val);
    paramsList.EndList().Build();

    paramsBuilder
        .AddParam("$K").Uint64(Params.TopK).Build();

    return TQueryInfo(query, paramsBuilder.Build());
}

TQueryInfo TVectorWorkloadGenerator::SelectScanImpl() {
    std::string query = std::format(R"_(--!syntax_v1
        DECLARE $EmbeddingList as List<Float>;
        $EmbeddingString = Knn::ToBinaryStringFloat($EmbeddingList);

        SELECT id, Knn::CosineDistance(embedding, $EmbeddingString) AS similarity
        FROM {0}
        ORDER BY similarity
        LIMIT $K;
    )_", Params.TableName.c_str());

    return SelectImpl(query);
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

    return SelectImpl(query);
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

    return SelectImpl(query);
}

void TVectorWorkloadParams::ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) {
    auto addCommonParam = [&]() {
        opts.AddLongOption(0, "table", "Table name.")
            .DefaultValue("vector_index_workload").StoreResult(&TableName);
        opts.AddLongOption(0, "index", "Index name.")
            .DefaultValue("index").StoreResult(&IndexName);
    };

    auto addInitParam = [&]() {
        opts.AddLongOption(0, "rows", "Number of vectors to init the table.")
            .Required().StoreResult(&VectorInitCount);
        opts.AddLongOption(0, "distance", "Distance/similarity function")
            .Required().StoreResult(&Distance);
        opts.AddLongOption(0, "vector-type", "Type of vectors")
            .Required().StoreResult(&VectorType);
        opts.AddLongOption(0, "dimension", "Vector dimension.")
            .Required().StoreResult(&VectorDimension);
        opts.AddLongOption(0, "kmeans-tree-levels", "Number of levels in the kmeans tree")
            .Required().StoreResult(&KmeansTreeLevels);
        opts.AddLongOption(0, "kmeans-tree-clusters", "Number of cluster in kmeans")
            .Required().StoreResult(&KmeansTreeClusters);
        opts.AddLongOption(0, "vector-dimension", "Vector dimension.")
            .Required().StoreResult(&VectorDimension);
    };

    auto addUpsertParam = [&]() {
    };

    auto addSelectParam = [&]() {
        opts.AddLongOption(0, "targets", "Number of vectors to search as targets.")
            .DefaultValue(1000).StoreResult(&Targets);
        opts.AddLongOption(0, "top-k", "Number of top vector to return.")
            .DefaultValue(5).StoreResult(&TopK);
        opts.AddLongOption(0, "kmeans-tree-clusters", " Number of top clusters during search.")
            .DefaultValue(1).StoreResult(&KmeansTreeSearchClusters);    
    };

    switch (commandType) {
    case TWorkloadParams::ECommandType::Init:
        addCommonParam();
        addInitParam();
        break;
    case TWorkloadParams::ECommandType::Run:
        addCommonParam();
        switch (static_cast<TVectorWorkloadGenerator::EType>(workloadType)) {
        case TVectorWorkloadGenerator::EType::Upsert:
            addUpsertParam();
            break;
        case TVectorWorkloadGenerator::EType::Select:
            addSelectParam();
            break;
        }
        break;
    default:
        break;
    }
}

size_t TVectorWorkloadParams::GetVectorDimension() {
    return 768;
//     std::string query = std::format(R"_(--!syntax_v1
//         SELECT ListLength(Knn::FloatFromBinaryString(embedding)) FROM {0} LIMIT 1;
//     )_", Params.TableName.c_str());


//     auto driver = NConsoleClient::TYdbCommand::CreateDriver(ConnectionConfig);
//     auto client = NQuery::TQueryClient(driver);

//     std::optional<TResultSet> resultSet;
//     NYdb::NStatusHelpers::ThrowOnError(client.RetryQuerySync([&resultSet, &params](TSession session) {
//         auto result = session.ExecuteQuery(
//             QUERY,
//             TTxControl::BeginTx(STALE_RO ? TTxSettings::StaleRO() : TTxSettings::TTxSettings::SerializableRW()).CommitTx(),
//             params.Build()).GetValueSync();
        
//         if (!result.IsSuccess()) {
//             return result;
//         }
//         resultSet = result.GetResultSet(0);
//         return result;
//     })); 

//     return 0;
}

void TVectorWorkloadParams::Validate(const ECommandType commandType, int workloadType) {
    switch (commandType) {
        case TWorkloadParams::ECommandType::Init:
            break;
        case TWorkloadParams::ECommandType::Run:
            switch (static_cast<TVectorWorkloadGenerator::EType>(workloadType)) {
                case TVectorWorkloadGenerator::EType::Upsert:
                    break;
                case TVectorWorkloadGenerator::EType::Select:
                    VectorDimension = GetVectorDimension();
                    VectorGenerator = MakeHolder<TVectorGenerator>(VectorDimension, Targets);
                    break;
            }
            break;
        case TWorkloadParams::ECommandType::Clean:
            break;
        case TWorkloadParams::ECommandType::Root:
            break;
        case TWorkloadParams::ECommandType::Import:
          break;
    }
    return;
}

const std::vector<float>& TVectorWorkloadParams::GetRandomSelectEmbedding() const {
    return VectorGenerator->GetRandomSelectEmbedding();
}

THolder<IWorkloadQueryGenerator> TVectorWorkloadParams::CreateGenerator() const {
    return MakeHolder<TVectorWorkloadGenerator>(this);
}

TString TVectorWorkloadParams::GetWorkloadName() const {
    return "vector";
}

}
