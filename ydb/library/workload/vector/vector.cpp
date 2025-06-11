#include "vector.h"

#include <util/datetime/base.h>
#include <util/generic/serialized_enum.h>

#include <format>
#include <string>
#include <random>

namespace NYdbWorkload {

std::string MakeSelect(const char* tableName, const char* indexName) {
    return std::format(R"_(--!syntax_v1
        DECLARE $EmbeddingList as List<Float>;
        $EmbeddingString = Knn::ToBinaryStringFloat($EmbeddingList);

        pragma ydb.KMeansTreeSearchTopSize = "10";
        SELECT id
        FROM {0}
        {1}
        ORDER BY Knn::CosineDistance(embedding, $EmbeddingString)
        LIMIT $K;
    )_", 
        tableName, 
        indexName ? std::format("VIEW {0}", indexName) : ""
    );
}

NYdb::TParams MakeSelectParams(const TFloatVector& target, ui64 topK) {
    NYdb::TParamsBuilder paramsBuilder;
    
    auto& paramsList = paramsBuilder.AddParam("$EmbeddingList").BeginList();
    for(float val: target)
        paramsList.AddListItem().Float(val);
    paramsList.EndList().Build();

    paramsBuilder.AddParam("$K").Uint64(topK).Build();

    return paramsBuilder.Build();
}

TVectorRecallEvaluator::TVectorRecallEvaluator(size_t vectorDimension, size_t vectorCount) 
    : Rd()
    , Gen(Rd())
    , VectorValueGenerator(0.0, 1.0)
    , PregeneratedIndexGenerator(0, vectorCount - 1)
{
    Gen.seed(Now().MicroSeconds());

    PregenerateSelectEmbeddings(vectorDimension, vectorCount);
}

TVectorRecallEvaluator::~TVectorRecallEvaluator() {
    if (ProcessedTargets)
        Cout << "Average recall: " << GetAverageRecall() << Endl;
}

void TVectorRecallEvaluator::PregenerateSelectEmbeddings(size_t vectorDimension, size_t targetCount) {
    SelectTargets.reserve(targetCount);
    for (size_t i = 0; i < targetCount; ++i) {
        TSelectTarget target {
            .Target = TFloatVector(vectorDimension),
            .Etalons = {}
        };
        target.Target.resize(vectorDimension); 
        for (size_t j = 0; j < vectorDimension; ++j) {
            target.Target[j] = VectorValueGenerator(Gen);
        }
        SelectTargets.emplace_back(target);
    }
}

const TFloatVector& TVectorRecallEvaluator::GetRandomSelectEmbedding() {
    return SelectTargets[PregeneratedIndexGenerator(Gen)].Target;
}

void TVectorRecallEvaluator::FillEtalons(const char* tableName, size_t topK, NYdb::NQuery::TQueryClient& queryClient) {
    Cout << "Recall initialization..." << Endl;
    // Process each target vector
    for (TSelectTarget& target : SelectTargets) {
        // Create the brute force scan query
        std::string scanQuery = MakeSelect(tableName, nullptr);
        NYdb::TParams params = MakeSelectParams(target.Target, topK);
        
        std::optional<NYdb::TResultSet> resultSet;
        
        // Execute the query to get etalon results
        NYdb::NStatusHelpers::ThrowOnError(queryClient.RetryQuerySync([&scanQuery, &params, &resultSet](NYdb::NQuery::TSession session) {
            auto result = session.ExecuteQuery(
                scanQuery,
                NYdb::NQuery::TTxControl::NoTx(),
                params)
                .GetValueSync();
            
            Y_ABORT_UNLESS(result.IsSuccess(), "Query failed: %s", result.GetIssues().ToString().c_str());
            
            resultSet = result.GetResultSet(0);
            return result;
        }));

        // Process the result set to extract IDs
        NYdb::TResultSetParser parser(*resultSet);
        
        target.Etalons.reserve(topK);
        
        // Extract all IDs from the result set
        while (parser.TryNextRow()) {
            ui64 id = parser.ColumnParser("id").GetUint64();
            target.Etalons.emplace_back(id);
        }
    }
    Cout << "Recall initialization compteted for " << SelectTargets.size() << " targets." << Endl;
}
const TFloatVector& TVectorRecallEvaluator::GetTargetEmbedding(size_t index) const {
    return SelectTargets.at(index).Target;
}

const std::vector<ui64>& TVectorRecallEvaluator::GetTargetEtalons(size_t index) const {
    return SelectTargets.at(index).Etalons;
}

void TVectorRecallEvaluator::AddRecall(double recall) {
    TotalRecall += recall;
    ProcessedTargets++;
}

double TVectorRecallEvaluator::GetAverageRecall() const {
    return ProcessedTargets > 0 ? TotalRecall / ProcessedTargets : 0.0;
}

size_t TVectorRecallEvaluator::GetTargetCount() const {
    return SelectTargets.size();
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
//    return TQueryInfoList(1, SelectScanImpl());
    return TQueryInfoList(1, SelectIndexImpl());
}

TQueryInfo TVectorWorkloadGenerator::SelectImpl(const std::string& query) {
    const auto& targetEmbedding = Params.VectorRecallEvaluator->GetTargetEmbedding(CurrentTargetIndex);
    NYdb::TParams params = MakeSelectParams(targetEmbedding, Params.TopK);
    TQueryInfo queryInfo(query, std::move(params));
    queryInfo.GenericQueryResultCallback = std::bind(&TVectorWorkloadGenerator::RecallCallback, this, std::placeholders::_1);
    return queryInfo;
}

void TVectorWorkloadGenerator::RecallCallback(NYdb::NQuery::TExecuteQueryResult queryResult) {
    Y_ABORT_UNLESS(queryResult.IsSuccess(), "Query failed: %s", queryResult.GetIssues().ToString().c_str());
    
    // Get the result set
    auto resultSet = queryResult.GetResultSet(0);
    NYdb::TResultSetParser parser(resultSet);
    
    // Extract IDs from index search results
    std::vector<ui64> indexResults;
    indexResults.reserve(Params.TopK);
    while (parser.TryNextRow()) {
        ui64 id = parser.ColumnParser("id").GetUint64();
        indexResults.push_back(id);
    }
    
    // Get the etalons for the current target
    const auto& etalons = Params.VectorRecallEvaluator->GetTargetEtalons(CurrentTargetIndex);

    if (etalons.empty()) {
        Cerr << "Warning: Etalon set is empty for target " << CurrentTargetIndex << Endl;
        return;
    }

    std::unordered_set<ui64> etalonSet(etalons.begin(), etalons.end());
    
    // Calculate recall
    // Recall = (number of relevant ids retrieved) / (total number of relevant ids)
    size_t relevantRetrieved = 0;
    for (const auto& id : indexResults) {
        if (etalonSet.find(id) != etalonSet.end()) {
            relevantRetrieved++;
        }
    }
    
    if (relevantRetrieved == 0 && !indexResults.empty()) {
        const auto& targetVector = Params.VectorRecallEvaluator->GetTargetEmbedding(CurrentTargetIndex);
        std::ostringstream vectorStr;
        vectorStr << "[";
        for (size_t i = 0; i < targetVector.size(); ++i) {
            if (i > 0) vectorStr << ",";
            vectorStr << targetVector[i] << "f";
        }
        vectorStr << "]";
        
        Cerr << "Warning: Zero relevant results for target " << CurrentTargetIndex 
             << ". Target vector: " << vectorStr.str() << Endl;
    }

    // Calculate recall for this target
    double recall = etalons.empty() ? 0.0 : static_cast<double>(relevantRetrieved) / etalons.size();
    
    // Add to total recall
    Params.VectorRecallEvaluator->AddRecall(recall);

    // Move to the next target
    CurrentTargetIndex = (CurrentTargetIndex + 1) % Params.VectorRecallEvaluator->GetTargetCount();
}

TQueryInfo TVectorWorkloadGenerator::SelectScanImpl() {
    std::string query = MakeSelect(Params.TableName.c_str(), nullptr);
    return SelectImpl(query);
}

TQueryInfo TVectorWorkloadGenerator::SelectIndexImpl() {
    std::string query = MakeSelect(Params.TableName.c_str(), Params.IndexName.c_str());
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

size_t TVectorWorkloadParams::GetVectorDimension() const {
    std::string query = std::format(R"_(--!syntax_v1
        SELECT Unwrap(ListLength(Knn::FloatFromBinaryString(embedding))) FROM {0} LIMIT 1;
    )_", TableName.c_str());

    std::optional<NYdb::TResultSet> resultSet;
    NYdb::NStatusHelpers::ThrowOnError(QueryClient->RetryQuerySync([&query, &resultSet](NYdb::NQuery::TSession session) {
        auto result = session.ExecuteQuery(
            query,
            NYdb::NQuery::TTxControl::NoTx())
            .GetValueSync();
        
        if (!result.IsSuccess()) {
            return result;
        }
        resultSet = result.GetResultSet(0);
        return result;
    })); 

    NYdb::TResultSetParser parser(*resultSet);
    Y_ABORT_UNLESS(parser.TryNextRow());
    ui64 dimension = parser.ColumnParser(0).GetUint64();
    return dimension;
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
                    VectorRecallEvaluator = MakeHolder<TVectorRecallEvaluator>(VectorDimension, Targets);
                    VectorRecallEvaluator->FillEtalons(TableName.c_str(), TopK, *QueryClient);
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

const TFloatVector& TVectorWorkloadParams::GetRandomSelectEmbedding() const {
    return VectorRecallEvaluator->GetRandomSelectEmbedding();
}

THolder<IWorkloadQueryGenerator> TVectorWorkloadParams::CreateGenerator() const {
    return MakeHolder<TVectorWorkloadGenerator>(this);
}

TString TVectorWorkloadParams::GetWorkloadName() const {
    return "vector";
}

}
