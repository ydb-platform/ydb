#include "vector.h"

#include <util/datetime/base.h>
#include <util/generic/serialized_enum.h>

#include <format>
#include <string>
#include <random>

namespace NYdbWorkload {

std::string MakeSelect(const char* tableName, const char* indexName, size_t kmeansTreeClusters) {
    return std::format(R"_(--!syntax_v1
        DECLARE $Embedding as String;
        pragma ydb.KMeansTreeSearchTopSize="{0}";
        SELECT id
        FROM {1}
        {2}
        ORDER BY Knn::CosineDistance(embedding, $Embedding)
        LIMIT $K;
    )_", 
        kmeansTreeClusters,
        tableName, 
        indexName ? std::format("VIEW {0}", indexName) : ""
    );
}

NYdb::TParams MakeSelectParams(const std::string& embeddingBytes, ui64 topK) {
    NYdb::TParamsBuilder paramsBuilder;
    
    paramsBuilder.AddParam("$Embedding").String(embeddingBytes).Build();
    paramsBuilder.AddParam("$K").Uint64(topK).Build();

    return paramsBuilder.Build();
}

TVectorRecallEvaluator::TVectorRecallEvaluator() 
{
}

TVectorRecallEvaluator::~TVectorRecallEvaluator() {
    if (ProcessedTargets)
        Cout << "Average recall: " << GetAverageRecall() << Endl;
}

void TVectorRecallEvaluator::SampleExistingVectors(const char* tableName, size_t targetCount, NYdb::NQuery::TQueryClient& queryClient) {
    Cout << "Sampling " << targetCount << " vectors from dataset..." << Endl;
    
    // Create a local random generator
    std::random_device rd;
    std::mt19937_64 gen(rd());
       
    // First query to get min and max ID range
    std::string rangeQuery = std::format(R"_(--!syntax_v1
        SELECT Unwrap(MIN(id)) as min_id, Unwrap(MAX(id)) as max_id FROM {0};
    )_", tableName);
    
    // Execute the range query
    std::optional<NYdb::TResultSet> rangeResultSet;
    NYdb::NStatusHelpers::ThrowOnError(queryClient.RetryQuerySync([&rangeQuery, &rangeResultSet](NYdb::NQuery::TSession session) {
        auto result = session.ExecuteQuery(
            rangeQuery,
            NYdb::NQuery::TTxControl::NoTx())
            .GetValueSync();
        
        Y_ABORT_UNLESS(result.IsSuccess(), "Range query failed: %s", result.GetIssues().ToString().c_str());
        
        rangeResultSet = result.GetResultSet(0);
        return result;
    }));
    
    // Parse the range result
    NYdb::TResultSetParser rangeParser(*rangeResultSet);
    Y_ABORT_UNLESS(rangeParser.TryNextRow());
    ui64 minId = rangeParser.ColumnParser("min_id").GetUint64();
    ui64 maxId = rangeParser.ColumnParser("max_id").GetUint64();
    
    Y_ABORT_UNLESS(minId <= maxId, "Invalid ID range in the dataset: min=%lu, max=%lu", minId, maxId);
    
    // Query to get total count of vectors in the table
    std::string countQuery = std::format(R"_(--!syntax_v1
        SELECT Unwrap(COUNT(*)) FROM {0};
    )_", tableName);
    
    // Execute the count query
    std::optional<NYdb::TResultSet> countResultSet;
    NYdb::NStatusHelpers::ThrowOnError(queryClient.RetryQuerySync([&countQuery, &countResultSet](NYdb::NQuery::TSession session) {
        auto result = session.ExecuteQuery(
            countQuery,
            NYdb::NQuery::TTxControl::NoTx())
            .GetValueSync();
        
        Y_ABORT_UNLESS(result.IsSuccess(), "Count query failed: %s", result.GetIssues().ToString().c_str());
        
        countResultSet = result.GetResultSet(0);
        return result;
    }));
    
    // Parse the count result
    NYdb::TResultSetParser countParser(*countResultSet);
    Y_ABORT_UNLESS(countParser.TryNextRow());
    ui64 totalVectors = countParser.ColumnParser(0).GetUint64();
    
    Y_ABORT_UNLESS(totalVectors > 0, "No vectors found in the dataset");
    
    // If we have fewer vectors than requested targets, adjust targetCount
    if (totalVectors < targetCount) {
        Cerr << "Warning: Requested " << targetCount << " targets but only " << totalVectors 
             << " vectors exist in the dataset. Using all available vectors." << Endl;
        targetCount = totalVectors;
    }
    
    // Generate random IDs in the range
    std::set<ui64> sampleIds;
    std::uniform_int_distribution<ui64> idDist(minId, maxId);
    size_t attemptsLimit = targetCount * 10; // Limit the number of attempts to avoid infinite loops
    size_t attempts = 0;
    
    // Reserve space for targets
    SelectTargets.clear();
    SelectTargets.reserve(targetCount);
    
    // Keep sampling until we have enough targets or reach attempt limit
    while (SelectTargets.size() < targetCount && attempts < attemptsLimit) {
        ui64 randomId = idDist(gen);
        
        // Skip if we've already tried this ID
        if (sampleIds.find(randomId) != sampleIds.end()) {
            attempts++;
            continue;
        }
        
        sampleIds.insert(randomId);
        attempts++;
        
        // Query to get vector by ID
        std::string vectorQuery = std::format(R"_(--!syntax_v1
            SELECT id, Unwrap(embedding) as embedding FROM {0} WHERE id = {1};
        )_", tableName, randomId);
        
        std::optional<NYdb::TResultSet> vectorResultSet;
        NYdb::NStatusHelpers::ThrowOnError(queryClient.RetryQuerySync([&vectorQuery, &vectorResultSet](NYdb::NQuery::TSession session) {
            auto result = session.ExecuteQuery(
                vectorQuery,
                NYdb::NQuery::TTxControl::NoTx())
                .GetValueSync();
            
            Y_ABORT_UNLESS(result.IsSuccess(), "Vector query failed: %s", result.GetIssues().ToString().c_str());
            
            vectorResultSet = result.GetResultSet(0);
            return result;
        }));
        
        // Parse the vector result
        NYdb::TResultSetParser vectorParser(*vectorResultSet);
        if (!vectorParser.TryNextRow()) {
            // ID doesn't exist, try another random ID
            continue;
        }
        
        ui64 id = vectorParser.ColumnParser("id").GetUint64();
        std::string embeddingBytes = vectorParser.ColumnParser("embedding").GetString();
        
        // Ensure we got a valid embedding
        if (embeddingBytes.empty()) {
            Cerr << "Warning: Empty embedding retrieved for id " << id << Endl;
            continue;
        }
        
        // Create target with the sampled vector
        TSelectTarget target;
        target.EmbeddingBytes = std::move(embeddingBytes);
        target.Etalons.push_back(id);  // The vector's own ID is an etalon
        
        SelectTargets.push_back(std::move(target));
    }
    
    if (SelectTargets.size() < targetCount) {
        Cerr << "Warning: Could only sample " << SelectTargets.size() << " vectors after " 
             << attempts << " attempts." << Endl;
    } else {
        Cout << "Successfully sampled " << SelectTargets.size() << " vectors from the dataset." << Endl;
    }
}



void TVectorRecallEvaluator::FillEtalons(const char* tableName, size_t topK, NYdb::NQuery::TQueryClient& queryClient) {
    Cout << "Recall initialization..." << Endl;
    
    // Prepare the query template
    std::string queryTemplate = MakeSelect(tableName, nullptr, 0);
    
    // Maximum number of concurrent queries to execute
    const size_t MAX_CONCURRENT_QUERIES = 20;
    
    // Process targets in batches
    for (size_t batchStart = 0; batchStart < SelectTargets.size(); batchStart += MAX_CONCURRENT_QUERIES) {
        size_t batchEnd = std::min(batchStart + MAX_CONCURRENT_QUERIES, SelectTargets.size());
        
        // Start async queries for this batch
        std::vector<std::pair<size_t, NYdb::NQuery::TAsyncExecuteQueryResult>> asyncQueries;
        asyncQueries.reserve(batchEnd - batchStart);
        
        for (size_t i = batchStart; i < batchEnd; i++) {
            NYdb::TParams params = MakeSelectParams(SelectTargets[i].EmbeddingBytes, topK);
            
            auto asyncResult = queryClient.RetryQuery([queryTemplate, params](NYdb::NQuery::TSession session) {
                return session.ExecuteQuery(
                    queryTemplate,
                    NYdb::NQuery::TTxControl::NoTx(),
                    params);
            });
            
            asyncQueries.emplace_back(i, std::move(asyncResult));
        }
        
        // Wait for all queries in this batch to complete
        for (auto& [targetIndex, asyncResult] : asyncQueries) {
            auto result = asyncResult.GetValueSync();
            Y_ABORT_UNLESS(result.IsSuccess(), "Query failed for target %zu: %s", 
                          targetIndex, result.GetIssues().ToString().c_str());
            
            auto resultSet = result.GetResultSet(0);
            NYdb::TResultSetParser parser(resultSet);
            
            // Clear and prepare etalons for this target
            SelectTargets[targetIndex].Etalons.clear();
            SelectTargets[targetIndex].Etalons.reserve(topK);
            
            // Extract all IDs from the result set
            while (parser.TryNextRow()) {
                ui64 id = parser.ColumnParser("id").GetUint64();
                SelectTargets[targetIndex].Etalons.push_back(id);
            }
        }
        
        // Log progress for large datasets
        if (SelectTargets.size() > 100 && (batchEnd % 100 == 0 || batchEnd == SelectTargets.size())) {
            Cout << "Processed " << batchEnd << " of " << SelectTargets.size() << " targets..." << Endl;
        }
    }
    
    // Verify all targets have etalons
    size_t emptyEtalons = 0;
    for (const auto& target : SelectTargets) {
        if (target.Etalons.empty()) {
            emptyEtalons++;
        }
    }
    
    if (emptyEtalons > 0) {
        Cerr << "Warning: " << emptyEtalons << " targets have empty etalon sets" << Endl;
    }
    
    Cout << "Recall initialization completed for " << SelectTargets.size() << " targets." << Endl;
}



const std::string& TVectorRecallEvaluator::GetTargetEmbedding(size_t index) const {
    return SelectTargets.at(index).EmbeddingBytes;
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
    while (parser.TryNextRow()) {
        ui64 id = parser.ColumnParser("id").GetUint64();
        indexResults.push_back(id);
    }
    
    // Get the etalons for the current target
    const auto& etalons = Params.VectorRecallEvaluator->GetTargetEtalons(CurrentTargetIndex);
    
    // Check if target ID is first in results
    if (!indexResults.empty() && !etalons.empty()) {
        ui64 targetId = etalons[0]; // First etalon is the target ID itself
        
        if (indexResults[0] != targetId) {
            Cerr << "Warning: Target ID " << targetId << " is not the first result for target " 
                 << CurrentTargetIndex << ". Found " << indexResults[0] << " instead." << Endl;
        }
        
        // Calculate recall (including the target vector itself)
        size_t relevantRetrieved = 0;
        for (const auto& id : indexResults) {
            // Check if this ID is in the etalon results
            if (std::find(etalons.begin(), etalons.end(), id) != etalons.end()) {
                relevantRetrieved++;
            }
        }
        
        // Calculate recall for this target
        double recall = etalons.empty() ? 0.0 : static_cast<double>(relevantRetrieved) / etalons.size();
        
        // Add to total recall
        Params.VectorRecallEvaluator->AddRecall(recall);
        
        // Add warning when zero relevant results found, but don't print embedding bytes
        if (relevantRetrieved == 0 && !indexResults.empty()) {
            Cerr << "Warning: Zero relevant results for target " << CurrentTargetIndex << Endl;
        }
    } else {
        // Handle empty results or empty etalons
        Cerr << "Warning: Empty results or etalons for target " << CurrentTargetIndex << Endl;
    }
    
    // Move to the next target
    CurrentTargetIndex = (CurrentTargetIndex + 1) % Params.VectorRecallEvaluator->GetTargetCount();
}


TQueryInfo TVectorWorkloadGenerator::SelectScanImpl() {
    std::string query = MakeSelect(Params.TableName.c_str(), nullptr, 0);
    return SelectImpl(query);
}

TQueryInfo TVectorWorkloadGenerator::SelectIndexImpl() {
    std::string query = MakeSelect(Params.TableName.c_str(), Params.IndexName.c_str(), Params.KmeansTreeSearchClusters);
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
                    VectorRecallEvaluator = MakeHolder<TVectorRecallEvaluator>();
                    VectorRecallEvaluator->SampleExistingVectors(TableName.c_str(), Targets, *QueryClient);
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

THolder<IWorkloadQueryGenerator> TVectorWorkloadParams::CreateGenerator() const {
    return MakeHolder<TVectorWorkloadGenerator>(this);
}

TString TVectorWorkloadParams::GetWorkloadName() const {
    return "vector";
}

}
