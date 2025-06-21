#include "vector_sql.h"
#include "vector_recall_evaluator.h"
#include "vector_workload_params.h"

#include <util/datetime/base.h>
#include <util/generic/serialized_enum.h>

#include <format>
#include <string>

#include <algorithm>

namespace NYdbWorkload {

// TVectorRecallEvaluator implementation
TVectorRecallEvaluator::TVectorRecallEvaluator(const TVectorWorkloadParams& params)
    : Params(params)
{
}

TVectorRecallEvaluator::~TVectorRecallEvaluator() {
    if (ProcessedTargets)
        Cout << "Average recall: " << GetAverageRecall() << Endl;
}

void TVectorRecallEvaluator::SampleExistingVectors() {
    Cout << "Sampling " << Params.Targets << " vectors from dataset..." << Endl;
    
    // Create a local random generator
    std::random_device rd;
    std::mt19937_64 gen(rd());
       
    // First query to get min and max ID range
    std::string minMaxQuery = std::format(R"_(--!syntax_v1
        SELECT Unwrap(MIN(id)) as min_id, Unwrap(MAX(id)) as max_id FROM {0};
    )_", Params.TableName.c_str());
    
    // Execute the range query
    std::optional<NYdb::TResultSet> rangeResultSet;
    NYdb::NStatusHelpers::ThrowOnError(Params.QueryClient->RetryQuerySync([&minMaxQuery, &rangeResultSet](NYdb::NQuery::TSession session) {
        auto result = session.ExecuteQuery(
            minMaxQuery,
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
    )_", Params.TableName.c_str());
    
    // Execute the count query
    std::optional<NYdb::TResultSet> countResultSet;
    NYdb::NStatusHelpers::ThrowOnError(Params.QueryClient->RetryQuerySync([&countQuery, &countResultSet](NYdb::NQuery::TSession session) {
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
    
    // If we have fewer vectors than requested targets, adjust Params.Targets
    Y_ABORT_UNLESS(totalVectors >= Params.Targets,  "Requested more targets than vectors exist in the dataset.");
    
    // Generate random IDs in the range
    std::set<ui64> sampleIds;
    std::uniform_int_distribution<ui64> idDist(minId, maxId);
    size_t attemptsLimit = Params.Targets * 10; // Limit the number of attempts to avoid infinite loops
    size_t attempts = 0;
    
    // Reserve space for targets
    SelectTargets.clear();
    SelectTargets.reserve(Params.Targets);
    
    // Keep sampling until we have enough targets or reach attempt limit
    while (SelectTargets.size() < Params.Targets && attempts < attemptsLimit) {
        ui64 randomId = idDist(gen);
        
        // Skip if we've already tried this ID
        if (sampleIds.find(randomId) != sampleIds.end()) {
            attempts++;
            continue;
        }
        
        sampleIds.insert(randomId);
        attempts++;
        
        // Create query string based on whether we have a prefix column
        std::string vectorQuery;
        if (Params.PrefixColumn) {
            vectorQuery = std::format(R"_(--!syntax_v1
                SELECT {0}, Unwrap({1}) as embedding, Unwrap({2}) as prefix_value 
                FROM {3} 
                WHERE id = {4};
            )_", Params.KeyColumn.c_str(), Params.EmbeddingColumn.c_str(), Params.PrefixColumn->c_str(), Params.TableName.c_str(), randomId);
        } else {
            vectorQuery = std::format(R"_(--!syntax_v1
                SELECT {0}, Unwrap({1}) as embedding 
                FROM {2} 
                WHERE id = {3};
            )_", Params.KeyColumn.c_str(), Params.EmbeddingColumn.c_str(), Params.TableName.c_str(), randomId);
        }
        
        std::optional<NYdb::TResultSet> vectorResultSet;
        NYdb::NStatusHelpers::ThrowOnError(Params.QueryClient->RetryQuerySync([&vectorQuery, &vectorResultSet](NYdb::NQuery::TSession session) {
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
        
        ui64 id = vectorParser.ColumnParser(Params.KeyColumn).GetUint64();
        std::string embeddingBytes = vectorParser.ColumnParser(Params.EmbeddingColumn).GetString();
        
        // Ensure we got a valid embedding
        Y_ABORT_UNLESS(!embeddingBytes.empty(), "Empty embedding retrieved for id %" PRIu64, id);
        
        // Create target with the sampled vector
        TSelectTarget target;
        target.EmbeddingBytes = std::move(embeddingBytes);
        target.NearestNeighbors.push_back(id);  // The vector's own ID is an etalon
        
        // Get prefix value if column was specified
        if (Params.PrefixColumn) {
            const auto& prefixCell = vectorParser.ColumnParser("prefix_value");
            NYdb::EPrimitiveType primitiveType = prefixCell.GetPrimitiveType();
            
            // Handle different integer types based on primitive type
            switch (primitiveType) {
                case NYdb::EPrimitiveType::Int8:
                    target.PrefixValue = prefixCell.GetInt8();
                    break;
                case NYdb::EPrimitiveType::Int16:
                    target.PrefixValue = prefixCell.GetInt16();
                    break;
                case NYdb::EPrimitiveType::Int32:
                    target.PrefixValue = prefixCell.GetInt32();
                    break;
                case NYdb::EPrimitiveType::Int64:
                    target.PrefixValue = prefixCell.GetInt64();
                    break;
                case NYdb::EPrimitiveType::Uint8:
                    target.PrefixValue = prefixCell.GetUint8();
                    break;
                case NYdb::EPrimitiveType::Uint16:
                    target.PrefixValue = prefixCell.GetUint16();
                    break;
                case NYdb::EPrimitiveType::Uint32:
                    target.PrefixValue = prefixCell.GetUint32();
                    break;
                case NYdb::EPrimitiveType::Uint64: {
                    ui64 uvalue = prefixCell.GetUint64();
                    Y_ABORT_UNLESS(uvalue <= static_cast<ui64>(std::numeric_limits<i64>::max()), 
                                  "Prefix value %" PRIu64 " is too large for i64", uvalue);
                    target.PrefixValue = static_cast<i64>(uvalue);
                    break;
                }
                default:
                    Y_ABORT_UNLESS(false, "Unexpected primitive type for prefix column: %d", 
                                  static_cast<int>(primitiveType));
            }
        }
        
        SelectTargets.push_back(std::move(target));
    }
    
    if (SelectTargets.size() < Params.Targets) {
        Cerr << "Warning: Could only sample " << SelectTargets.size() << " vectors after " << attempts << " attempts." << Endl;
    } else {
        Cout << "Successfully sampled " << SelectTargets.size() << " vectors from the dataset." << Endl;
    }
    Y_ABORT_UNLESS(!SelectTargets.empty(), "Failed to sample any vectors from the dataset");
}

void TVectorRecallEvaluator::FillEtalons() {
    Cout << "Recall initialization..." << Endl;
    
    // Prepare the query template
    std::string queryTemplate = MakeSelect(Params.TableName, {}, Params.KeyColumn, Params.EmbeddingColumn, Params.PrefixColumn, 0, Params.Metric);
    
    // Maximum number of concurrent queries to execute
    const size_t MAX_CONCURRENT_QUERIES = 20;
    
    // Process targets in batches
    for (size_t batchStart = 0; batchStart < SelectTargets.size(); batchStart += MAX_CONCURRENT_QUERIES) {
        size_t batchEnd = std::min(batchStart + MAX_CONCURRENT_QUERIES, SelectTargets.size());
        
        // Start async queries for this batch
        std::vector<std::pair<size_t, NYdb::NQuery::TAsyncExecuteQueryResult>> asyncQueries;
        asyncQueries.reserve(batchEnd - batchStart);
        
        for (size_t i = batchStart; i < batchEnd; i++) {
            const auto& targetEmbedding = GetTargetEmbedding(i);
            std::optional<i64> prefixValue;
            if (Params.PrefixColumn) {
                prefixValue = GetPrefixValue(i);
            }

            NYdb::TParams params = MakeSelectParams(targetEmbedding, prefixValue, Params.Limit);
            
            auto asyncResult = Params.QueryClient->RetryQuery([queryTemplate, params](NYdb::NQuery::TSession session) {
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
            SelectTargets[targetIndex].NearestNeighbors.clear();
            SelectTargets[targetIndex].NearestNeighbors.reserve(Params.Limit);
            
            // Extract all IDs from the result set
            while (parser.TryNextRow()) {
                ui64 id = parser.ColumnParser(Params.KeyColumn).GetUint64();
                SelectTargets[targetIndex].NearestNeighbors.push_back(id);
            }
            if (SelectTargets[targetIndex].NearestNeighbors.empty()) {
                Cerr << "Warning: target " << targetIndex << " have empty etalon sets" << Endl;
            }
        }
        
        // Log progress for large datasets
        if (SelectTargets.size() > 100 && (batchEnd % 100 == 0 || batchEnd == SelectTargets.size())) {
            Cout << "Processed " << batchEnd << " of " << SelectTargets.size() << " targets..." << Endl;
        }
    }
    
    Cout << "Recall initialization completed for " << SelectTargets.size() << " targets." << Endl;
}

const std::string& TVectorRecallEvaluator::GetTargetEmbedding(size_t index) const {
    return SelectTargets.at(index).EmbeddingBytes;
}

const std::vector<ui64>& TVectorRecallEvaluator::GetTargetEtalons(size_t index) const {
    return SelectTargets.at(index).NearestNeighbors;
}

void TVectorRecallEvaluator::AddRecall(double recall) {
    std::lock_guard<std::mutex> lock(Mutex);
    TotalRecall += recall;
    ProcessedTargets++;
}

double TVectorRecallEvaluator::GetAverageRecall() const {
    return ProcessedTargets > 0 ? TotalRecall / ProcessedTargets : 0.0;
}

double TVectorRecallEvaluator::GetTotalRecall() const {
    return TotalRecall;
}

size_t TVectorRecallEvaluator::GetProcessedTargets() const {
    return ProcessedTargets;
}

size_t TVectorRecallEvaluator::GetTargetCount() const {
    return SelectTargets.size();
}

i64 TVectorRecallEvaluator::GetPrefixValue(size_t targetIndex) const {
    return SelectTargets.at(targetIndex).PrefixValue;
}

// Process query results
void TVectorRecallEvaluator::ProcessQueryResult(const NYdb::NQuery::TExecuteQueryResult& queryResult, size_t targetIndex, bool verbose) {
    if (!queryResult.IsSuccess()) {
        // Ignore the error. It's printed in the verbose mode
        return;
    }
    
    // Get the result set
    auto resultSet = queryResult.GetResultSet(0);
    NYdb::TResultSetParser parser(resultSet);
    
    // Extract IDs from index search results
    std::vector<ui64> indexResults;
    while (parser.TryNextRow()) {
        ui64 id = parser.ColumnParser(Params.KeyColumn).GetUint64();
        indexResults.push_back(id);
    }
    
    // Get the etalons for the specified target
    const auto& etalons = GetTargetEtalons(targetIndex);
    std::unordered_set<ui64> etalonSet(etalons.begin(), etalons.end());

    // Check if target ID is first in results
    if (!indexResults.empty() && !etalons.empty()) {
        ui64 targetId = etalons[0]; // First etalon is the target ID itself
        
        if (verbose && indexResults[0] != targetId) {
            Cerr << "Warning: Target ID " << targetId << " is not the first result for target " 
                 << targetIndex << ". Found " << indexResults[0] << " instead." << Endl;
        }
        
        // Calculate recall
        size_t relevantRetrieved = 0;
        for (const auto& id : indexResults) {
            if (etalonSet.count(id)) {
                relevantRetrieved++;
            }
        }
        
        // Calculate recall for this target
        double recall = etalons.empty() ? 0.0 : static_cast<double>(relevantRetrieved) / etalons.size();
        AddRecall(recall);

        // Add warning when zero relevant results found
        if (verbose && relevantRetrieved == 0 && !indexResults.empty()) {
            Cerr << "Warning: Zero relevant results for target " << targetIndex << Endl;
        }
    } else {
        // Handle empty results or empty etalons
        if (verbose)
            Cerr << "Warning: Empty results or etalons for target " << targetIndex << Endl;
    }
}

} // namespace NYdbWorkload
