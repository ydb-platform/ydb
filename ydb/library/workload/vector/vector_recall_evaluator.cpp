#include "vector_sql.h"
#include "vector_recall_evaluator.h"
#include "vector_sampler.h"
#include "vector_workload_params.h"

#include <util/datetime/base.h>
#include <util/generic/serialized_enum.h>

#include <format>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include <algorithm>

namespace NYdbWorkload {

// TVectorRecallEvaluator implementation
TVectorRecallEvaluator::TVectorRecallEvaluator(const TVectorWorkloadParams& params)
    : Params(params)
{
}

void TVectorRecallEvaluator::MeasureRecall(const TVectorSampler& sampler) {
    Cout << "Recall measurement..." << Endl;
    
    // Prepare the query for scan
    std::string queryScan = MakeSelect(Params.TableName, {}, Params.KeyColumn, Params.EmbeddingColumn, Params.PrefixColumn, 0, Params.Metric);
    
    // Create the query for index search
    std::string queryIndex = MakeSelect(Params.TableName, Params.IndexName, Params.KeyColumn, Params.EmbeddingColumn, Params.PrefixColumn, Params.KmeansTreeSearchClusters, Params.Metric);

    // Process targets in batches
    for (size_t batchStart = 0; batchStart < sampler.GetTargetCount(); batchStart += Params.RecallThreads) {
        size_t batchEnd = std::min(batchStart + Params.RecallThreads, sampler.GetTargetCount());
        
        // Start async queries for this batch - both scan and index queries
        std::vector<std::pair<size_t, NYdb::NQuery::TAsyncExecuteQueryResult>> asyncScanQueries;
        std::vector<std::pair<size_t, NYdb::NQuery::TAsyncExecuteQueryResult>> asyncIndexQueries;
        asyncScanQueries.reserve(batchEnd - batchStart);
        asyncIndexQueries.reserve(batchEnd - batchStart);
        
        for (size_t i = batchStart; i < batchEnd; i++) {
            const auto& targetEmbedding = sampler.GetTargetEmbedding(i);
            std::optional<i64> prefixValue;
            if (Params.PrefixColumn) {
                prefixValue = sampler.GetPrefixValue(i);
            }

            NYdb::TParams params = MakeSelectParams(targetEmbedding, prefixValue, Params.Limit);
            
            // Execute scan query for ground truth
            auto asyncScanResult = Params.QueryClient->RetryQuery([queryScan, params](NYdb::NQuery::TSession session) {
                return session.ExecuteQuery(
                    queryScan,
                    NYdb::NQuery::TTxControl::NoTx(),
                    params);
            });
            
            // Execute index query for recall measurement
            auto asyncIndexResult = Params.QueryClient->RetryQuery([queryIndex, params](NYdb::NQuery::TSession session) {
                return session.ExecuteQuery(
                    queryIndex,
                    NYdb::NQuery::TTxControl::NoTx(),
                    params);
            });
            
            asyncScanQueries.emplace_back(i, std::move(asyncScanResult));
            asyncIndexQueries.emplace_back(i, std::move(asyncIndexResult));
        }
        
        // Wait for all scan queries in this batch to complete and build ground truth
        std::unordered_map<size_t, std::vector<ui64>> batchEtalons;
        for (auto& [targetIndex, asyncResult] : asyncScanQueries) {
            auto result = asyncResult.GetValueSync();
            Y_ABORT_UNLESS(result.IsSuccess(), "Scan query failed for target %zu: %s", 
                          targetIndex, result.GetIssues().ToString().c_str());
            
            auto resultSet = result.GetResultSet(0);
            NYdb::TResultSetParser parser(resultSet);
            
            // Build etalons for this target locally
            std::vector<ui64> etalons;
            etalons.reserve(Params.Limit);
            
            // Extract all IDs from the result set
            while (parser.TryNextRow()) {
                ui64 id = parser.ColumnParser(Params.KeyColumn).GetUint64();
                etalons.push_back(id);
            }
            if (etalons.empty()) {
                Cerr << "Warning: target " << targetIndex << " have empty etalon sets" << Endl;
            }
            
            batchEtalons[targetIndex] = std::move(etalons);
        }
        
        // Wait for all index queries in this batch to complete and measure recall
        for (auto& [targetIndex, asyncResult] : asyncIndexQueries) {
            auto result = asyncResult.GetValueSync();
            // Process the index query result and calculate recall using etalon nearest neighbours
            ProcessIndexQueryResult(result, targetIndex, batchEtalons[targetIndex], false);
        }
        
        // Log progress for large datasets
        if (sampler.GetTargetCount() > 100 && (batchEnd % 100 == 0 || batchEnd == sampler.GetTargetCount())) {
            Cout << "Processed " << batchEnd << " of " << sampler.GetTargetCount() << " targets..." << Endl;
        }
    }
    
    Cout << "Recall measurement completed for " << sampler.GetTargetCount() << " targets." << Endl;
    if (ProcessedTargets > 0) {
        Cout << "Average recall: " << GetAverageRecall() << " (processed " << ProcessedTargets << " targets)" << Endl;
    }
}

void TVectorRecallEvaluator::AddRecall(double recall) {
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

// Process index query results
void TVectorRecallEvaluator::ProcessIndexQueryResult(const NYdb::NQuery::TExecuteQueryResult& queryResult, size_t targetIndex, const std::vector<ui64>& etalons, bool verbose) {
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
    
    // Create etalon set for efficient lookup
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
