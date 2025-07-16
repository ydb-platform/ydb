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

using Clock = std::chrono::steady_clock;

// TVectorRecallEvaluator implementation
TVectorRecallEvaluator::TVectorRecallEvaluator(const TVectorWorkloadParams& params)
    : Params(params)
{
}

void TVectorRecallEvaluator::SelectReferenceResults(const TVectorSampler& sampler) {
    Cout << "Selecting reference results..." << Endl;
    const auto startTime = Clock::now();

    auto [functionName, isAscending] = GetMetricInfo(Params.Metric);

    // We'll select multiple reference results with one full scan using a window function
    auto refQueryBuilder = TStringBuilder() << "--!syntax_v1\n"
        << "DECLARE $Samples as List<Struct<id: uint64, embedding: string";
    if (Params.PrefixColumn) {
        refQueryBuilder << ", prefix: " << Params.PrefixType;
    }
    refQueryBuilder << ">>;\n"
        << "SELECT s.id AS id"
        << ", " << (isAscending ? "BOTTOM_BY" : "TOP_BY") << "(UNWRAP(CAST(m." << Params.KeyColumn << " AS string))" <<
            ", Knn::" << functionName << "(m." << Params.EmbeddingColumn << ", s.embedding), " << Params.Limit << ") result_ids"
        << " FROM " << Params.TableName << " m"
        << (Params.PrefixColumn ? " INNER JOIN " : " CROSS JOIN ") << "AS_TABLE($Samples) AS s";
    if (Params.PrefixColumn) {
        refQueryBuilder << " ON s.prefix = m." << *Params.PrefixColumn;
    }
    refQueryBuilder << " GROUP BY s.id";

    std::string refQuery = refQueryBuilder;

    // Process targets in batches
    const ui64 batchSize = 10;
    for (ui64 batchStart = 0; batchStart < sampler.GetTargetCount(); batchStart += batchSize) {
        const size_t batchEnd = (batchStart + batchSize < sampler.GetTargetCount() ? batchStart + batchSize : sampler.GetTargetCount());
        NYdb::TParamsBuilder paramsBuilder;

        auto & builder = paramsBuilder.AddParam("$Samples").BeginList();
        for (size_t i = batchStart; i < batchEnd; i++) {
            builder.AddListItem()
                .BeginStruct()
                .AddMember("id").Uint64(i)
                .AddMember("embedding").String(sampler.GetTargetEmbedding(i));
            if (Params.PrefixColumn) {
                builder.AddMember("prefix", sampler.GetPrefixValue(i));
            }
            builder.EndStruct();
        }
        builder.EndList().Build();

        std::optional<NYdb::TResultSet> resultSet;
        NYdb::NStatusHelpers::ThrowOnError(Params.QueryClient->RetryQuerySync([&](NYdb::NQuery::TSession session) {
            auto result = session.ExecuteQuery(refQuery, NYdb::NQuery::TTxControl::NoTx(), paramsBuilder.Build())
                .GetValueSync();
            Y_ABORT_UNLESS(result.IsSuccess(), "Reference search result query failed: %s", result.GetIssues().ToString().c_str());
            resultSet = result.GetResultSet(0);
            return result;
        }));

        NYdb::TResultSetParser parser(*resultSet);
        while (parser.TryNextRow()) {
            ui64 id = parser.ColumnParser("id").GetUint64();
            std::vector<std::string> refList;
            auto& lst = parser.ColumnParser("result_ids");
            lst.OpenList();
            while (lst.TryNextListItem()) {
                refList.push_back(lst.GetString());
            }
            lst.CloseList();
            References[id] = refList;
        }
    }
    Cout << "Reference results for " << sampler.GetTargetCount()
        << " targets selected in " << (int)((Clock::now() - startTime) / std::chrono::seconds(1)) << " seconds.\n";
}

void TVectorRecallEvaluator::MeasureRecall(const TVectorSampler& sampler) {
    SelectReferenceResults(sampler);

    Cout << "Recall measurement..." << Endl;

    // Create the query for index search
    std::string queryIndex = MakeSelect(Params, Params.IndexName);

    // Process targets in batches
    const auto startTime = Clock::now();
    for (size_t batchStart = 0; batchStart < sampler.GetTargetCount(); batchStart += Params.RecallThreads) {
        size_t batchEnd = std::min(batchStart + Params.RecallThreads, sampler.GetTargetCount());

        // Start async queries for this batch
        std::vector<std::pair<size_t, NYdb::NQuery::TAsyncExecuteQueryResult>> asyncIndexQueries;
        asyncIndexQueries.reserve(batchEnd - batchStart);

        for (size_t i = batchStart; i < batchEnd; i++) {
            const auto& targetEmbedding = sampler.GetTargetEmbedding(i);
            std::optional<NYdb::TValue> prefixValue;
            if (Params.PrefixColumn) {
                prefixValue = sampler.GetPrefixValue(i);
            }

            NYdb::TParams params = MakeSelectParams(targetEmbedding, prefixValue, Params.Limit);

            // Execute index query for recall measurement
            auto asyncIndexResult = Params.QueryClient->RetryQuery([queryIndex, params](NYdb::NQuery::TSession session) {
                return session.ExecuteQuery(
                    queryIndex,
                    NYdb::NQuery::TTxControl::NoTx(),
                    params);
            });

            asyncIndexQueries.emplace_back(i, std::move(asyncIndexResult));
        }

        // Wait for all index queries in this batch to complete and measure recall
        for (auto& [targetIndex, asyncResult] : asyncIndexQueries) {
            auto result = asyncResult.GetValueSync();
            // Process the index query result and calculate recall using reference nearest neighbours
            ProcessIndexQueryResult(result, targetIndex, References[targetIndex], false);
        }
    }

    Cout << "Recall measurement completed for " << sampler.GetTargetCount()
        << " targets in " << (int)((Clock::now() - startTime) / std::chrono::seconds(1)) << " seconds."
        << "\nAverage recall: " << GetAverageRecall() << Endl;
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
void TVectorRecallEvaluator::ProcessIndexQueryResult(const NYdb::NQuery::TExecuteQueryResult& queryResult,
    size_t targetIndex, const std::vector<std::string>& references, bool verbose) {
    Y_ABORT_UNLESS(queryResult.IsSuccess(), "Query failed: %s", queryResult.GetIssues().ToString().c_str());

    // Get the result set
    auto resultSet = queryResult.GetResultSet(0);
    NYdb::TResultSetParser parser(resultSet);

    // Extract IDs from index search results
    std::vector<std::string> indexResults;
    while (parser.TryNextRow()) {
        indexResults.push_back(parser.ColumnParser("id").GetString());
    }

    // Create reference set for efficient lookup
    std::unordered_set<std::string> referenceSet(references.begin(), references.end());

    if (references.empty()) {
        Cerr << "Warning: Empty references for target " << targetIndex << "\n";
    } else if (indexResults.empty()) {
        Cerr << "Warning: Empty index results for target " << targetIndex << "\n";
    } else {
        // Check if the first reference is the target ID itself
        const std::string & targetId = references[0];

        if (verbose && indexResults[0] != targetId) {
            Cerr << "Warning: Target ID " << targetId << " is not the first result for target "
                 << targetIndex << ". Found " << indexResults[0] << " instead." << Endl;
        }

        // Calculate recall
        size_t relevantRetrieved = 0;
        for (const auto& id : indexResults) {
            if (referenceSet.count(id)) {
                relevantRetrieved++;
            }
        }

        // Calculate recall for this target
        double recall = references.empty() ? 0.0 : static_cast<double>(relevantRetrieved) / references.size();
        AddRecall(recall);

        // Add warning when zero relevant results found
        if (verbose && relevantRetrieved == 0 && !indexResults.empty()) {
            Cerr << "Warning: Zero relevant results for target " << targetIndex << Endl;
        }
    }
}

} // namespace NYdbWorkload
