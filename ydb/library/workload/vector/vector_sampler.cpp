#include "vector_sampler.h"
#include "vector_workload_params.h"

#include <util/datetime/base.h>
#include <util/generic/serialized_enum.h>

#include <format>
#include <string>
#include <set>
#include <random>
#include <algorithm>

namespace NYdbWorkload {

TVectorSampler::TVectorSampler(const TVectorWorkloadParams& params)
    : Params(params)
{
}

void TVectorSampler::SampleExistingVectors() {
    Cout << "Sampling " << Params.Targets << " vectors from dataset..." << Endl;

    // Create a local random generator
    std::random_device rd;
    std::mt19937_64 gen(rd());

    // First query to get min and max ID range
    std::string minMaxQuery = std::format(R"_(--!syntax_v1
        SELECT Unwrap(MIN({1})) as min_id, Unwrap(MAX({1})) as max_id FROM {0};
    )_", Params.TableName.c_str(), Params.KeyColumn.c_str());

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
                WHERE {0} = {4};
            )_", Params.KeyColumn.c_str(), Params.EmbeddingColumn.c_str(), Params.PrefixColumn->c_str(), Params.TableName.c_str(), randomId);
        } else {
            vectorQuery = std::format(R"_(--!syntax_v1
                SELECT {0}, Unwrap({1}) as embedding
                FROM {2}
                WHERE {0} = {3};
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

        // Get prefix value if column was specified
        if (Params.PrefixColumn) {
            target.PrefixValue = vectorParser.GetValue("prefix_value");
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

const std::string& TVectorSampler::GetTargetEmbedding(size_t index) const {
    return SelectTargets.at(index).EmbeddingBytes;
}

size_t TVectorSampler::GetTargetCount() const {
    return SelectTargets.size();
}

NYdb::TValue TVectorSampler::GetPrefixValue(size_t targetIndex) const {
    return *SelectTargets.at(targetIndex).PrefixValue;
}

} // namespace NYdbWorkload