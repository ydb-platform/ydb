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

ui64 TVectorSampler::SelectOneId(bool min) {
    std::string query = std::format(R"_(--!syntax_v1
        SELECT Unwrap(CAST({1} AS uint64)) AS id FROM {0} ORDER BY {1} {2} LIMIT 1;
    )_", Params.TableName.c_str(), Params.KeyColumn.c_str(), min ? "" : "DESC");

    // Execute the query
    std::optional<NYdb::TResultSet> rangeResultSet;
    NYdb::NStatusHelpers::ThrowOnError(Params.QueryClient->RetryQuerySync([&query, &rangeResultSet](NYdb::NQuery::TSession session) {
        auto result = session.ExecuteQuery(
            query,
            NYdb::NQuery::TTxControl::NoTx())
            .GetValueSync();

        Y_ABORT_UNLESS(result.IsSuccess(), "Range query failed: %s", result.GetIssues().ToString().c_str());

        rangeResultSet = result.GetResultSet(0);
        return result;
    }));

    // Parse the range result
    NYdb::TResultSetParser rangeParser(*rangeResultSet);
    Y_ABORT_UNLESS(rangeParser.TryNextRow());
    return rangeParser.ColumnParser("id").GetUint64();
}

void TVectorSampler::SelectPredefinedVectors() {
    Cout << "Selecting " << Params.Targets << " vectors from table " << Params.QueryTableName << " ..." << Endl;

    auto vectorQueryBuilder = TStringBuilder() << "--!syntax_v1\n"
        << "SELECT Unwrap(" << Params.EmbeddingColumn << ") as embedding";
    if (Params.PrefixColumn) {
        vectorQueryBuilder << ", Unwrap(" << Params.PrefixColumn << ") as prefix_value";
    }
    vectorQueryBuilder << " FROM " << Params.QueryTableName
        << " ORDER BY " << Params.QueryTableKeyColumn
        << " LIMIT " << Params.Targets;

    std::string vectorQuery = vectorQueryBuilder;
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
    while (vectorParser.TryNextRow()) {
        TSelectTarget target;
        target.EmbeddingBytes = vectorParser.ColumnParser("embedding").GetString();
        // Get prefix value if column was specified
        if (Params.PrefixColumn) {
            target.PrefixValue = vectorParser.GetValue("prefix_value");
        }
        SelectTargets.push_back(std::move(target));
    }

    if (SelectTargets.size() < Params.Targets) {
        Cerr << "Warning: Only selected " << SelectTargets.size() << " vectors." << Endl;
    } else {
        Cout << "Successfully selected " << SelectTargets.size() << " vectors from the query table." << Endl;
    }

    Y_ABORT_UNLESS(!SelectTargets.empty(), "Query table %s is empty", Params.QueryTableName.c_str());
}

void TVectorSampler::SampleExistingVectors() {
    Y_ABORT_UNLESS(Params.KeyIsInt, "Sampling source data is only supported with integer key column, "
        "but key column '%s' is not of an integer type.", Params.KeyColumn.c_str());

    Cout << "Sampling " << Params.Targets << " vectors from dataset..." << Endl;

    // Create a local random generator
    std::random_device rd;
    std::mt19937_64 gen(rd());

    // Select min and max ID
    ui64 minId = SelectOneId(true);
    ui64 maxId = SelectOneId(false);

    Y_ABORT_UNLESS(minId <= maxId, "Invalid ID range in the dataset: min=%lu, max=%lu", minId, maxId);

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
                SELECT Unwrap(CAST({0} as uint64)) as id, Unwrap({1}) as embedding, Unwrap({2}) as prefix_value
                FROM {3}
                WHERE {0} = {4};
            )_", Params.KeyColumn.c_str(), Params.EmbeddingColumn.c_str(), Params.PrefixColumn->c_str(), Params.TableName.c_str(), randomId);
        } else {
            vectorQuery = std::format(R"_(--!syntax_v1
                SELECT Unwrap(CAST({0} as uint64)) as id, Unwrap({1}) as embedding
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

        ui64 id = vectorParser.ColumnParser("id").GetUint64();
        std::string embeddingBytes = vectorParser.ColumnParser("embedding").GetString();

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

const NYdb::TValue TVectorSampler::GetPrefixValue(size_t targetIndex) const {
    return *SelectTargets.at(targetIndex).PrefixValue;
}

} // namespace NYdbWorkload