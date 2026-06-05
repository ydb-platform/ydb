#include "fulltext_quality_evaluator.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>

#include <util/datetime/base.h>
#include <util/string/builder.h>

#include <algorithm>
#include <cmath>
#include <format>

namespace NYdbWorkload {

using Clock = std::chrono::steady_clock;

TFulltextQualityEvaluator::TFulltextQualityEvaluator(const TFulltextWorkloadParams& params)
    : Params(params)
{
}

void TFulltextQualityEvaluator::LoadQueries() {
    const TString queryTablePath = Params.GetFullTableName(Params.QueriesTable.c_str());
    const TString selectQuery = std::format(
        R"sql(
        --!syntax_v1
        SELECT `id`, `query`
        FROM `{}`
    )sql",
        queryTablePath.c_str());

    std::optional<NYdb::TResultSet> resultSet;
    NYdb::NStatusHelpers::ThrowOnError(Params.QueryClient->RetryQuerySync([&selectQuery, &resultSet](NYdb::NQuery::TSession session) {
        const auto result = session.ExecuteQuery(
                                       selectQuery,
                                       NYdb::NQuery::TTxControl::NoTx())
                                .GetValueSync();
        Y_ENSURE(result.IsSuccess(), std::format("Failed to read queries table: {}", result.GetIssues().ToString().c_str()));
        resultSet = result.GetResultSet(0);
        return result;
    }));

    NYdb::TResultSetParser parser(*resultSet);
    while (parser.TryNextRow()) {
        const ui64 id = parser.ColumnParser("id").GetUint64();

        auto& column = parser.ColumnParser("query");
        const bool optional = column.GetKind() == NYdb::TTypeParser::ETypeKind::Optional;
        if (optional) {
            column.OpenOptional();
        }

        TString queryText;
        switch (column.GetPrimitiveType()) {
            case NYdb::EPrimitiveType::Utf8:
                queryText = column.GetUtf8();
                break;
            case NYdb::EPrimitiveType::String:
                queryText = column.GetString();
                break;
            default:
                break;
        }

        if (optional) {
            column.CloseOptional();
        }

        if (!queryText.empty()) {
            Queries[id] = queryText;
        }
    }

    Cout << "Loaded " << Queries.size() << " queries for quality evaluation" << Endl;
}

void TFulltextQualityEvaluator::LoadRelevances() {
    const TString relevanceTablePath = Params.GetFullTableName(Params.QueryRelevanceTable.c_str());
    const TString selectQuery = std::format(
        R"sql(
        --!syntax_v1
        SELECT `query_id`, `document_id`, `relevance`
        FROM `{}`
    )sql",
        relevanceTablePath.c_str());

    std::optional<NYdb::TResultSet> resultSet;
    NYdb::NStatusHelpers::ThrowOnError(Params.QueryClient->RetryQuerySync([&selectQuery, &resultSet](NYdb::NQuery::TSession session) {
        const auto result = session.ExecuteQuery(
                                       selectQuery,
                                       NYdb::NQuery::TTxControl::NoTx())
                                .GetValueSync();
        Y_ENSURE(result.IsSuccess(), std::format("Failed to read relevances table: {}", result.GetIssues().ToString().c_str()));
        resultSet = result.GetResultSet(0);
        return result;
    }));

    NYdb::TResultSetParser parser(*resultSet);
    while (parser.TryNextRow()) {
        const ui64 queryId = parser.ColumnParser("query_id").GetUint64();
        const ui64 documentId = parser.ColumnParser("document_id").GetUint64();
        const float relevance = parser.ColumnParser("relevance").GetFloat();

        Relevances[queryId].push_back(TRelevanceJudgment{documentId, relevance});
    }

    Cout << "Loaded relevance judgments for " << Relevances.size() << " queries" << Endl;
}

double TFulltextQualityEvaluator::ComputeDcg(const TVector<float>& relevances, size_t k) const {
    double dcg = 0.0;
    const size_t n = std::min(relevances.size(), k);
    for (size_t i = 0; i < n; ++i) {
        dcg += (std::pow(2.0, relevances[i]) - 1.0) / std::log2(static_cast<double>(i + 2));
    }
    return dcg;
}

double TFulltextQualityEvaluator::ComputeNdcg(const TVector<float>& retrievedRelevances, const TVector<float>& idealRelevances, size_t k) const {
    const double dcg = ComputeDcg(retrievedRelevances, k);
    const double idcg = ComputeDcg(idealRelevances, k);
    if (idcg == 0.0) {
        return 0.0;
    }
    return dcg / idcg;
}

void TFulltextQualityEvaluator::MeasureQuality() {
    LoadQueries();
    LoadRelevances();

    Cout << "Measuring nDCG@" << K << " quality..." << Endl;
    const auto startTime = Clock::now();

    const TString tablePath = Params.GetFullTableName(Params.TableName.c_str());

    for (const auto& [queryId, queryText] : Queries) {
        auto relevIt = Relevances.find(queryId);
        if (relevIt == Relevances.end()) {
            continue;
        }
        const auto& judgments = relevIt->second;

        std::unordered_map<ui64, float> relevanceMap;
        for (const auto& j : judgments) {
            relevanceMap[j.DocumentId] = j.Relevance;
        }

        TVector<float> idealRelevances;
        idealRelevances.reserve(judgments.size());
        for (const auto& j : judgments) {
            idealRelevances.push_back(j.Relevance);
        }
        std::sort(idealRelevances.begin(), idealRelevances.end(), std::greater<float>());

        TString query;
        if (Params.IndexIsRelevance) {
            query = std::format(
                R"sql(
                    --!syntax_v1
                    DECLARE $query AS Utf8;
                    SELECT `id`, FulltextScore(`text`, $query) AS score
                    FROM `{}` VIEW `{}`
                    WHERE FulltextScore(`text`, $query) > 0
                    ORDER BY score DESC
                    LIMIT {};
                )sql",
                tablePath.c_str(),
                Params.IndexName.c_str(),
                K);
        } else {
            query = std::format(
                R"sql(
                    --!syntax_v1
                    DECLARE $query AS Utf8;
                    SELECT `id`
                    FROM `{}` VIEW `{}`
                    WHERE FulltextMatch(`text`, $query)
                    LIMIT {};
                )sql",
                tablePath.c_str(),
                Params.IndexName.c_str(),
                K);
        }

        NYdb::TParams params = NYdb::TParamsBuilder()
                                   .AddParam("$query")
                                   .Utf8(queryText)
                                   .Build()
                                   .Build();

        std::optional<NYdb::TResultSet> resultSet;
        NYdb::NStatusHelpers::ThrowOnError(Params.QueryClient->RetryQuerySync([&query, &params, &resultSet](NYdb::NQuery::TSession session) {
            const auto result = session.ExecuteQuery(
                                           query,
                                           NYdb::NQuery::TTxControl::NoTx(),
                                           params)
                                    .GetValueSync();
            if (!result.IsSuccess()) {
                return result;
            }
            resultSet = result.GetResultSet(0);
            return result;
        }));

        if (!resultSet) {
            continue;
        }

        TVector<float> retrievedRelevances;
        NYdb::TResultSetParser parser(*resultSet);
        while (parser.TryNextRow()) {
            const ui64 docId = parser.ColumnParser("id").GetUint64();
            auto it = relevanceMap.find(docId);
            if (it != relevanceMap.end()) {
                retrievedRelevances.push_back(it->second);
            } else {
                retrievedRelevances.push_back(0.0f);
            }
        }

        const double ndcg = ComputeNdcg(retrievedRelevances, idealRelevances, K);
        TotalNdcg += ndcg;
        ProcessedQueries++;
    }

    const auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(Clock::now() - startTime).count();
    Cout << "nDCG@" << K << " measurement completed for " << ProcessedQueries
         << " queries in " << elapsed << " seconds."
         << "\nAverage nDCG@" << K << ": " << GetAverageNdcg() << Endl;
}

double TFulltextQualityEvaluator::GetAverageNdcg() const {
    return ProcessedQueries > 0 ? TotalNdcg / ProcessedQueries : 0.0;
}

size_t TFulltextQualityEvaluator::GetProcessedQueries() const {
    return ProcessedQueries;
}

} // namespace NYdbWorkload
