#include "vector_sql.h"

#include <util/datetime/base.h>
#include <util/generic/serialized_enum.h>

#include <format>
#include <string>

#include <algorithm>

namespace NYdbWorkload {

// Utility function to get metric info for SQL query
// Returns a tuple of (function_name, is_ascending)
std::tuple<std::string, bool> GetMetricInfo(NYdb::NTable::TVectorIndexSettings::EMetric metric) {
    switch (metric) {
        case NYdb::NTable::TVectorIndexSettings::EMetric::InnerProduct:
            return {"InnerProductSimilarity", false}; // Similarity, higher is better (DESC)

        case NYdb::NTable::TVectorIndexSettings::EMetric::CosineSimilarity:
            return {"CosineSimilarity", false}; // Similarity, higher is better (DESC)

        case NYdb::NTable::TVectorIndexSettings::EMetric::CosineDistance:
            return {"CosineDistance", true}; // Distance, lower is better (ASC)

        case NYdb::NTable::TVectorIndexSettings::EMetric::Manhattan:
            return {"ManhattanDistance", true}; // Distance, lower is better (ASC)

        case NYdb::NTable::TVectorIndexSettings::EMetric::Euclidean:
            return {"EuclideanDistance", true}; // Distance, lower is better (ASC)

        case NYdb::NTable::TVectorIndexSettings::EMetric::Unspecified:
        default:
            Y_ABORT("Unspecified metric");
    }
}


// Utility function to create select query
std::string MakeSelect(const TVectorWorkloadParams& params, const TString& indexName) {
    auto [functionName, isAscending] = GetMetricInfo(params.Metric);

    TStringBuilder ret;
    ret << "--!syntax_v1" << "\n";
    ret << "DECLARE $Embedding as String;" << "\n";
    if (params.PrefixColumn)
        ret << "DECLARE $PrefixValue as " << params.PrefixType << ";" << "\n";
    ret << "pragma ydb.KMeansTreeSearchTopSize=\"" << params.KmeansTreeSearchClusters << "\";" << "\n";
    ret << "SELECT UNWRAP(CAST(" << params.KeyColumn << " AS string)) AS id FROM " << params.TableName << "\n";
    if (!indexName.empty())
        ret << "VIEW " << indexName << "\n";
    if (params.PrefixColumn)
        ret << "WHERE " << params.PrefixColumn << " = $PrefixValue" << "\n";
    ret << "ORDER BY Knn::" << functionName << "(" << params.EmbeddingColumn << ", $Embedding) " << (isAscending ? "ASC" : "DESC") << "\n";
    ret << "LIMIT $Limit" << "\n";
    return ret;
}


// Utility function to create parameters for select query
NYdb::TParams MakeSelectParams(const std::string& embeddingBytes, const std::optional<NYdb::TValue>& prefixValue, ui64 limit) {
    NYdb::TParamsBuilder paramsBuilder;

    paramsBuilder.AddParam("$Embedding").String(embeddingBytes).Build();
    paramsBuilder.AddParam("$Limit").Uint64(limit).Build();

    if (prefixValue.has_value()) {
        paramsBuilder.AddParam("$PrefixValue", *prefixValue);
    }

    return paramsBuilder.Build();
}

} // namespace NYdbWorkload
