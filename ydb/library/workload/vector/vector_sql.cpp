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
std::string MakeSelect(const TString& tableName, const TString& indexName, 
        const std::string& keyColumn, const std::string& embeddingColumn, const std::optional<std::string>& prefixColumn, 
        size_t kmeansTreeClusters, NYdb::NTable::TVectorIndexSettings::EMetric metric) {

    auto [functionName, isAscending] = GetMetricInfo(metric);

    TStringBuilder ret;
    ret << "--!syntax_v1" << "\n";
    ret << "DECLARE $Embedding as String;" << "\n";
    if (prefixColumn)
        ret << "DECLARE $PrefixValue as Int64;" << "\n";
    ret << "pragma ydb.KMeansTreeSearchTopSize=\"" << kmeansTreeClusters << "\";" << "\n";
    ret << "SELECT " << keyColumn << " FROM " << tableName << "\n";
    if (!indexName.empty())
        ret << "VIEW " << indexName << "\n";
    if (prefixColumn)
        ret << "WHERE " << prefixColumn << " = $PrefixValue" << "\n";
    ret << "ORDER BY Knn::" << functionName << "(" << embeddingColumn << ", $Embedding) " << (isAscending ? "ASC" : "DESC") << "\n";
    ret << "LIMIT $Limit" << "\n";
    return ret;
}


// Utility function to create parameters for select query
NYdb::TParams MakeSelectParams(const std::string& embeddingBytes, std::optional<i64> prefixValue, ui64 limit) {
    NYdb::TParamsBuilder paramsBuilder;
    
    paramsBuilder.AddParam("$Embedding").String(embeddingBytes).Build();
    paramsBuilder.AddParam("$Limit").Uint64(limit).Build();

    if (prefixValue.has_value()) {
        paramsBuilder.AddParam("$PrefixValue").Int64(*prefixValue).Build();
    }

    return paramsBuilder.Build();
}

} // namespace NYdbWorkload
