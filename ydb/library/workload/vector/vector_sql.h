#pragma once

#include <ydb/library/workload/abstract/workload_query_generator.h>

#include <cctype>
#include <mutex>
#include <vector>
#include <string>
#include <atomic>

namespace NYdbWorkload {


// Utility function to get metric info for SQL query
// Returns a tuple of (function_name, is_ascending)
std::tuple<std::string, bool> GetMetricInfo(NYdb::NTable::TVectorIndexSettings::EMetric metric);


// Utility function to create select query
std::string MakeSelect(const TString& tableName, const TString& indexName, 
    const std::string& keyColumn, const std::string& embeddingColumn, const std::optional<std::string>& prefixColumn, 
    size_t kmeansTreeClusters, NYdb::NTable::TVectorIndexSettings::EMetric metric);


// Utility function to create parameters for select query
NYdb::TParams MakeSelectParams(const std::string& embeddingBytes, std::optional<i64> prefixValue, ui64 limit);

} // namespace NYdbWorkload
