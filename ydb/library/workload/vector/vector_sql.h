#pragma once

#include "vector_workload_params.h"

namespace NYdbWorkload {


// Utility function to get metric info for SQL query
// Returns a tuple of (function_name, is_ascending)
std::tuple<std::string, bool> GetMetricInfo(NYdb::NTable::TVectorIndexSettings::EMetric metric);


// Utility function to create select query
std::string MakeSelect(const TVectorWorkloadParams& params, const TString& indexName);


// Utility function to create parameters for select query
NYdb::TParams MakeSelectParams(const std::string& embeddingBytes, const std::optional<NYdb::TValue>& prefixValue, ui64 limit);

} // namespace NYdbWorkload
