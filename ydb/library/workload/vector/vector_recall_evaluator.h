#pragma once

#include <ydb/library/workload/abstract/workload_query_generator.h>

#include <cctype>
#include <vector>
#include <string>

namespace NYdbWorkload {

class TVectorWorkloadParams;
class TVectorSampler;

class TVectorRecallEvaluator {
public:
    TVectorRecallEvaluator(const TVectorWorkloadParams& params);
    
    // Core functionality for recall measurement using sampled vectors
    void MeasureRecall(const TVectorSampler& sampler);
    
    // Recall metrics methods
    void AddRecall(double recall);
    double GetAverageRecall() const;
    double GetTotalRecall() const;
    size_t GetProcessedTargets() const;
    
private:
    // Process index query results (internal method)
    void ProcessIndexQueryResult(const NYdb::NQuery::TExecuteQueryResult& result, size_t targetIndex, const std::vector<ui64>& etalons, bool verbose);
    
    const TVectorWorkloadParams& Params;

    double TotalRecall = 0.0;
    size_t ProcessedTargets = 0;
};

} // namespace NYdbWorkload
