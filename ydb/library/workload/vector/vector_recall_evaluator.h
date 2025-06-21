#pragma once

#include <ydb/library/workload/abstract/workload_query_generator.h>

#include <cctype>
#include <vector>
#include <string>

namespace NYdbWorkload {

class TVectorWorkloadParams;

class TVectorRecallEvaluator {
public:
    TVectorRecallEvaluator(const TVectorWorkloadParams& params);
    
    // Core functionality for vector sampling and etalon generation
    void SampleExistingVectors();
    void FillEtalons();

    // Target access methods
    const std::string& GetTargetEmbedding(size_t index) const;
    size_t GetTargetCount() const;
    i64 GetPrefixValue(size_t targetIndex) const;
    
    // Recall metrics methods
    void AddRecall(double recall);
    double GetAverageRecall() const;
    double GetTotalRecall() const;
    size_t GetProcessedTargets() const;
    
private:
    // Process index query results (internal method)
    void ProcessIndexQueryResult(const NYdb::NQuery::TExecuteQueryResult& result, size_t targetIndex, const std::vector<ui64>& etalons, bool verbose);
    const TVectorWorkloadParams& Params;

    struct TSelectTarget {
        std::string EmbeddingBytes;             // Sample targets to use in select workload
        i64 PrefixValue = 0;                    // Sample prefix value
    };
    std::vector<TSelectTarget> SelectTargets;

    double TotalRecall = 0.0;
    size_t ProcessedTargets = 0;
};

} // namespace NYdbWorkload
