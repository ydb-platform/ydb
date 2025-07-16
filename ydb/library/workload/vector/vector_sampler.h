#pragma once

#include <ydb/library/workload/abstract/workload_query_generator.h>

#include <cctype>
#include <vector>
#include <string>

namespace NYdbWorkload {

class TVectorWorkloadParams;

class TVectorSampler {
public:
    TVectorSampler(const TVectorWorkloadParams& params);

    // Core functionality for vector sampling
    void SelectPredefinedVectors();
    void SampleExistingVectors();

    // Target access methods
    const std::string& GetTargetEmbedding(size_t index) const;
    size_t GetTargetCount() const;
    const NYdb::TValue GetPrefixValue(size_t targetIndex) const;

private:
    const TVectorWorkloadParams& Params;

    struct TSelectTarget {
        std::string EmbeddingBytes;                 // Sample targets to use in select workload
        std::optional<NYdb::TValue> PrefixValue;    // Sample prefix value
    };
    std::vector<TSelectTarget> SelectTargets;

    ui64 SelectOneId(bool min);
};

} // namespace NYdbWorkload