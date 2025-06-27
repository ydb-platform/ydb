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
    void SampleExistingVectors();

    // Target access methods
    const std::string& GetTargetEmbedding(size_t index) const;
    size_t GetTargetCount() const;
    i64 GetPrefixValue(size_t targetIndex) const;

private:
    const TVectorWorkloadParams& Params;

    struct TSelectTarget {
        std::string EmbeddingBytes;             // Sample targets to use in select workload
        i64 PrefixValue = 0;                    // Sample prefix value
    };
    std::vector<TSelectTarget> SelectTargets;
};

} // namespace NYdbWorkload