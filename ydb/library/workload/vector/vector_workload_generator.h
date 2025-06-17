#pragma once

#include "vector_workload_params.h"

#include <ydb/library/workload/abstract/workload_query_generator.h>

#include <cctype>
#include <mutex>
#include <vector>
#include <string>
#include <atomic>

namespace NYdbWorkload {

class TVectorWorkloadParams;

class TVectorWorkloadGenerator final: public TWorkloadQueryGeneratorBase<TVectorWorkloadParams> {
public:
    using TBase = TWorkloadQueryGeneratorBase<TVectorWorkloadParams>;
    TVectorWorkloadGenerator(const TVectorWorkloadParams* params);

    std::string GetDDLQueries() const override;
    TQueryInfoList GetInitialData() override;
    TVector<std::string> GetCleanPaths() const override;
    TQueryInfoList GetWorkload(int type) override;
    TVector<TWorkloadType> GetSupportedWorkloadTypes() const override;
    
    enum class EType {
        Upsert,
        Select
    };

private:
    TQueryInfoList Upsert();
    TQueryInfoList Select();

    // Callback that uses the decoupled TVectorRecallEvaluator
    void RecallCallback(NYdb::NQuery::TExecuteQueryResult queryResult, size_t targetIndex);
    
    // Helper method to get next target index
    size_t GetNextTargetIndex(size_t currentIndex) const;

    // Using atomic for thread safety
    static thread_local std::atomic<size_t> ThreadLocalTargetIndex;
};

} // namespace NYdbWorkload
