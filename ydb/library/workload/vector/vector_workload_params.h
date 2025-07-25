#pragma once

#include <ydb/library/workload/benchmark_base/workload.h>
#include <ydb/library/workload/abstract/workload_query_generator.h>

#include <cctype>
#include <mutex>
#include <vector>
#include <string>
#include <atomic>



namespace NYdbWorkload {

class TVectorWorkloadParams final: public TWorkloadBaseParams {
    friend class TVectorWorkloadGenerator;
public:
    void ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) override;
    THolder<IWorkloadQueryGenerator> CreateGenerator() const override;
    TString GetWorkloadName() const override;
    void Validate(const ECommandType commandType, int workloadType) override;

    void Init() override;

    TString TableName;
    TString QueryTableName;
    TString IndexName;
    std::string KeyColumn;
    std::string EmbeddingColumn;
    std::string QueryTableKeyColumn;
    std::optional<std::string> PrefixColumn;
    std::optional<std::string> PrefixType;
    NYdb::NTable::TVectorIndexSettings::EMetric Metric;
    TString Distance;
    TString VectorType;
    size_t KmeansTreeLevels = 0;
    size_t KmeansTreeClusters = 0;
    size_t VectorDimension = 0;
    size_t Targets = 0;
    size_t VectorInitCount = 0;
    size_t KmeansTreeSearchClusters = 0;
    size_t Limit = 0;
    size_t RecallThreads = 0;
    ui64 TableRowCount = 0;
    bool Recall = false;
    bool NonIndexedSearch = false;
    bool KeyIsInt = false;
};


} // namespace NYdbWorkload
