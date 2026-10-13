#pragma once

#include "configure_opts.h"

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
    TWorkloadDataInitializer::TList CreateDataInitializers() const override;
    TString GetWorkloadName() const override;
    void Validate(const ECommandType commandType, int workloadType) override;

    void Init() override;

    void ConfigureCommonOpts(NLastGetopt::TOpts& opts);
    void ConfigureIndexOpts(NLastGetopt::TOpts& opts);

    TVector<TString> GetColumns() const;
    TString GetDistanceDDL() const;
    TString GetIndexTypeDDL() const;
    TString GetHnswSettingsDDL() const;

    NVector::TTableOpts TableOpts;
    NVector::TTablePartitioningOpts TablePartitioningOpts;
    NVector::TVectorOpts VectorOpts;

    TString IndexName = "index";

    TString IndexType = "KmeansTree";
    ui64 MinRows = 10000;
    ui32 M = 16;
    ui32 EfConstruction = 200;
    ui32 HnswEfSearch = 15;
    ui64 DeltaRows = 10000;
    bool KmeansTreeCovering = false;
    bool KmeansTreePrefixed = false;

    size_t UpsertBulkSize = 100;
    bool UpsertPrefixed = false;
    size_t UpsertPrefixCount = 1000;

    int RunWorkloadType = -1;

    TString QueryTableName;
    std::vector<std::string> KeyColumns;
    std::string EmbeddingColumn;
    std::vector<std::string> QueryTableKeyColumns;
    std::optional<std::string> PrefixColumn;
    std::optional<std::string> PrefixType;
    NYdb::NTable::TVectorIndexSettings::EMetric Metric;
    TString Distance;
    size_t KmeansTreeLevels = 0;
    size_t KmeansTreeClusters = 0;
    size_t Targets = 0;
    size_t VectorInitCount = 0;
    size_t KmeansTreeSearchClusters = 0;
    size_t Limit = 0;
    size_t RecallThreads = 0;
    ui64 TableRowCount = 0;
    bool Recall = false;
    bool NonIndexedSearch = false;
    bool StaleRO = false;
    bool Hnsw = false;
    bool KeyIsInt = false;
};


} // namespace NYdbWorkload
