#pragma once

#include <ydb/library/workload/abstract/workload_query_generator.h>

#include <cctype>

#include <random>

namespace NYdbWorkload {

class TVectorRecallEvaluator {
public:
    TVectorRecallEvaluator();
    ~TVectorRecallEvaluator();
    
    void SampleExistingVectors(const char* tableName, size_t targetCount, NYdb::NQuery::TQueryClient& queryClient);
    void FillEtalons(const char* tableName, size_t topK, NYdb::NQuery::TQueryClient& queryClient);
    const std::string& GetTargetEmbedding(size_t index) const;
    const std::vector<ui64>& GetTargetEtalons(size_t index) const;
    void AddRecall(double recall);
    double GetAverageRecall() const;
    size_t GetTargetCount() const;
private:
    struct TSelectTarget {
        std::string EmbeddingBytes;             // Sample targets to use in select workload
        std::vector<ui64> Etalons;              // Etalon vector Ids for recall measurement
    };
    std::vector<TSelectTarget> SelectTargets;

    double TotalRecall = 0.0;
    size_t ProcessedTargets = 0;
    
    mutable std::mutex Mutex;
};

class TVectorWorkloadGenerator;

class TVectorWorkloadParams final: public TWorkloadParams {
    friend class TVectorWorkloadGenerator;
public:
    void ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) override;
    THolder<IWorkloadQueryGenerator> CreateGenerator() const override;
    TString GetWorkloadName() const override;
    void Validate(const ECommandType commandType, int workloadType) override;

    TString TableName;
    TString IndexName;
    TString Distance;
    TString VectorType;
    size_t KmeansTreeLevels = 0;
    size_t KmeansTreeClusters = 0;
    size_t VectorDimension = 0;
    size_t Targets = 0;
    size_t VectorInitCount = 0;
    size_t KmeansTreeSearchClusters = 0;
    size_t TopK = 0;
private:
    size_t GetVectorDimension() const;

    THolder<TVectorRecallEvaluator> VectorRecallEvaluator;
};

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

    void RecallCallback(NYdb::NQuery::TExecuteQueryResult queryResult, size_t targetIndex);

    static thread_local std::optional<size_t> ThreadLocalTargetIndex;
};

} // namespace NYdbWorkload
