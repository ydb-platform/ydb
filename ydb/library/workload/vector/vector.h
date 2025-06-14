#pragma once

#include <ydb/library/workload/abstract/workload_query_generator.h>

#include <cctype>
#include <mutex>
#include <vector>
#include <string>
#include <atomic>

namespace NYdbWorkload {

class TVectorRecallEvaluator {
public:
    TVectorRecallEvaluator();
    ~TVectorRecallEvaluator();
    
    // Core functionality for vector sampling and etalon generation
    void SampleExistingVectors(const char* tableName, size_t targetCount, NYdb::NQuery::TQueryClient& queryClient);
    void FillEtalons(const char* tableName, const char* indexPrefixColumn, size_t topK, NYdb::NQuery::TQueryClient& queryClient);
    
    // Target access methods
    const std::string& GetTargetEmbedding(size_t index) const;
    const std::vector<ui64>& GetTargetEtalons(size_t index) const;
    size_t GetTargetCount() const;
    
    // Recall metrics methods
    void AddRecall(double recall);
    double GetAverageRecall() const;
    double GetTotalRecall() const;
    size_t GetProcessedTargets() const;
    
    // Result processing method
    void ProcessQueryResult(const NYdb::NQuery::TExecuteQueryResult& result, size_t targetIndex, bool verbose);

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
    std::optional<std::string> IndexPrefixColumn;
    TString Distance;
    TString VectorType;
    size_t KmeansTreeLevels = 0;
    size_t KmeansTreeClusters = 0;
    size_t VectorDimension = 0;
    size_t Targets = 0;
    size_t VectorInitCount = 0;
    size_t KmeansTreeSearchClusters = 0;
    size_t TopK = 0;
    bool Recall;
private:
    size_t GetVectorDimension() const;
    std::optional<std::string> GetIndexPrefixColumn() const;

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

    // Callback that uses the decoupled TVectorRecallEvaluator
    void RecallCallback(NYdb::NQuery::TExecuteQueryResult queryResult, size_t targetIndex);
    
    // Helper method to get next target index
    size_t GetNextTargetIndex(size_t currentIndex) const;

    // Using atomic for thread safety
    static thread_local std::atomic<size_t> ThreadLocalTargetIndex;
};

} // namespace NYdbWorkload
