#pragma once

#include <ydb/library/workload/abstract/workload_query_generator.h>

#include <cctype>

#include <random>

namespace NYdbWorkload {

using TFloatVector = std::vector<float>;

class TVectorRecallEvaluator {
public:
    TVectorRecallEvaluator(size_t vectorDimension, size_t targetCount);
    ~TVectorRecallEvaluator();
    
    void PregenerateSelectEmbeddings(size_t vectorDimension, size_t vectorCount);
    const TFloatVector& GetRandomSelectEmbedding();
    void FillEtalons(const char* tableName, size_t topK, NYdb::NQuery::TQueryClient& queryClient);
    const TFloatVector& GetTargetEmbedding(size_t index) const;
    const std::vector<ui64>& GetTargetEtalons(size_t index) const;
    void AddRecall(double recall);
    double GetAverageRecall() const;
    size_t GetTargetCount() const;
private:
    std::random_device Rd;
    std::mt19937_64 Gen;
    std::normal_distribution<float> VectorValueGenerator;
    std::uniform_int_distribution<size_t> PregeneratedIndexGenerator;

    struct TSelectTarget {
        TFloatVector Target;                    // Random target to use in select workload
        std::vector<ui64> Etalons;              // Etalon vector Ids for recall measurement
    };
    std::vector<TSelectTarget> SelectTargets;

    double TotalRecall = 0.0;
    size_t ProcessedTargets = 0;
};

class TVectorWorkloadGenerator;

class TVectorWorkloadParams final: public TWorkloadParams {
    friend class TVectorWorkloadGenerator;
public:
    void ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) override;
    THolder<IWorkloadQueryGenerator> CreateGenerator() const override;
    TString GetWorkloadName() const override;
    void Validate(const ECommandType commandType, int workloadType) override;

    const TFloatVector& GetRandomSelectEmbedding() const;
    
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

    void RecallCallback(NYdb::NQuery::TExecuteQueryResult);

    TQueryInfo SelectImpl(const std::string& query);
    TQueryInfo SelectScanImpl();
    TQueryInfo SelectIndexImpl();
    TQueryInfo SelectPrefixIndexImpl();

    size_t CurrentTargetIndex = 0;
};

} // namespace NYdbWorkload
