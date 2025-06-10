#pragma once

#include <ydb/library/workload/abstract/workload_query_generator.h>

#include <cctype>

#include <random>

namespace NYdbWorkload {

class TVectorGenerator {
public:
    TVectorGenerator(size_t vectorDimension, size_t vectorCount);
    
    void PregenerateSelectEmbeddings(size_t vectorDimension, size_t vectorCount);
    const std::vector<float>& GetRandomSelectEmbedding();
private:
    std::random_device Rd;
    std::mt19937_64 Gen;
    std::normal_distribution<float> VectorValueGenerator;
    std::uniform_int_distribution<size_t> PregeneratedIndexGenerator;
    std::vector<std::vector<float>> SelectEmbeddings;
};

class TVectorWorkloadParams final: public TWorkloadParams {
public:
    void ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) override;
    THolder<IWorkloadQueryGenerator> CreateGenerator() const override;
    TString GetWorkloadName() const override;
    void Validate(const ECommandType commandType, int workloadType) override;

    const std::vector<float>& GetRandomSelectEmbedding() const;
    
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
    size_t GetVectorDimension();

    THolder<TVectorGenerator> VectorGenerator;
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

    TQueryInfo SelectImpl(const std::string& query);
    TQueryInfo SelectScanImpl();
    TQueryInfo SelectIndexImpl();
    TQueryInfo SelectPrefixIndexImpl();
};

} // namespace NYdbWorkload
