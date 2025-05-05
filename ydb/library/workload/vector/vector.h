#pragma once

#include <ydb/library/workload/abstract/workload_query_generator.h>

#include <cctype>

#include <random>

namespace NYdbWorkload {

class TVectorWorkloadParams final: public TWorkloadParams {
public:
    void ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) override;
    THolder<IWorkloadQueryGenerator> CreateGenerator() const override;
    TString GetWorkloadName() const override;
    TString TableName;
    TString IndexName;
    TString Distance;
    TString VectorType;
    size_t KmeansTreeLevels = 0;
    size_t KmeansTreeClusters = 0;
    size_t VectorDimension = 0;
    size_t VectorSelectCount = 0;
    size_t VectorInitCount = 0;
    size_t MaxId = 0;
    size_t Limit = 0;
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
        SelectScan,
        SelectIndex,
        SelectPrefixIndex
    };

private:
    TQueryInfoList SelectScan();
    TQueryInfoList SelectIndex();
    TQueryInfoList SelectPrefixIndex();

    TQueryInfo SelectScanImpl();
    TQueryInfo SelectIndexImpl();
    TQueryInfo SelectPrefixIndexImpl();

    void PregenerateSelectEmbeddings();

    std::random_device Rd;
    std::mt19937_64 Gen;
    std::exponential_distribution<> RandExpDistrib;
    std::normal_distribution<float> VectorValueGenerator;
    std::uniform_int_distribution<size_t> PregeneratedIndexGenerator;

    std::vector<std::vector<float>> SelectEmbeddings;
};

} // namespace NYdbWorkload
