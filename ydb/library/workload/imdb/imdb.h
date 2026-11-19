#pragma once

#include <ydb/library/workload/benchmark_base/workload.h>

namespace NYdbWorkload {

class TImdbWorkloadParams final: public TWorkloadBaseParams {
public:
    TImdbWorkloadParams();
    THolder<IWorkloadQueryGenerator> CreateGenerator() const override;
    TWorkloadDataInitializer::TList CreateDataInitializers() const override;
    TString GetWorkloadName() const override;
};

class TImdbWorkloadGenerator final: public TWorkloadGeneratorBase {
public:
    explicit TImdbWorkloadGenerator(const TImdbWorkloadParams& params);
    TQueryInfoList GetWorkload(int type) override;
    TVector<TWorkloadType> GetSupportedWorkloadTypes() const override;
    TVector<TString> GetAnalyzeQueries() const;

protected:
    TString GetTablesYaml() const override;
    TSpecialDataTypes GetSpecialDataTypes() const override;
    TQueryInfoList GetInitialData() override;

private:
    const TImdbWorkloadParams& Params;
};

} // namespace NYdbWorkload
