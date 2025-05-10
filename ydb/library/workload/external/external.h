#pragma once

#include <ydb/library/workload/benchmark_base/workload.h>

namespace NYdbWorkload {

namespace NExternal {

class TExternalWorkloadParams final : public TWorkloadBaseParams {
public:
    void ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) override;
    THolder<IWorkloadQueryGenerator> CreateGenerator() const override;
    TWorkloadDataInitializer::TList CreateDataInitializers() const override;
    TString GetWorkloadName() const override;

    YDB_READONLY_DEF(TFsPath, DataPath);
    YDB_READONLY(EQuerySyntax, Syntax, EQuerySyntax::YQL);
};

class TExternalGenerator final: public TWorkloadGeneratorBase {
public:
    explicit TExternalGenerator(const TExternalWorkloadParams& params);
    TQueryInfoList GetWorkload(int type) override;
    TVector<TWorkloadType> GetSupportedWorkloadTypes() const override;

protected:
    TString GetTablesYaml() const override;
    TWorkloadGeneratorBase::TSpecialDataTypes GetSpecialDataTypes() const override;
    TQueryInfoList GetInitialData() override;

private:
    TQueryInfoList GetWorkloadFromDir(const TFsPath& dir) const;
    const TExternalWorkloadParams& Params;
};

} // namespace NLog

} // namespace NYdbWorkload
