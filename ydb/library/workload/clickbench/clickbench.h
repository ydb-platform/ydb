#pragma once

#include <ydb/library/workload/benchmark_base/workload.h>
#include <util/folder/path.h>

namespace NYdbWorkload {

class TClickbenchWorkloadParams final: public TWorkloadBaseParams {
public:
    TClickbenchWorkloadParams();
    void ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) override;
    THolder<IWorkloadQueryGenerator> CreateGenerator() const override;
    TWorkloadDataInitializer::TList CreateDataInitializers() const override;
    TString GetWorkloadName() const override;
    YDB_READONLY_DEF(TFsPath, DataFiles);
    YDB_READONLY(EQuerySyntax, Syntax, EQuerySyntax::YQL);
};

class TClickbenchWorkloadGenerator final: public TWorkloadGeneratorBase {
public:
    explicit TClickbenchWorkloadGenerator(const TClickbenchWorkloadParams& params);
    TQueryInfoList GetWorkload(int type) override;
    TVector<TWorkloadType> GetSupportedWorkloadTypes() const override;

    class TBulkDataGenerator;
protected:
    TString GetTablesYaml() const override;
    TWorkloadGeneratorBase::TSpecialDataTypes GetSpecialDataTypes() const override;
    TQueryInfoList GetInitialData() override;

private:
    class TDataGenerartor;
    const TClickbenchWorkloadParams& Params;
};

} // namespace NYdbWorkload
