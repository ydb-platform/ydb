#pragma once

#include <ydb/library/workload/tpc_base/tpc_base.h>
#include <util/folder/path.h>

namespace NYdbWorkload {

class TTpcdsWorkloadParams final: public TTpcBaseWorkloadParams {
public:
    void ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) override;
    THolder<IWorkloadQueryGenerator> CreateGenerator() const override;
    TString GetWorkloadName() const override;
//    TWorkloadDataInitializer::TList CreateDataInitializers() const override;
    YDB_READONLY(ui64, Scale, 1);
    YDB_READONLY_DEF(TSet<TString>, Tables);
    YDB_READONLY(ui32, ProcessIndex, 0);
    YDB_READONLY(ui32, ProcessCount, 1);
    YDB_READONLY_DEF(TFsPath, ExternalQueriesDir);
};

class TTpcdsWorkloadGenerator final: public TTpcBaseWorkloadGenerator {
public:
    explicit TTpcdsWorkloadGenerator(const TTpcdsWorkloadParams& params);
    TQueryInfoList GetWorkload(int type) override;
    TVector<TWorkloadType> GetSupportedWorkloadTypes() const override;

protected:
    TString DoGetDDLQueries() const override;
    TQueryInfoList GetInitialData() override;

private:
    const TTpcdsWorkloadParams& Params;
};

} // namespace NYdbWorkload
