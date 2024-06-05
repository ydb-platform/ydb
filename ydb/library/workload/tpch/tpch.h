#pragma once

#include <ydb/library/workload/benchmark_base/workload.h>
#include <util/folder/path.h>

namespace NYdbWorkload {

class TTpchWorkloadParams final: public TWorkloadBaseParams {
public:
    void ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) override;
    THolder<IWorkloadQueryGenerator> CreateGenerator() const override;
    TString GetWorkloadName() const override;
    YDB_READONLY(ui64, Scale, 1);
    YDB_READONLY_DEF(TSet<TString>, Tables);
    YDB_READONLY(ui32, ProcessIndex, 0);
    YDB_READONLY(ui32, ProcessCount, 1);
    YDB_READONLY_DEF(TFsPath, ExternalQueriesDir);
};

class TTpchWorkloadGenerator final: public TWorkloadGeneratorBase {
public:
    explicit TTpchWorkloadGenerator(const TTpchWorkloadParams& params);
    TQueryInfoList GetWorkload(int type) override;
    TVector<TWorkloadType> GetSupportedWorkloadTypes() const override;

    class TBulkDataGenerator;
protected:
    TString DoGetDDLQueries() const override;
    TQueryInfoList GetInitialData() override;

private:
    const TTpchWorkloadParams& Params;
};

} // namespace NYdbWorkload
