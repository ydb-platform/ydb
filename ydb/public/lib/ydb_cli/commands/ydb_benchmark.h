#include "ydb_workload.h"

namespace NYdb::NConsoleClient {

class TWorkloadCommandBenchmark final: public TWorkloadCommandBase {
public:
    TWorkloadCommandBenchmark(NYdbWorkload::TWorkloadParams& params, const NYdbWorkload::IWorkloadQueryGenerator::TWorkloadType& workload);
    virtual void Config(TConfig& config) override;

protected:
    int DoRun(NYdbWorkload::IWorkloadQueryGenerator& workloadGen, TConfig& config) override;

private:
    TString PatchQuery(const TStringBuf& original) const;
    bool NeedRun(const ui32 queryIdx) const;

    template <typename TClient>
    bool RunBench(TClient& client, NYdbWorkload::IWorkloadQueryGenerator& workloadGen);

private:
    TString QueryExecuterType;
    TString OutFilePath;
    ui32 IterationsCount;
    TString JsonReportFileName;
    TString CsvReportFileName;
    TString MiniStatFileName;
    TString PlanFileName;
    TSet<ui32> QueriesToRun;
    TSet<ui32> QueriesToSkip;
    TVector<TString> QuerySettings;
    ui32 VerboseLevel = 0;
};

}