#pragma once

#include "tx_mode.h"
#include "ydb_workload.h"

namespace NYdb::NConsoleClient {

namespace BenchmarkUtils {
    class TQueryBenchmarkResult;
    struct TQueryBenchmarkSettings;
}

class TWorkloadCommandBenchmark final: public TWorkloadCommandBase {
public:
    TWorkloadCommandBenchmark(NYdbWorkload::TWorkloadParams& params, const NYdbWorkload::IWorkloadQueryGenerator::TWorkloadType& workload);
    virtual void Config(TConfig& config) override;

protected:
    int DoRun(NYdbWorkload::IWorkloadQueryGenerator& workloadGen, TConfig& config) override;

private:
    TString PatchQuery(const TStringBuf& original) const;
    bool NeedRun(const TString& queryName) const;

    int RunBench(NYdbWorkload::IWorkloadQueryGenerator& workloadGen);
    void SavePlans(const BenchmarkUtils::TQueryBenchmarkResult& res, TStringBuf queryName, const TStringBuf name) const;
    void PrintResult(const BenchmarkUtils::TQueryBenchmarkResult& res, IOutputStream& out) const;
    BenchmarkUtils::TQueryBenchmarkSettings GetBenchmarkSettings(bool withProgress) const;

private:
    class TIterationExecution;
    TString OutFilePath;
    ETxMode TxMode = ETxMode::SerializableRW;
    ui32 IterationsCount;
    TString JsonReportFileName;
    TString CsvReportFileName;
    TString MiniStatFileName;
    TString PlanFileName;
    TSet<TString> QueriesToRun;
    TSet<TString> QueriesToSkip;
    TVector<TString> QuerySettings;
    ui32 VerboseLevel = 0;
    TDuration GlobalTimeout = TDuration::Zero();
    TDuration RequestTimeout = TDuration::Zero();
    TInstant GlobalDeadline = TInstant::Max();
    NYdb::NRetry::TRetryOperationSettings RetrySettings;
    ui32 Threads = 0;
};

}