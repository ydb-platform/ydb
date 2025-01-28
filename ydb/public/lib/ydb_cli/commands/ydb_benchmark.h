#pragma once

#include "ydb_workload.h"

namespace NYdb::NConsoleClient {

namespace BenchmarkUtils {
    class TQueryBenchmarkResult;
    struct TQueryBenchmarkDeadline;
}

class TWorkloadCommandBenchmark final: public TWorkloadCommandBase {
public:
    enum class EQueryExecutor {
        Scan /* "scan" */,
        Generic /* "generic" */
    };

    TWorkloadCommandBenchmark(NYdbWorkload::TWorkloadParams& params, const NYdbWorkload::IWorkloadQueryGenerator::TWorkloadType& workload);
    virtual void Config(TConfig& config) override;

protected:
    int DoRun(NYdbWorkload::IWorkloadQueryGenerator& workloadGen, TConfig& config) override;

private:
    TString PatchQuery(const TStringBuf& original) const;
    bool NeedRun(const ui32 queryIdx) const;

    template <typename TClient>
    int RunBench(TClient* client, NYdbWorkload::IWorkloadQueryGenerator& workloadGen);
    void SavePlans(const BenchmarkUtils::TQueryBenchmarkResult& res, ui32 queryNum, const TStringBuf name) const;
    void PrintResult(const BenchmarkUtils::TQueryBenchmarkResult& res, IOutputStream& out, const std::string& expected) const;
    BenchmarkUtils::TQueryBenchmarkDeadline GetDeadline() const;

private:
    EQueryExecutor QueryExecuterType = EQueryExecutor::Generic;
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
    TDuration GlobalTimeout = TDuration::Zero();
    TDuration RequestTimeout = TDuration::Zero();
    TInstant GlobalDeadline = TInstant::Max();
};

}