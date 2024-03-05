#pragma once

#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>
#include <ydb/public/sdk/cpp/client/ydb_query/client.h>
#include <ydb/library/workload/workload_query_generator.h>

#include <library/cpp/histogram/hdr/histogram.h>
#include <util/datetime/base.h>
#include <util/system/spinlock.h>

#include <memory>
#include <string>

namespace NYdb {
namespace NConsoleClient {

class TCommandWorkload : public TClientCommandTree {
public:
    TCommandWorkload();
};

class TWorkloadCommand : public TYdbCommand {
public:
    TWorkloadCommand(
        const TString& name,
        const std::initializer_list<TString>& aliases = std::initializer_list<TString>(),
        const TString& description = TString()
    );

    virtual void Config(TConfig& config) override;
    NTable::TSession GetSession();

protected:
    void PrepareForRun(TConfig& config);

    int RunWorkload(NYdbWorkload::IWorkloadQueryGenerator& workloadGen, const int type);
    void WorkerFn(int taskId, NYdbWorkload::IWorkloadQueryGenerator& workloadGen, const int type);
    void PrintWindowStats(int windowIt);

    std::unique_ptr<NYdb::TDriver> Driver;
    std::unique_ptr<NTable::TTableClient> TableClient;
    std::unique_ptr<NQuery::TQueryClient> QueryClient;

    size_t TotalSec;
    size_t Threads;
    ui64 Rate;
    unsigned int ClientTimeoutMs;
    unsigned int OperationTimeoutMs;
    unsigned int CancelAfterTimeoutMs;
    unsigned int WindowSec;
    bool Quiet;
    bool PrintTimestamp;
    TString QueryExecuterType;

    TInstant StartTime;
    TInstant StopTime;

    // Think about moving histograms to workload library.
    // Histograms will also be useful in actor system workload.
    TSpinLock HdrLock;
    NHdr::THistogram WindowHist;
    NHdr::THistogram TotalHist;

    std::atomic_uint64_t TotalQueries;
    std::atomic_uint64_t TotalRetries;
    std::atomic_uint64_t WindowRetryCount;
    std::atomic_uint64_t TotalErrors;
    std::atomic_uint64_t WindowErrors;

protected:
    int InitTables(NYdbWorkload::IWorkloadQueryGenerator& workloadGen);
    int CleanTables(const NYdbWorkload::IWorkloadQueryGenerator& workloadGen, TConfig& config);
};

class TWorkloadCommandBase: public TWorkloadCommand {
public:
    TWorkloadCommandBase(
        const TString& name,
        const TString& key,
        const NYdbWorkload::TWorkloadParams::ECommandType commandType,
        const TString& description = TString());
    virtual void Config(TConfig& config) override;

protected:
    NYdbWorkload::TWorkloadParams::ECommandType CommandType;
    THolder<NYdbWorkload::TWorkloadParams> Params;
    int Type = 0;
};

class TWorkloadCommandInit : public TWorkloadCommandBase {
public:
    TWorkloadCommandInit(const TString& key);
    virtual void Config(TConfig& config) override;
    virtual int Run(TConfig& config) override;
};

class TWorkloadCommandRun : public TWorkloadCommandBase {
public:
    TWorkloadCommandRun(const TString& key, const NYdbWorkload::IWorkloadQueryGenerator::TWorkloadType& workload);
    virtual int Run(TConfig& config) override;
};

class TWorkloadCommandClean : public TWorkloadCommandBase {
public:
    TWorkloadCommandClean(const TString& key);
    virtual int Run(TConfig& config) override;
};

class TWorkloadCommandRoot : public TClientCommandTree {
public:
    TWorkloadCommandRoot(const TString& key);
};

}
}
