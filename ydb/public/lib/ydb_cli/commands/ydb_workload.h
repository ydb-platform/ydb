#pragma once

#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>

#include <library/cpp/histogram/hdr/histogram.h>
#include <util/datetime/base.h>
#include <util/system/spinlock.h>

#include <memory>
#include <string>

namespace NYdbWorkload {
    class IWorkloadQueryGenerator;
}

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
    using TWorkloadQueryGenPtr = std::shared_ptr<NYdbWorkload::IWorkloadQueryGenerator>;

    void PrepareForRun(TConfig& config);

    int RunWorkload(TWorkloadQueryGenPtr workloadGen, const int type);
    void WorkerFn(int taskId, TWorkloadQueryGenPtr workloadGen, const int type);
    void PrintWindowStats(int windowIt);

    static constexpr TDuration WINDOW_DURATION = TDuration::Seconds(1);

    std::unique_ptr<NYdb::TDriver> Driver;
    std::unique_ptr<NTable::TTableClient> TableClient;

    size_t Seconds;
    size_t Threads;
    bool Quiet;
    bool PrintTimestamp;

    TInstant StartTime;
    TInstant StopTime;

    // Think about moving histograms to workload library.
    // Histograms will also be useful in actor system workload.
    TSpinLock HdrLock;
    NHdr::THistogram WindowHist;
    NHdr::THistogram TotalHist;

    std::atomic_uint64_t TotalRetries;
    std::atomic_uint64_t WindowRetryCount;
    std::atomic_uint64_t TotalErrors;
    std::atomic_uint64_t WindowErrors;
};

}
}
