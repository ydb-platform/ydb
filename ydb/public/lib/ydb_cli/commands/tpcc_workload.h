#pragma once

#include <ydb/library/workload/tpcc/tpcc_config.h>
#include <ydb/library/workload/tpcc/tpcc_workload.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>

#include <library/cpp/histogram/hdr/histogram.h>
#include <util/datetime/base.h>
#include <util/system/spinlock.h>

#include <memory>
#include <string>

namespace NYdb {
namespace NConsoleClient {

using namespace NYdbWorkload;
using namespace NTPCC;

class TCommandTPCCWorkload : public TClientCommandTree {
public:
    TCommandTPCCWorkload();
};

class TTPCCWorkloadCommand : public TYdbCommand {
public:
    TTPCCWorkloadCommand(
        const TString& name,
        const std::initializer_list<TString>& aliases = std::initializer_list<TString>(),
        const TString& description = TString()
    );

    void Config(TConfig& config) override;
    NTable::TSession GetSession();

protected:
    void PrepareForRun(TConfig& config);

    std::shared_ptr<NYdb::TDriver> Driver;
    std::unique_ptr<NTable::TTableClient> TableClient;

    TTPCCWorkloadParams Params;
    std::unique_ptr<TTPCCWorkload> Workload;
    unsigned int ClientTimeoutMs;
    unsigned int OperationTimeoutMs;
    unsigned int CancelAfterTimeoutMs;
    unsigned int WindowDurationSec;
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

class TTPCCInitCommand : public TTPCCWorkloadCommand {
public:
    TTPCCInitCommand();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;
};

class TTPCCCleanCommand : public TTPCCWorkloadCommand {
public:
    TTPCCCleanCommand();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;
};

}

}
