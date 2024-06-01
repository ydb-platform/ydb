#pragma once

#include <ydb/library/workload/tpcc/config.h>
#include <ydb/library/workload/tpcc/workload.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>

#include <library/cpp/histogram/hdr/histogram.h>
#include <util/datetime/base.h>
#include <util/system/spinlock.h>

#include <memory>

using namespace NYdbWorkload;
using namespace NTPCC;

namespace NYdb {
namespace NConsoleClient {

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
    void PrepareForRun(TConfig& config, i32 threadsNum);

    std::shared_ptr<NYdb::TDriver> Driver;
    std::unique_ptr<NTable::TTableClient> TableClient;

    TLoadParams LoadParams;
    TRunParams RunParams;
    std::unique_ptr<TTPCCWorkload> Workload;
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


class TTPCCRunCommand : public TTPCCWorkloadCommand {
public:
    TTPCCRunCommand();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;
};


}

}
