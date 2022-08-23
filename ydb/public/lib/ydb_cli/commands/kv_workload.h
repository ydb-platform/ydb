#pragma once

#include "ydb/public/lib/ydb_cli/commands/ydb_workload.h"

namespace NYdb {
namespace NConsoleClient {

class TCommandKv : public TClientCommandTree {
public:
    TCommandKv();
};

class TCommandKvInit : public TWorkloadCommand {
public:
    TCommandKvInit();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    ui64 InitRowCount;
    ui64 MinPartitions;
    ui64 MaxFirstKey;
    bool PartitionsByLoad;
};

class TCommandKvClean : public TWorkloadCommand {
public:
    TCommandKvClean();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;
};

class TCommandKvRun : public TClientCommandTree {
public:
    TCommandKvRun();
};

class TCommandKvRunUpsertRandom : public TWorkloadCommand {
public:
    TCommandKvRunUpsertRandom();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    ui64 MaxFirstKey;

};

class TCommandKvRunSelectRandom : public TWorkloadCommand {
public:
    TCommandKvRunSelectRandom();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    ui64 MaxFirstKey;

};

}
}
