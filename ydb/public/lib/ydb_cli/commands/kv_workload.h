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
    ui64 PartitionSize;
    ui64 MaxFirstKey;
    ui64 StringLen;
    ui64 ColumnsCnt;
    ui64 IntColumnsCnt;
    ui64 KeyColumnsCnt;
    ui64 RowsCnt;
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
    ui64 StringLen;
    ui64 ColumnsCnt;
    ui64 IntColumnsCnt;
    ui64 KeyColumnsCnt;
    ui64 RowsCnt;

};

class TCommandKvRunInsertRandom : public TWorkloadCommand {
public:
    TCommandKvRunInsertRandom();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    ui64 MaxFirstKey;
    ui64 StringLen;
    ui64 ColumnsCnt;
    ui64 IntColumnsCnt;
    ui64 KeyColumnsCnt;
    ui64 RowsCnt;

};

class TCommandKvRunSelectRandom : public TWorkloadCommand {
public:
    TCommandKvRunSelectRandom();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    ui64 MaxFirstKey;
    ui64 ColumnsCnt;
    ui64 IntColumnsCnt;
    ui64 KeyColumnsCnt;
    ui64 RowsCnt;

};

class TCommandKvRunReadRowsRandom : public TWorkloadCommand {
public:
    TCommandKvRunReadRowsRandom();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    ui64 MaxFirstKey;
    ui64 ColumnsCnt;
    ui64 IntColumnsCnt;
    ui64 KeyColumnsCnt;
    ui64 RowsCnt;

};

class TCommandKvRunMixed : public TWorkloadCommand {
public:
    TCommandKvRunMixed();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    ui64 MaxFirstKey;
    ui64 StringLen;
    ui64 ColumnsCnt;
    ui64 IntColumnsCnt;
    ui64 KeyColumnsCnt;
    bool ChangePartitionsSize;
    bool DoReadRows;
    bool DoSelect;

};

}
}
