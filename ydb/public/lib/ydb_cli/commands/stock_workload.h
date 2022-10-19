#pragma once

#include "ydb/public/lib/ydb_cli/commands/ydb_workload.h"

namespace NYdb {
namespace NConsoleClient {

class TCommandStock : public TClientCommandTree {
public:
    TCommandStock();
};

class TCommandStockInit : public TWorkloadCommand {
public:
    TCommandStockInit();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    size_t ProductCount;
    size_t Quantity;
    size_t OrderCount;
    unsigned int MinPartitions;
    bool PartitionsByLoad;
};

class TCommandStockClean : public TWorkloadCommand {
public:
    TCommandStockClean();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

};

class TCommandStockRun : public TClientCommandTree {
public:
    TCommandStockRun();
};

class TCommandStockRunInsertRandomOrder : public TWorkloadCommand {
public:
    TCommandStockRunInsertRandomOrder();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    size_t ProductCount;
};

class TCommandStockRunSubmitRandomOrder : public TWorkloadCommand {
public:
    TCommandStockRunSubmitRandomOrder();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    size_t ProductCount;
};

class TCommandStockRunSubmitSameOrder : public TWorkloadCommand {
public:
    TCommandStockRunSubmitSameOrder();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    size_t ProductCount;
};

class TCommandStockRunGetRandomCustomerHistory : public TWorkloadCommand {
public:
    TCommandStockRunGetRandomCustomerHistory();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    unsigned int Limit;
};

class TCommandStockRunGetCustomerHistory : public TWorkloadCommand {
public:
    TCommandStockRunGetCustomerHistory();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    unsigned int Limit;
};

}
}
