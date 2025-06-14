#pragma once

#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>

#include <memory>

namespace NYdb::NTPCC {
struct TRunConfig;
} // NYdb::NTPCC

namespace NYdb::NConsoleClient {

class TCommandTPCC : public TClientCommandTree
{
public:
    TCommandTPCC();

    virtual void Config(TConfig& config) override;

private:
    std::shared_ptr<NTPCC::TRunConfig> RunConfig;
};

} // namespace NYdb::NConsoleClient
