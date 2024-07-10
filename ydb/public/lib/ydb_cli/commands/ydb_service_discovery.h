#pragma once

#include <ydb/public/lib/ydb_cli/commands/ydb_common.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>

#include <ydb/public/sdk/cpp/client/ydb_discovery/discovery.h>

namespace NYdb {
namespace NConsoleClient {

class TCommandDiscovery : public TClientCommandTree {
public:
    TCommandDiscovery();
};

class TCommandListEndpoints : public TYdbSimpleCommand {
public:
    TCommandListEndpoints();
    void Config(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    void PrintResponse(NDiscovery::TListEndpointsResult& result);
};

class TCommandWhoAmI : public TYdbSimpleCommand {
public:
    TCommandWhoAmI();
    void Config(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    void PrintResponse(NDiscovery::TWhoAmIResult& result);

    bool WithGroups = false;
};

}
}
