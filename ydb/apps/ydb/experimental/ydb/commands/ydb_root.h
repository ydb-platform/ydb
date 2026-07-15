#pragma once

#include <ydb/apps/ydb/commands/ydb_root.h>

namespace NYdb {
namespace NConsoleClient {

class TClientCommandInternalRoot : public TClientCommandRoot
{
public:
    TClientCommandInternalRoot(const TString& name, const TClientSettings& settings);
    void Config(TConfig& config) override;
};

int NewInternalClient(int argc, char** argv);

} // NConsoleClient
} // NYdb
