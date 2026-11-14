#pragma once

#include "ydb_command.h"

namespace NYdb::NConsoleClient {

class TCommandListObjects : public TYdbCommand, public TCommandWithPath {
public:
    TCommandListObjects();
    void Config(TConfig& config) override;
    void ExtractParams(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    bool IncludeIndexData = false;
    TString OutputFile;
};

} // namespace NYdb::NConsoleClient
