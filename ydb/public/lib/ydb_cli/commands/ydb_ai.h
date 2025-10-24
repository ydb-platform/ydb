#pragma once

#include "ydb_command.h"

namespace NYdb::NConsoleClient {

class TCommandAi final : public TYdbCommand {
    using TBase = TYdbCommand;

public:
    TCommandAi();

    void Config(TConfig& config) override;

    int Run(TConfig& config) override;
};

} // namespace NYdb::NConsoleClient
