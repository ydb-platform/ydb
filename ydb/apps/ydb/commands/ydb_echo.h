#pragma once

#include <ydb/public/lib/ydb_cli/common/command.h>

namespace NYdb::NConsoleClient {

class TCommandEcho final : public TClientCommand {
public:
    TCommandEcho();

    void Config(TConfig& config) override;
    int Run(TConfig& config) override;
};

} // namespace NYdb::NConsoleClient
