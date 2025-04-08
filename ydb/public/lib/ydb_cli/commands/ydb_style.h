#pragma once

#include "ydb_command.h"

#include <ydb/public/lib/ydb_cli/common/format.h>
#include <ydb/public/lib/ydb_cli/common/parameters.h>

namespace NYdb::NConsoleClient {

    class TCommandStyle
        : public TYdbCommand,
          public TCommandWithOutput,
          public TCommandWithParameters {
    public:
        TCommandStyle();
        void Config(TConfig& config) override;
        void Parse(TConfig& config) override;
        int Run(TConfig& config) override;
    };

} // namespace NYdb::NConsoleClient
