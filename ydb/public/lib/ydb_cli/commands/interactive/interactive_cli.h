#pragma once

#include <util/generic/string.h>

#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>
#include <ydb/public/lib/ydb_cli/common/command.h>

namespace NYdb::NConsoleClient {

class TInteractiveCLI {
public:
    TInteractiveCLI(std::string prompt);

    int Run(TClientCommand::TConfig& config);

private:
    std::string Prompt;
};

} // namespace NYdb::NConsoleClient
