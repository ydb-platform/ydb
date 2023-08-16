#pragma once

#include <util/generic/string.h>

#include <ydb/public/lib/ydb_cli/common/command.h>

namespace NYdb {
namespace NConsoleClient {

class TInteractiveCLI
{
public:
    TInteractiveCLI(TClientCommand::TConfig & config, std::string prompt);

    void Run();

private:
    TClientCommand::TConfig & Config;
    std::string Prompt;
};

}
}
