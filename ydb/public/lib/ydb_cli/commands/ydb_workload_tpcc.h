#pragma once

#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>

namespace NYdb::NConsoleClient {

class TCommandTPCC : public TClientCommandTree
{
public:
    TCommandTPCC();
};

} // namespace NYdb::NConsoleClient
