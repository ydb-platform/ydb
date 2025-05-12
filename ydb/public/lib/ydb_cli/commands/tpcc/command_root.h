#pragma once

#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>
#include <ydb/public/lib/ydb_cli/common/interruptible.h>

namespace NYdb::NConsoleClient {

class TCommandTPCC : public TClientCommandTree
{
public:
    TCommandTPCC();
};

} // namespace NYdb::NConsoleClient
