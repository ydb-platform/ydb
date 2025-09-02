#pragma once

#include "ydb_command.h"

namespace NYdb::NConsoleClient {

class TCommandDebug : public TClientCommandTree {
public:
    TCommandDebug();
};

} // NYdb::NConsoleClient
