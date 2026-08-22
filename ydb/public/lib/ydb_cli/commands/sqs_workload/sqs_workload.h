#pragma once

#include <ydb/public/lib/ydb_cli/common/command.h>

namespace NYdb::NConsoleClient {
    class TCommandWorkloadSqs: public TClientCommandTree {
    public:
        TCommandWorkloadSqs();
    };
} // namespace NYdb::NConsoleClient
