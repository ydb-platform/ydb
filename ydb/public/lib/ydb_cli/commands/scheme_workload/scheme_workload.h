#pragma once

#include <ydb/public/lib/ydb_cli/common/command.h>

namespace NYdb::NConsoleClient {

class TCommandWorkloadScheme : public TClientCommandTree {
public:
    TCommandWorkloadScheme();
};

} // namespace NYdb::NConsoleClient
