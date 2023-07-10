#pragma once

#include <ydb/public/lib/ydb_cli/common/command.h>

namespace NYdb::NConsoleClient {

class TCommandWorkloadTransfer : public TClientCommandTree {
public:
    TCommandWorkloadTransfer();
};

}
