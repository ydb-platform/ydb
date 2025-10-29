#pragma once

#include "ydb_command.h"
#include "ydb_common.h"
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_bridge.h>
#include <ydb/public/lib/ydb_cli/common/format.h>

#include <util/generic/string.h>
#include <vector>

namespace NYdb::NConsoleClient {

class TCommandClusterState : public TClientCommandTree {
public:
    TCommandClusterState();
};

class TCommandClusterStateFetch : public TYdbReadOnlyCommand, public TCommandWithOutput {
public:
    TCommandClusterStateFetch();
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    ui32 DurationSeconds = 0;
    ui32 PeriodSeconds = 0;
};

}
