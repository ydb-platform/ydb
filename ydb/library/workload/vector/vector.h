#pragma once

#include "vector_workload_params.h"

#include <ydb/public/lib/ydb_cli/common/command.h>

namespace NYdb::NConsoleClient {

class TCommandVector : public TClientCommandTree {
public:
    TCommandVector();
    virtual void Config(TConfig& config) override;

private:
    std::unique_ptr<NYdbWorkload::TVectorWorkloadParams> Params;
};

} // namespace NYdb::NConsoleClient
