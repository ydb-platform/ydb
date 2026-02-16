#pragma once

#include "fulltext_workload_params.h"

#include <ydb/public/lib/ydb_cli/common/command.h>

namespace NYdb::NConsoleClient {

    class TCommandFulltext: public TClientCommandTree {
    public:
        TCommandFulltext();
        virtual void Config(TConfig& config) override;

    private:
        std::unique_ptr<NYdbWorkload::TFulltextWorkloadParams> Params;
    };

} // namespace NYdb::NConsoleClient
