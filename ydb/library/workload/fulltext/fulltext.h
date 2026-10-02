#pragma once

#include "fulltext_workload_params.h"

#include <ydb/public/lib/ydb_cli/common/command.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_workload.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_workload_import.h>

namespace NYdb::NConsoleClient {

    class TFulltextRunTree final: public TClientCommandTree {
    public:
        TFulltextRunTree(NYdbWorkload::TFulltextWorkloadParams& params);
        virtual void Config(TConfig& config) override;

    private:
        NYdbWorkload::TFulltextWorkloadParams& Params;
    };

    class TFulltextRunCommand final: public TWorkloadCommand {
    public:
        TFulltextRunCommand(
            NYdbWorkload::TFulltextWorkloadParams& params,
            const NYdbWorkload::IWorkloadQueryGenerator::TWorkloadType& workload);
        virtual void Config(TConfig& config) override;
        virtual int Run(TConfig& config) override;

    private:
        NYdbWorkload::TFulltextWorkloadParams& Params;
        int Type = 0;
    };

    class TCommandFulltext: public TClientCommandTree {
    public:
        TCommandFulltext();
        virtual void Config(TConfig& config) override;

    private:
        std::unique_ptr<NYdbWorkload::TFulltextWorkloadParams> Params;
    };

} // namespace NYdb::NConsoleClient
