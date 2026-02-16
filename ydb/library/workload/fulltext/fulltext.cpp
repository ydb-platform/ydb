#include "fulltext.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_workload_import.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_workload.h>

namespace NYdb::NConsoleClient {

    TCommandFulltext::TCommandFulltext()
        : TClientCommandTree("fulltext", {}, "YDB fulltext workload")
        , Params(std::make_unique<NYdbWorkload::TFulltextWorkloadParams>())
    {
        AddCommand(std::make_unique<TWorkloadCommandInit>(*Params));
        if (auto import = TWorkloadCommandImport::Create(*Params)) {
            AddCommand(std::move(import));
        }

        auto supportedWorkloads = Params->CreateGenerator()->GetSupportedWorkloadTypes();
        switch (supportedWorkloads.size()) {
            case 0:
                break;
            case 1:
                supportedWorkloads.back().CommandName = "run";
                AddCommand(std::make_unique<TWorkloadCommandRun>(*Params, supportedWorkloads.back()));
                break;
            default: {
                auto run = std::make_unique<TClientCommandTree>("run", std::initializer_list<TString>(), "Run YDB fulltext workload");
                for (const auto& type : supportedWorkloads) {
                    run->AddCommand(std::make_unique<TWorkloadCommandRun>(*Params, type));
                }
                AddCommand(std::move(run));
                break;
            }
        }
        AddCommand(std::make_unique<TWorkloadCommandClean>(*Params));
    }

    void TCommandFulltext::Config(TConfig& config) {
        TClientCommandTree::Config(config);
        Params->ConfigureOpts(config.Opts->GetOpts(), NYdbWorkload::TWorkloadParams::ECommandType::Root, 0);
    }

} // namespace NYdb::NConsoleClient
