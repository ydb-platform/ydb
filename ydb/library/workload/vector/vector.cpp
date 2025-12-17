#include "vector.h"

#include "vector_command_index.h"
#include "vector_workload_params.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_workload_import.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_workload.h>

namespace NYdb::NConsoleClient {

TCommandVector::TCommandVector()
    : TClientCommandTree("vector", {}, "YDB vector workload. Reference: https://ydb.tech/docs/concepts/vector_search")
    , Params(std::make_unique<NYdbWorkload::TVectorWorkloadParams>())
{
    if (const auto desc = Params->GetDescription(NYdbWorkload::TWorkloadParams::ECommandType::Root, 0)) {
        Description = desc;
    }
    AddCommand(std::make_unique<TWorkloadCommandInit>(*Params));
    if (auto import = TWorkloadCommandImport::Create(*Params)) {
        AddCommand(std::move(import));
    }

    AddCommand(std::make_unique<NYdbWorkload::TWorkloadCommandBuildIndex>(*Params));
    AddCommand(std::make_unique<NYdbWorkload::TWorkloadCommandDropIndex>(*Params));

    auto supportedWorkloads = Params->CreateGenerator()->GetSupportedWorkloadTypes();
    switch (supportedWorkloads.size()) {
    case 0:
        break;
    case 1:
        supportedWorkloads.back().CommandName = "run";
        AddCommand(std::make_unique<TWorkloadCommandRun>(*Params, supportedWorkloads.back()));
        break;
    default: {
        auto run = std::make_unique<TClientCommandTree>("run", std::initializer_list<TString>(), "Run YDB vector workload");
        for (const auto& type: supportedWorkloads) {
            run->AddCommand(std::make_unique<TWorkloadCommandRun>(*Params, type));
        }
        AddCommand(std::move(run));
        break;
    }
    }
    AddCommand(std::make_unique<TWorkloadCommandClean>(*Params));
}

void TCommandVector::Config(TConfig& config) {
    TClientCommandTree::Config(config);
    Params->ConfigureOpts(config.Opts->GetOpts(), NYdbWorkload::TWorkloadParams::ECommandType::Root, 0);
}

} // namespace NYdb::NConsoleClient
