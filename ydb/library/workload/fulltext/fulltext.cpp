#include "fulltext.h"
#include "markov_model_builder.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_workload.h>

namespace NYdb::NConsoleClient {

    TFulltextRunTree::TFulltextRunTree(NYdbWorkload::TFulltextWorkloadParams& params)
        : TClientCommandTree("run", {}, "Run YDB fulltext workload")
        , Params(params)
    {}

    void TFulltextRunTree::Config(TConfig& config) {
        TClientCommandTree::Config(config);

        Params.ConfigureOpts(config.Opts->GetOpts(), NYdbWorkload::TWorkloadParams::ECommandType::Run, -1);
    }

    TFulltextRunCommand::TFulltextRunCommand(
        NYdbWorkload::TFulltextWorkloadParams& params,
        const NYdbWorkload::IWorkloadQueryGenerator::TWorkloadType& workload)
        : TWorkloadCommand(workload.CommandName, std::initializer_list<TString>(), workload.Description)
        , Params(params)
        , Type(workload.Type)
    {
        Aliases = workload.Aliases;
    }

    void TFulltextRunCommand::Config(TConfig& config) {
        TWorkloadCommand::Config(config);
        config.Opts->SetFreeArgsNum(0);
        Params.ConfigureOpts(config.Opts->GetOpts(), NYdbWorkload::TWorkloadParams::ECommandType::Run, Type);
    }

    int TFulltextRunCommand::Run(TConfig& config) {
        PrepareForRun(config);
        Params.SetClients(QueryClient.get(), nullptr, TableClient.get(), nullptr);
        Params.DbPath = config.Database;
        Params.Verbose = config.IsVerbose();
        auto workloadGen = Params.CreateGenerator();
        Params.Validate(NYdbWorkload::TWorkloadParams::ECommandType::Run, Type);
        Params.Init();
        workloadGen->Init();
        return RunWorkload(*workloadGen, Type);
    }

    TCommandFulltext::TCommandFulltext()
        : TClientCommandTree("fulltext", {}, "YDB fulltext workload")
        , Params(std::make_unique<NYdbWorkload::TFulltextWorkloadParams>())
    {
        AddCommand(std::make_unique<TWorkloadCommandInit>(*Params));
        if (auto importCmd = TWorkloadCommandImport::Create(*Params)) {
            AddCommand(std::move(importCmd));
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
                auto run = std::make_unique<TFulltextRunTree>(*Params);
                for (const auto& type : supportedWorkloads) {
                    run->AddCommand(std::make_unique<TFulltextRunCommand>(*Params, type));
                }
                AddCommand(std::move(run));
                break;
            }
        }
        AddCommand(std::make_unique<TWorkloadCommandClean>(*Params));
        AddCommand(std::make_unique<TMarkovModelBuilder>());
    }

    void TCommandFulltext::Config(TConfig& config) {
        TClientCommandTree::Config(config);
        Params->ConfigureOpts(config.Opts->GetOpts(), NYdbWorkload::TWorkloadParams::ECommandType::Root, 0);
    }

} // namespace NYdb::NConsoleClient
