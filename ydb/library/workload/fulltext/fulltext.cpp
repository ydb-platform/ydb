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

        config.Opts->AddLongOption('s', "seconds", "Seconds to run workload.")
            .DefaultValue(TotalSec).StoreResult(&TotalSec);
        config.Opts->AddLongOption('t', "threads", "Number of parallel threads in workload.")
            .DefaultValue(Threads).StoreResult(&Threads);
        config.Opts->AddLongOption("quiet", "Quiet mode. Doesn't print statistics each second.")
            .StoreTrue(&Quiet);
        config.Opts->AddLongOption("print-timestamp", "Print timestamp each second with statistics.")
            .StoreTrue(&PrintTimestamp);
        config.Opts->AddLongOption("client-timeout", "Client timeout. Supports time units (e.g., '5s', '1m'). Plain number interpreted as milliseconds.")
            .DefaultValue(ClientTimeoutStr).StoreResult(&ClientTimeoutStr);
        config.Opts->AddLongOption("operation-timeout", "Operation timeout. Supports time units (e.g., '5s', '1m'). Plain number interpreted as milliseconds.")
            .DefaultValue(OperationTimeoutStr).StoreResult(&OperationTimeoutStr);
        config.Opts->AddLongOption("cancel-after", "Cancel after timeout. Supports time units (e.g., '5s', '1m'). Plain number interpreted as milliseconds.")
            .DefaultValue(CancelAfterTimeoutStr).StoreResult(&CancelAfterTimeoutStr);
        config.Opts->AddLongOption("window", "Window duration in seconds.")
            .DefaultValue(WindowSec).StoreResult(&WindowSec);
        config.Opts->AddLongOption("executer", "Query executer type (data or generic).")
            .DefaultValue("generic").StoreResult(&QueryExecuterType)
            .ChoicesWithCompletion({{"data", "Data queries"}, {"generic", "Generic queries"}});
        Params.ConfigureOpts(config.Opts->GetOpts(), NYdbWorkload::TWorkloadParams::ECommandType::Run, -1);
    }

    TFulltextRunCommand::TFulltextRunCommand(
        NYdbWorkload::TFulltextWorkloadParams& params,
        const NYdbWorkload::IWorkloadQueryGenerator::TWorkloadType& workload,
        const TFulltextRunTree& tree)
        : TWorkloadCommand(workload.CommandName, std::initializer_list<TString>(), workload.Description)
        , Params(params)
        , Type(workload.Type)
        , Tree(tree)
    {
        Aliases = workload.Aliases;
    }

    void TFulltextRunCommand::Config(TConfig& config) {
        TYdbCommand::Config(config);
        config.Opts->SetFreeArgsNum(0);
        Params.ConfigureOpts(config.Opts->GetOpts(), NYdbWorkload::TWorkloadParams::ECommandType::Run, Type);
    }

    int TFulltextRunCommand::Run(TConfig& config) {
        TotalSec = Tree.TotalSec;
        Threads = Tree.Threads;
        WindowSec = Tree.WindowSec;
        Quiet = Tree.Quiet;
        PrintTimestamp = Tree.PrintTimestamp;
        ClientTimeoutStr = Tree.ClientTimeoutStr;
        OperationTimeoutStr = Tree.OperationTimeoutStr;
        CancelAfterTimeoutStr = Tree.CancelAfterTimeoutStr;
        QueryExecuterType = Tree.QueryExecuterType;

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
                    run->AddCommand(std::make_unique<TFulltextRunCommand>(*Params, type, *run));
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
