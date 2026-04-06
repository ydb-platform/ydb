#include "ydb_config.h"
#include "ydb_profile.h"

#include <ydb/public/lib/ydb_cli/common/completion.h>
#include <ydb/public/lib/ydb_cli/common/completion_generator.h>

#include <library/cpp/getopt/small/completer.h>

namespace NYdb::NConsoleClient {

TCommandConfig::TCommandConfig(TClientCommandTree* rootTree)
    : TClientCommandTree("config", {}, "Manage YDB CLI configuration")
    , RootTree(rootTree)
{
    AddCommand(std::make_unique<TCommandProfile>());
    AddCommand(std::make_unique<TCommandConnectionInfo>());
    AddCommand(std::make_unique<TCommandCompletion>(RootTree));
}

void TCommandConfig::Config(TConfig& config) {
    TClientCommandTree::Config(config);
}

TCommandCompletion::TCommandCompletion(TClientCommandTree* rootTree)
    : TClientCommand("completion", {}, "Generate tab completion script for bash or zsh")
    , RootTree(rootTree)
{}

void TCommandCompletion::Config(TConfig& config) {
    TClientCommand::Config(config);
    config.NeedToConnect = false;
    config.SetFreeArgsMax(1);
    SetFreeArgTitle(0, "<shell>", "Shell syntax for completion script (bash or zsh)");
    config.Opts->GetOpts().GetFreeArgSpec(0)
        .CompletionArgHelp("shell syntax for completion script")
        .Completer(NLastGetopt::NComp::Choice({{"zsh"}, {"bash"}}));
}

int TCommandCompletion::Run(TConfig& config) {
    if (config.ParseResult->GetFreeArgCount() > 0) {
        Shell = config.ParseResult->GetFreeArgs()[0];
    } else {
        Shell = DetectShellFromEnv();
        if (Shell.empty()) {
            Cerr << MakeCompletionInfo("ydb", "config completion") << Endl;
            return EXIT_FAILURE;
        }
    }

    auto modChooser = TModChooser();
    auto rootWrapper = TYdbCommandTreeAutoCompletionWrapper(RootTree, config);
    rootWrapper.RegisterModes(modChooser);

    if (Shell == "bash") {
        NLastGetoptFork::TBashCompletionGenerator(&modChooser, &RootTree->Opts.GetOpts()).Generate("ydb", Cout);
    } else if (Shell == "zsh") {
        NLastGetoptFork::TZshCompletionGenerator(&modChooser, &RootTree->Opts.GetOpts()).Generate("ydb", Cout);
    } else {
        Cerr << "Unknown shell name " << Shell.Quote() << Endl;
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}

} // namespace NYdb::NConsoleClient
