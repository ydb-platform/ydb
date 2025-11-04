#include "completion.h"
#include <library/cpp/getopt/small/completion_generator.h>

namespace NYdb {
namespace NConsoleClient {
void TYdbCommandAutoCompletionWrapper::RegisterOptions(NLastGetopt::TOpts &opts) {
  opts = command->Opts.GetOpts();
}

int TYdbCommandAutoCompletionWrapper::DoRun(NLastGetopt::TOptsParseResult &&parsedOptions) {
  Y_UNUSED(parsedOptions);
  return EXIT_FAILURE;
}

void TYdbCommandTreeAutoCompletionWrapper::RegisterOptions(NLastGetopt::TOpts &opts) {
  opts = commandTree->Opts.GetOpts();
}

int TYdbCommandTreeAutoCompletionWrapper::DoRun(NLastGetopt::TOptsParseResult &&parsedOptions) {
  Y_UNUSED(parsedOptions);
  return EXIT_FAILURE;
}

void RegisterModes(TModChooser &chooser, const TClientCommandTree *commandTree) {
  chooser.SetDescription(commandTree->Description);

  Cerr << "RegisterModes " << commandTree->Name << Endl;
  for (auto &[name, command] : commandTree->GetSubCommands()) {
    Cerr << "RegisterModes " << commandTree->Name << " " << name << Endl;
    // Check if the command is a TClientCommandTree (which inherits from TMainClassModes)
    TClientCommandTree *tree = dynamic_cast<TClientCommandTree *>(command.get());
    TClientCommand *cmd = dynamic_cast<TClientCommand *>(command.get());
    if (tree) {
      Cerr << "RegisterModes added (tree)" << commandTree->Name << " " << name << Endl;
      TMainClassModes* ptr = new TYdbCommandTreeAutoCompletionWrapper(tree);
      chooser.AddMode(name, ptr, command->Description, command->Hidden, command->Hidden);
    } else if (cmd) {
        TMainClassArgs* ptr = new TYdbCommandAutoCompletionWrapper(cmd);
        Cerr << "RegisterModes added " << commandTree->Name << " " << name << Endl;
        chooser.AddMode(name, ptr, command->Description, command->Hidden, command->Hidden);
    }
  }
}


void TYdbCommandTreeAutoCompletionWrapper::RegisterModes(TModChooser &chooser) {
    ::NYdb::NConsoleClient::RegisterModes(chooser, commandTree);
}

NLastGetopt::TOpt GenerateCompletionOption(const TClientCommandTree *commandTree) {
  NLastGetopt::TOpt completionOption =
      NLastGetopt::TOpt()
          .AddLongName("completion")
          .Help("generate tab completion script for zsh or bash")
          .CompletionHelp("generate tab completion script")
          .OptionalArgument("shell-syntax")
          .CompletionArgHelp("shell syntax for completion script")
          .IfPresentDisableCompletion()
          .Completer(NLastGetopt::NComp::Choice({{"zsh"}, {"bash"}}))
          .Handler1T<TString>([commandTree](TStringBuf shell) {
            if (shell.empty()) {
            //   Cerr << Wrap(80, NLastGetopt::MakeInfo("ydb", "--completion")) << Endl;
              exit(0);
            } 
            
            TModChooser *modChooser = new TModChooser();
            RegisterModes(*modChooser, commandTree);
        

            auto modes = modChooser->GetUnsortedModes();
            std::cerr << "Available modes:" << std::endl;
            for (const auto &mode : modes) {
                std::cerr << "  " << mode->Name;
            }

            if (shell == "bash") {
              NLastGetopt::TBashCompletionGenerator(modChooser).Generate("ydb", Cout);
            } else if (shell == "zsh") {
              NLastGetopt::TZshCompletionGenerator(modChooser).Generate("ydb", Cout);
            } else {
              Cerr << "Unknown shell name " << TString{shell}.Quote() << Endl;
              exit(1);
            }
            exit(0);
          });
  return completionOption;
}
} // namespace NConsoleClient
} // namespace NYdb