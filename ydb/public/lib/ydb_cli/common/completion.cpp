#include "completion.h"
#include "completion_generator.h"

namespace NYdb {
namespace NConsoleClient {
void TYdbCommandAutoCompletionWrapper::RegisterOptions(NLastGetopt::TOpts &opts) {
  command->Config(config);
  opts = command->Opts.GetOpts();
}

int TYdbCommandAutoCompletionWrapper::DoRun(NLastGetopt::TOptsParseResult &&parsedOptions) {
  Y_UNUSED(parsedOptions);
  return EXIT_FAILURE;
}

void TYdbCommandTreeAutoCompletionWrapper::RegisterOptions(NLastGetopt::TOpts &opts) {
  commandTree->Config(config);
  opts = commandTree->Opts.GetOpts();
}

int TYdbCommandTreeAutoCompletionWrapper::DoRun(NLastGetopt::TOptsParseResult &&parsedOptions) {
  Y_UNUSED(parsedOptions);
  return EXIT_FAILURE;
}

void TYdbCommandTreeAutoCompletionWrapper::RegisterModes(TModChooser &chooser) {
    chooser.SetDescription(commandTree->Description);
    subCommands.reserve(commandTree->SubCommands.size());
    for (auto &[name, command] : commandTree->SubCommands) {
    TClientCommandTree *tree = dynamic_cast<TClientCommandTree *>(command.get());
    TClientCommand *cmd = dynamic_cast<TClientCommand *>(command.get());
    if (tree) {
      TMainClassModes* ptr = new TYdbCommandTreeAutoCompletionWrapper(tree, config);
      chooser.AddMode(name, ptr, command->Description, command->Hidden, command->Hidden);
      subCommands.push_back(std::unique_ptr<TMainClass>(ptr));
    } else if (cmd) {
        TMainClassArgs* ptr = new TYdbCommandAutoCompletionWrapper(cmd, config);
        chooser.AddMode(name, ptr, command->Description, command->Hidden, command->Hidden);
        subCommands.push_back(std::unique_ptr<TMainClass>(ptr));
    }
  }
}


TString MakeInfo(TStringBuf command, TStringBuf flag) {
        TString info = (
            "This command generates shell script with completion function and prints it to `stdout`, "
            "allowing one to re-direct the output to the file of their choosing. "
            "Where you place the file will depend on which shell and operating system you are using."
            "\n"
            "\n"
            "\n"
            "{B}BASH (Linux){R}:"
            "\n"
            "\n"
            "For system-wide commands, completion files are stored in `/etc/bash_completion.d/`. "
            "For user commands, they are stored in `~/.local/share/bash-completion/completions`. "
            "So, pipe output of this script to a file in one of these directories:"
            "\n"
            "\n"
            "    $ mkdir -p ~/.local/share/bash-completion/completions"
            "\n"
            "    $ {command} {completion} bash >"
            "\n"
            "          ~/.local/share/bash-completion/completions/{command}"
            "\n"
            "\n"
            "You'll need to restart your shell for changes to take effect."
            "\n"
            "\n"
            "\n"
            "{B}BASH (OSX){R}:"
            "\n"
            "\n"
            "You'll need `bash-completion` (or `bash-completion@2` if you're using non-default, newer bash) "
            "homebrew formula. Completion files are stored in `/usr/local/etc/bash_completion.d`:"
            "\n"
            "\n"
            "    $ mkdir -p $(brew --prefix)/etc/bash_completion.d"
            "\n"
            "    $ {command} {completion} bash >"
            "\n"
            "          $(brew --prefix)/etc/bash_completion.d/{command}"
            "\n"
            "\n"
            "Alternatively, just source the script in your `~/.bash_profile`."
            "\n"
            "\n"
            "You'll need to restart your shell for changes to take effect."
            "\n"
            "\n"
            "\n"
            "{B}ZSH{R}:"
            "\n"
            "\n"
            "Zsh looks for completion scripts in any directory listed in `$fpath` variable. We recommend placing "
            "completions to `~/.zfunc`:"
            "\n"
            "\n"
            "    $ mkdir -m755 -p ~/.zfunc"
            "\n"
            "    $ {command} {completion} zsh > ~/.zfunc/_{command}"
            "\n"
            "\n"
            "Add the following lines to your `.zshrc` just before `compinit`:"
            "\n"
            "\n"
            "    fpath+=~/.zfunc"
            "\n"
            "\n"
            "You'll need to restart your shell for changes to take effect.");
        SubstGlobal(info, "{command}", command);
        SubstGlobal(info, "{completion}", flag);
        SubstGlobal(info, "{B}", NColorizer::StdErr().LightDefault());
        SubstGlobal(info, "{R}", NColorizer::StdErr().Reset());
        return info;
    }


NLastGetopt::TOpt GenerateCompletionOption(TStringBuf command, TClientCommandTree *commandTree, TClientCommand::TConfig& config) {
  NLastGetopt::TOpt completionOption =
      NLastGetopt::TOpt()
          .AddLongName("completion")
          .Help("generate tab completion script for zsh or bash")
          .CompletionHelp("generate tab completion script")
          .OptionalArgument("shell-syntax")
          .CompletionArgHelp("shell syntax for completion script")
          .IfPresentDisableCompletion()
          .Completer(NLastGetopt::NComp::Choice({{"zsh"}, {"bash"}}))
          .Handler1T<TString>([command, commandTree, config](TStringBuf shell) {
            if (shell.empty()) {
              Cerr << MakeInfo(command, "--completion") << Endl;
              exit(0);
            }
            
            auto modChooser = TModChooser();
            auto rootWrapper = TYdbCommandTreeAutoCompletionWrapper(commandTree, config);
            rootWrapper.RegisterModes(modChooser);

            if (shell == "bash") {
              NLastGetoptFork::TBashCompletionGenerator(&modChooser, &config.Opts->GetOpts()).Generate(command, Cout);
            } else if (shell == "zsh") {
              NLastGetoptFork::TZshCompletionGenerator(&modChooser, &config.Opts->GetOpts()).Generate(command, Cout);
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
