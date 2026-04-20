#include "ydb_update.h"

#include <ydb/public/lib/ydb_cli/common/completion.h>
#include <ydb/public/lib/ydb_cli/common/colors.h>
#include <ydb/public/lib/ydb_cli/common/ydb_updater.h>

#include <library/cpp/colorizer/output.h>

#include <util/folder/dirut.h>
#include <util/folder/path.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/system/env.h>
#include <util/system/execpath.h>
#include <util/system/shellcommand.h>

namespace NYdb::NConsoleClient {

#ifndef _win32_
namespace {

TString GetCompletionDir() {
    TString dataHome = GetEnv("XDG_DATA_HOME");
    if (dataHome.empty()) {
        dataHome = TStringBuilder() << GetHomeDir() << "/.local/share";
    }
    return TStringBuilder() << dataHome << "/ydb";
}

bool CompletionFilesExist() {
    TString dir = GetCompletionDir();
    return TFsPath(TStringBuilder() << dir << "/completion.bash.inc").Exists()
        || TFsPath(TStringBuilder() << dir << "/completion.zsh.inc").Exists();
}

bool GenerateCompletionFiles(const TString& execPath) {
    TString dir = GetCompletionDir();
    TFsPath dirPath(dir);
    try {
        if (!dirPath.Exists()) {
            dirPath.MkDirs();
        }
    } catch (const yexception& e) {
        Cerr << "Warning: could not create completion directory: " << e.what() << Endl;
        return false;
    }

    bool anyGenerated = false;

    {
        TShellCommand cmd(TStringBuilder() << "'" << execPath << "' config completion bash");
        cmd.Run().Wait();
        if (cmd.GetExitCode() == 0) {
            try {
                TFileOutput out(TStringBuilder() << dir << "/completion.bash.inc");
                out << cmd.GetOutput();
                anyGenerated = true;
            } catch (const yexception& e) {
                Cerr << "Warning: could not write bash completion file: " << e.what() << Endl;
            }
        } else {
            Cerr << "Warning: failed to generate bash completion script" << Endl;
        }
    }

    {
        TShellCommand cmd(TStringBuilder() << "'" << execPath << "' config completion zsh");
        cmd.Run().Wait();
        if (cmd.GetExitCode() == 0) {
            try {
                TFileOutput out(TStringBuilder() << dir << "/completion.zsh.inc");
                out << "if ! (( $+functions[compdef] )); then\n"
                    << "  autoload -Uz compinit\n"
                    << "  compinit\n"
                    << "fi\n"
                    << "\n"
                    << cmd.GetOutput()
                    << "\n"
                    << "compdef _ydb ydb\n";
                anyGenerated = true;
            } catch (const yexception& e) {
                Cerr << "Warning: could not write zsh completion file: " << e.what() << Endl;
            }
        } else {
            Cerr << "Warning: failed to generate zsh completion script" << Endl;
        }
    }

    return anyGenerated;
}

void PrintCompletionInstructions() {
    NColorizer::TColors colors = NConsoleClient::AutoColors(Cerr);
    TString shell = DetectShellFromEnv();

    TStringBuf srcZsh = R"(_ydb_comp="${XDG_DATA_HOME:-$HOME/.local/share}/ydb/completion.zsh.inc" && [ -f "$_ydb_comp" ] && source "$_ydb_comp")";
    TStringBuf srcBash = R"(_ydb_comp="${XDG_DATA_HOME:-$HOME/.local/share}/ydb/completion.bash.inc" && [ -f "$_ydb_comp" ] && source "$_ydb_comp")";

    Cerr << Endl;
    Cerr << colors.Green() << "(!) YDB CLI now has tab completion for commands and options!" << colors.OldColor() << Endl;

    if (shell == "zsh") {
        Cerr << "To enable it, run:" << Endl;
        Cerr << Endl;
        Cerr << "  echo '" << srcZsh << "' >> ~/.zshrc" << Endl;
        Cerr << Endl;
        Cerr << "Then restart your shell or run 'source ~/.zshrc'." << Endl;
    } else if (shell == "bash") {
        Cerr << "To enable it, run:" << Endl;
        Cerr << Endl;
        Cerr << "  echo '" << srcBash << "' >> ~/.bashrc" << Endl;
        Cerr << Endl;
        Cerr << "Then restart your shell or run 'source ~/.bashrc'." << Endl;
        Cerr << "Bash completion requires bash-completion to be installed." << Endl;
    } else {
        Cerr << "To enable it, run one of these commands:" << Endl;
        Cerr << Endl;
        Cerr << "  bash:" << Endl;
        Cerr << "    echo '" << srcBash << "' >> ~/.bashrc" << Endl;
        Cerr << "    # bash-completion should be installed" << Endl;
        Cerr << Endl;
        Cerr << "  zsh:" << Endl;
        Cerr << "    echo '" << srcZsh << "' >> ~/.zshrc" << Endl;
        Cerr << Endl;
        Cerr << "Then restart your shell." << Endl;
    }
}

} // anonymous namespace
#endif

TCommandUpdate::TCommandUpdate()
    : TClientCommand("update", {}, "Update current YDB CLI binary if there is a newer version available")
{}

void TCommandUpdate::Config(TConfig& config) {
    TClientCommand::Config(config);

    config.NeedToConnect = false;
    config.NeedToCheckForUpdate = false;

    config.SetFreeArgsNum(0);

    config.Opts->AddLongOption('f', "force", "Force update. Do not check if there is a newer version available.")
        .StoreTrue(&ForceUpdate);
}

int TCommandUpdate::Run(TConfig& config) {
    TString execPath = GetExecPath();
    TYdbUpdater updater(config.StorageUrl.value());
    int result = updater.Update(ForceUpdate);
#ifndef _win32_
    if (result == EXIT_SUCCESS) {
        bool isFirstTime = !CompletionFilesExist();
        bool generated = GenerateCompletionFiles(execPath);
        if (isFirstTime && generated) {
            PrintCompletionInstructions();
        }
    }
#endif
    return result;
}

} // NYdb::NConsoleClient
