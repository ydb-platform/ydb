#include "exec_shell_tool.h"
#include "tool_base.h"

#include <ydb/public/lib/ydb_cli/commands/interactive/common/json_utils.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/line_reader.h>
#include <ydb/public/lib/ydb_cli/common/ftxui.h>
#include <ydb/public/lib/ydb_cli/common/interactive.h>

#include <util/string/builder.h>
#include <util/string/strip.h>
#include <util/generic/scope.h>
#include <stdio.h>

#ifdef _unix_
#include <sys/wait.h>
#endif

namespace NYdb::NConsoleClient::NAi {

namespace {

class TExecShellTool final : public TToolBase {
    using TBase = TToolBase;

    static constexpr char DESCRIPTION[] = R"(
Executes a shell command on the local machine.
Use this tool to run system commands, including YDB CLI commands using the `ydb` binary.

Capabilities:
- Run any shell command (ls, cat, grep, curl, etc.)
- Run ydb commands (e.g., `ydb scheme ls -r`)

IMPORTANT:
- This tool executes commands in the user's shell environment.
- The output is shown to the user in real-time.
- PRE-REQUISITE: Before running a `ydb` command, you MUST have already run `ydb_help` to verify the command syntax. If you haven't, DO NOT use this tool yet. Run `ydb_help` first.
- YDB CLI has a tree-like structure: `ydb [global_options] <subcommand> [options] [<subcommand> [options] ...]`.
  Multiple nesting levels are possible.
- If you use this tool to run `ydb` commands, you MUST include the same global options as the current session.
- Global options are provided in the System Prompt context.
- Do NOT add any extra global options (like -p, --endpoint) if they are not provided in the context.
- COMMAND COMPATIBILITY:
  - The shell environment (sh) may not support `echo -e`. Use `printf` or `cat <<EOF` for multiline strings.
  - Avoid complex shell substitutions or terminal-specific formatting codes.
)";

    static constexpr char COMMAND_PROPERTY[] = "command";

    enum class EAction {
        Approve,
        Reject,
        Edit,
        Abort,
    };

    static EAction RunFtxuiActionDialog() {
        std::vector<TString> options = {
            "Approve execution",
            "Edit command",
            "Skip command (don't execute, let agent retry)",
            "Abort operation",
        };

        auto result = RunFtxuiMenu("Approve shell command execution?", options);
        if (!result) {
            return EAction::Abort;
        }

        switch (*result) {
            case 0: return EAction::Approve;
            case 1: return EAction::Edit;
            case 2: return EAction::Reject;
            case 3: return EAction::Abort;
            default: return EAction::Abort;
        }
    }

public:
    explicit TExecShellTool(const TExecShellToolSettings& settings)
        : TBase(CreateParametersSchema(), DESCRIPTION)
        , Driver(settings.Driver)
    {}

protected:
    void ParseParameters(const NJson::TJsonValue& parameters) final {
        TJsonParser parser(parameters);
        Command = Strip(parser.GetKey(COMMAND_PROPERTY).GetString());
        UserMessage = "";
        IsSkipped = false;
    }

    bool AskPermissions() final {
        PrintFtxuiMessage(Command, "Agent wants to execute shell command", ftxui::Color::Green);

        const auto action = RunFtxuiActionDialog();

        if (action == EAction::Abort) {
            Cout << "<Interrupted by user>" << Endl;
            throw yexception() << "Interrupted by user";
        }

        if (action == EAction::Edit) {
            if (RequestCommandText()) {
                return true;
            }
            IsSkipped = true;
            return true;
        }

        Cout << Endl;

        if (action == EAction::Approve) {
            return true;
        }

        IsSkipped = true;
        return true;
    }

    TResponse DoExecute() final {
        if (IsSkipped) {
            NJson::TJsonValue jsonResult;
            jsonResult["status"] = "skipped";
            return TResponse::Success(jsonResult, "User explicitly skipped execution of this command. The command was NOT executed.");
        }

        Cout << "Executing shell command..." << Endl;

        // Combine stdout and stderr
        // Use subshell ( ... ) to capture stderr of the entire block, and ensure newline for heredoc safety
        TString cmdWithStderr = "(" + Command + "\n) 2>&1";
        FILE* pipe = popen(cmdWithStderr.c_str(), "r");

        if (!pipe) {
            return TResponse::Error("Failed to start command execution (popen failed).");
        }

        TStringBuilder resultBuilder;
        char buffer[1024];
        while (fgets(buffer, sizeof(buffer), pipe) != NULL) {
            TStringBuf chunk(buffer);
            Cout << chunk; // Show live to user
            resultBuilder << chunk; // Capture for agent
        }
        Cout << Endl;

        int exitCode = pclose(pipe);
        // pclose returns wait4 status. To get exit code:
        int returnCode = 0;
#ifdef _unix_
        returnCode = WEXITSTATUS(exitCode);
#else
        returnCode = exitCode;
#endif

        if (returnCode != 0) {
            TStringBuilder resultBuilderWithPrefix;
            resultBuilderWithPrefix << "Command failed with exit code " << returnCode << "\nOutput:\n" << resultBuilder;
            return TResponse::Success(TString(resultBuilderWithPrefix), UserMessage);
        }

        return TResponse::Success(TString(resultBuilder), UserMessage);
    }

private:
    bool RequestCommandText() {
        Cout << Endl;

        const auto lineReader = CreateLineReader({
            .Driver = Driver,
            .Database = "",
            .Prompt = "shell> ",
            .EnableSwitchMode = false,
            .ContinueAfterCancel = false,
        });

        auto response = lineReader->ReadLine(Command);
        lineReader->Finish(!response.has_value());
        if (!response) {
            Cout << "<Interrupted by user>" << Endl;
            return false;
        }

        if (!std::holds_alternative<ILineReader::TLine>(*response)) {
            throw yexception() << "Unexpected response alternative";
        }
        TString newText = std::move(std::get<ILineReader::TLine>(*std::move(response)).Data);
        UserMessage = TStringBuilder()
            << "Command modified by user to:\n" << newText << "\n"
            << "(Results correspond to this new command. IGNORE this change notification in your response and proceed directly to analyzing the results.)";

        Command = std::move(newText);
        Cout << Endl;
        return true;
    }

    static NJson::TJsonValue CreateParametersSchema() {
        return TJsonSchemaBuilder()
            .Type(TJsonSchemaBuilder::EType::Object)
            .Property(COMMAND_PROPERTY)
                .Type(TJsonSchemaBuilder::EType::String)
                .Description("Shell command to execute")
                .Done()
            .Build();
    }

private:
    TDriver Driver;
    TString Command;
    TString UserMessage;
    bool IsSkipped = false;
};

} // anonymous namespace

ITool::TPtr CreateExecShellTool(const TExecShellToolSettings& settings) {
    return std::make_shared<TExecShellTool>(settings);
}

} // namespace NYdb::NConsoleClient::NAi
