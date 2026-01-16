#include "ydb_help_tool.h"
#include "tool_base.h"

#include <ydb/public/lib/ydb_cli/commands/interactive/common/json_utils.h>
#include <ydb/public/lib/ydb_cli/common/ftxui.h>
#include <ydb/public/lib/ydb_cli/common/log.h>

#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/strip.h>
#include <util/system/execpath.h>
#include <util/system/shellcommand.h>

namespace NYdb::NConsoleClient::NAi {

namespace {

class TYdbHelpTool final : public TToolBase {
    using TBase = TToolBase;

    static constexpr char DESCRIPTION[] = R"(
Displays help information for YDB CLI commands.
Use this tool to understand how to use specific YDB CLI commands, their arguments, and options.

YDB CLI Structure:
- The CLI has a tree-like structure: `ydb [global_options] <subcommand> [options] <subcommand> ...`
- Global options (e.g., connection settings like `--endpoint`, `--database`, `-p`) must be specified BEFORE the first subcommand.

Execution Rule (for `exec_shell`):
- If you use `exec_shell` to run `ydb` commands, you MUST include the same global options as the current session.
- Global options are provided in the System Prompt context.
- Example: If the user launched the tool with `ydb -p myprofile ...`, you must generate `ydb -p myprofile scheme ls`, not just `ydb scheme ls`.
- Failure to include connection options may cause the command to fail or connect to the wrong database.
- Do NOT add any extra global options (like -p, --endpoint) if they are not provided in the context.

The tool automatically prepends the current YDB CLI binary path and appends '--help'.

IMPORTANT:
- Do NOT include the binary name (e.g. 'ydb') in the command.
- Do NOT include '--help' flag.
- To get general help (full command tree), leave the arguments empty.
- Do NOT guess command names. First, call this tool with empty arguments to see the list of available commands.
- If you are unsure about command syntax or options, ALWAYS use this tool to verify them before execution.
- NEVER invent commands or options. Only use those that are documented in the help output.
- Do NOT use 'ydb sql' or 'ydb table query execute' to execute queries. Use the 'exec_query' tool instead.

Examples:
- To get help for `ydb scheme`: call with 'scheme'.
- To get help for `ydb scheme ls`: call with 'scheme ls'.
)";

    static constexpr char COMMAND_PROPERTY[] = "command";

public:
    explicit TYdbHelpTool(const TYdbHelpToolSettings& /*settings*/)
        : TBase(CreateParametersSchema(), DESCRIPTION)
    {}

protected:
    void ParseParameters(const NJson::TJsonValue& parameters) final {
        TJsonParser parser(parameters);
        Command = Strip(parser.GetKey(COMMAND_PROPERTY).GetString());
    }

    bool AskPermissions() final {
        if (Command.empty() || Command == "ydb") {
            PrintFtxuiMessage("", "Listing all YDB CLI commands...", ftxui::Color::Green);
        } else {
            PrintFtxuiMessage("", TStringBuilder() << "Analyzing 'ydb " << Command << "' command description...", ftxui::Color::Green);
        }
        Cout << Endl;
        return true;
    }

    TResponse DoExecute() final {
        TString binaryPath = GetExecPath();
        TStringBuilder cmdBuilder;
        cmdBuilder << binaryPath;

        // If command is empty or just "ydb", we add -hh to get full help
        if (Command.empty() || Command == "ydb") {
            cmdBuilder << " -hh";
        } else {
            if (!Command.StartsWith(" ")) {
                cmdBuilder << " ";
            }
            cmdBuilder << Command;
            if (!Command.EndsWith("help") && !Command.EndsWith("-h") && !Command.EndsWith("-?")) {
                cmdBuilder << " -hh";
            }
        }

        TString fullCmd = cmdBuilder;

        if (GetGlobalLogger().IsVerbose()) {
            Cout << "Executing: " << fullCmd << Endl;
        }

        TShellCommand shellCmd(fullCmd);
        shellCmd.Run().Wait();

        TString output = shellCmd.GetOutput();
        TString error = shellCmd.GetError();

        TString result = output;
        if (!error.empty()) {
            result += "\nStderr:\n" + error;
        }

        if (GetGlobalLogger().IsVerbose()) {
            Cout << result << Endl;
        }

        if (shellCmd.GetExitCode() != 0) {
            TStringBuilder resultBuilder;
            resultBuilder << "Command failed with exit code " << (shellCmd.GetExitCode().Defined() ? ToString(*shellCmd.GetExitCode()) : "unknown") << "\nOutput:\n" << result;
            return TResponse::Success(TString(resultBuilder));
        }

        return TResponse::Success(TString(result));
    }

private:
    static NJson::TJsonValue CreateParametersSchema() {
        return TJsonSchemaBuilder()
            .Type(TJsonSchemaBuilder::EType::Object)
            .Property(COMMAND_PROPERTY)
                .Type(TJsonSchemaBuilder::EType::String)
                .Description("Command arguments to get help for (e.g., 'scheme ls'). If empty, returns general help. Do NOT include 'ydb' or '--help'.")
                .Done()
            .Build();
    }

private:
    TString Command;
};

} // anonymous namespace

ITool::TPtr CreateYdbHelpTool(const TYdbHelpToolSettings& settings) {
    return std::make_shared<TYdbHelpTool>(settings);
}

} // namespace NYdb::NConsoleClient::NAi

