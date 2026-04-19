#include "ydb_help_tool.h"
#include "tool_base.h"

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/json_utils.h>
#include <ydb/public/lib/ydb_cli/common/ftxui.h>
#include <ydb/public/lib/ydb_cli/common/log.h>

#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/strip.h>

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

The tool will get information about the YDB CLI command by list of subcommands, for example, use tool arguments ['scheme', 'ls'] for 'ydb scheme ls' command.

IMPORTANT:
- Do NOT include the binary name (e.g. 'ydb') in the command.
- Do NOT include '--help' flag.
- To get general help (full command tree), leave the arguments empty.
- Do NOT guess command names. First, call this tool with empty arguments to see the list of available commands.
- If you are unsure about command syntax or options, ALWAYS use this tool to verify them before execution.
- NEVER invent commands or options. Only use those that are documented in the help output.
- Do NOT use 'ydb sql' or 'ydb table query execute' to execute queries. Use the 'exec_query' tool instead.

Examples:
- To get help for `ydb scheme`: call with ['scheme'].
- To get help for `ydb scheme ls`: call with ['scheme', 'ls'].
)";

    static constexpr char COMMAND_PROPERTY[] = "command";

public:
    explicit TYdbHelpTool(const TYdbHelpToolSettings& settings)
        : TBase(CreateParametersSchema(), DESCRIPTION)
        , UsageInfoGetter(settings.UsageInfoGetter)
    {
        Y_VALIDATE(UsageInfoGetter, "UsageInfoGetter is required");
    }

protected:
    void ParseParameters(const NJson::TJsonValue& parameters) final {
        TJsonParser parser(parameters);
        Commands.clear();
        parser.GetKey(COMMAND_PROPERTY).Iterate([&](TJsonParser parser) {
            TString command = Strip(parser.GetString());
            if (Commands.empty() && command == "ydb") {
                return;
            }
            Commands.emplace_back(std::move(command));
        });
    }

    bool AskPermissions() final {
        if (Commands.empty()) {
            PrintFtxuiMessage("", "Listing all YDB CLI commands...", ftxui::Color::Green);
        } else {
            PrintFtxuiMessage("", TStringBuilder() << "Analyzing 'ydb " << JoinSeq(" ", Commands) << "' command description...", ftxui::Color::Green);
        }
        return true;
    }

    TResponse DoExecute() final {
        try {
            TString usageInfo = UsageInfoGetter(Commands);
            return TResponse::Success(usageInfo);
        } catch (const std::exception& e) {
            return TResponse::Error(TStringBuilder() << "Failed to get usage info: " << e.what());
        }
    }

private:
    static NJson::TJsonValue CreateParametersSchema() {
        return TJsonSchemaBuilder()
            .Property(COMMAND_PROPERTY)
                .Description("Command arguments to get help for (e.g., ['scheme', 'ls'] for 'ydb scheme ls' command). If empty, returns general help. Do NOT include 'ydb' or '--help'.")
                .Items()
                    .Type(TJsonSchemaBuilder::EType::String)
                    .Description("Single command argument")
                    .Done()
                .Done()
            .Build();
    }

private:
    const TClientCommand::TConfig::TUsageInfoGetter UsageInfoGetter;
    std::vector<TString> Commands;
};

} // anonymous namespace

ITool::TPtr CreateYdbHelpTool(const TYdbHelpToolSettings& settings) {
    return std::make_shared<TYdbHelpTool>(settings);
}

} // namespace NYdb::NConsoleClient::NAi

