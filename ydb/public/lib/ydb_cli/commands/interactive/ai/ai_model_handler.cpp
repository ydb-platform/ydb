#include "ai_model_handler.h"

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/lib/ydb_cli/common/log.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/ai/models/model_anthropic.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/ai/models/model_openai.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/ai/tools/exec_query_tool.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/ai/tools/list_directory_tool.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/ai/tools/describe_tool.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/ai/tools/ydb_help_tool.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/ai/tools/exec_shell_tool.h>
#include <ydb/public/lib/ydb_cli/common/ftxui.h>

#include <util/string/strip.h>
#include <util/string/printf.h>

namespace NYdb::NConsoleClient::NAi {

namespace {

constexpr char SYSTEM_PROMPT[] = R"(You are an intelligent assistant working in a CLI terminal.
You have access to tools to interact with the YDB database.

*** IMPORTANT: YOU DO NOT KNOW THE YDB CLI COMMAND SYNTAX. ***
*** YOU MUST DISCOVER IT USING TOOLS. DO NOT HALLUCINATE COMMANDS. ***

OUTPUT FORMATTING:
- Your output is printed directly to the terminal console.
- Do NOT use Markdown formatting (no bold **, no headers #, no code blocks ```).
- Do NOT use LaTeX or special symbols.
- Separate paragraphs with an extra blank line for better readability.

CRITICAL EXECUTION RULES:

1. **MANDATORY DISCOVERY**: You are PROHIBITED from executing any `ydb` shell command unless you have successfully run `ydb_help` with empty arguments and then `ydb_help` for a subcommand that you are going to use in this session to verify its syntax.
   - WRONG: "I will import..." -> `exec_shell("ydb import ...")` (HALLUCINATION - STOP!)
   - CORRECT: "I need to check import syntax..." -> `ydb_help()` -> `ydb_help("import")` -> `exec_shell("ydb import file") -> etc.`

2. **UNKNOWN SCHEMA**: You are PROHIBITED from writing SQL queries or importing data without first inspecting the table schema using `describe`.

3. **UNKNOWN DATA VALUES**: You are PROHIBITED from filtering data (WHERE clause) using guessed values.
   - WRONG: Directly using a guessed value in WHERE clause (e.g., `WHERE status = 'some_guess'`) without knowing if it exists.
   - CORRECT: First inspect the data (e.g., `SELECT DISTINCT column FROM table LIMIT 20`) to see actual values, then use them in filtering.

4. **CONNECTION PARAMETERS**:
   - The user's connection parameters are provided in the [CONTEXT] below.
   - You MUST use ONLY those parameters when using ydb cli in exec_shell tool.
   - NEVER add `-p`, `--profile`, `--endpoint`, etc., unless they are explicitly in the [CONTEXT].

STRATEGY FOR ANY REQUEST:
1. Can I use native tools (`list_directory`, `describe`, `exec_query`)? If yes, use them.
2. If not, maybe I can use YDB CLI binary? If I need the YDB CLI binary:
   a. Call `ydb_help` (empty) to list all available commands.
   b. Call `ydb_help <subcommand>` to learn syntax.
   c. ONLY THEN construct and execute the `exec_shell` command.

INTERACTION GUIDELINES:
- **ALWAYS** propose a plan first, for ANY request that is going to use more than one tool.
  1. List the tools you intend to call and the actions you will take.
  2. Ask the user for confirmation if the plan consists of more than 2 tools.
  3. If you asked for confirmation, WAIT for the user's explicit confirmation ("yes", "ok", etc.) before executing ANY tool.
- Once confirmed, proceed with execution.
- If the user's request implies deleting or modifying data, be extra careful and verify the WHERE clause logic by inspecting the schema first.
- If a tool returns "skipped" status or "User skipped execution", DO NOT treat it as an error. Do NOT apologize. Just consider it as a user request to skip the tool execution. Do not output verbose confirmations like "I acknowledge that the user skipped". Proceed directly to the next logical step or ask what to do next.
)";

TString PrintToolsNames(const std::unordered_map<TString, ITool::TPtr>& tools) {
    TStringBuilder builder;
    for (ui64 i = 0; const auto& [name, tool] : tools) {
        if (i++) {
            builder << ", ";
        }
        builder << name;
    }
    return builder;
}

} // anonymous namespace

/*

FEATURES-TODO:

- Streamable model response printing
- Think about robust

*/

TModelHandler::TModelHandler(const TSettings& settings)
{
    SetupModel(settings.Profile, settings);
    SetupTools(settings);
}

void TModelHandler::HandleLine(const TString& input, std::function<void()> onStartWaiting, std::function<void()> onFinishWaiting, std::function<double()> getThinkingTime) {
    Y_VALIDATE(Model, "Model must be initialized before handling input");

    if (!input) {
        return;
    }

    std::vector<IModel::TMessage> messages = {IModel::TUserMessage{.Text = input}};
    while (!messages.empty()) {
        IModel::TResponse output;
        try {
            output = Model->HandleMessages(messages, onStartWaiting, onFinishWaiting);
            messages.clear();
        } catch (const std::exception& e) {
            if (onFinishWaiting) {
                onFinishWaiting();
            }
            Cerr << Colors.Red() << e.what() << Colors.OldColor() << Endl;
            break;
        }

        if (!output.Text && output.ToolCalls.empty()) {
            Cout << Colors.Yellow() << "Model answer is empty, try to reformulate question." << Colors.OldColor() << Endl;
            break;
        }

        if (output.Text) {
            TString title = "Agent response";
            if (getThinkingTime) {
                if (double elapsed = getThinkingTime(); elapsed > 0.0) {
                    title += Sprintf(" (after %.2fs)", elapsed);
                }
            }
            ::NYdb::NConsoleClient::PrintFtxuiMessage(StripStringRight(output.Text), title);

            if (!output.ToolCalls.empty()) {
                Cout << Endl;
            }
        }

        bool interrupted = false;
        std::vector<TString> userMessages;
        for (const auto& toolCall : output.ToolCalls) {
            messages.emplace_back(CallTool(toolCall, userMessages, interrupted));
        }

        for (auto& message : userMessages) {
            messages.emplace_back(IModel::TUserMessage{.Text = std::move(message)});
        }
        userMessages.clear();

        if (interrupted) {
            Model->AddMessages(messages);
            break;
        }
    }

    Cout << Endl;
}

void TModelHandler::ClearContext() {
    Y_VALIDATE(Model, "Model must be initialized before handling clearing context");
    Model->ClearContext();
}

IModel::TToolResponse TModelHandler::CallTool(const IModel::TResponse::TToolCall& toolCall, std::vector<TString>& userMessages, bool& interrupted) const {
    IModel::TToolResponse response = {.ToolCallId = toolCall.Id};

    const auto it = Tools.find(toolCall.Name);
    if (it == Tools.end()) {
        response.IsSuccess = false;
        response.Text = TStringBuilder() << "Call to unknown tool: " << toolCall.Name << ". Only allowed tools: " << PrintToolsNames(Tools);
        return response;
    }

    std::optional<ITool::TResponse> result;
    if (!interrupted) {
        try {
            result.emplace(it->second->Execute(toolCall.Parameters));
        } catch (const yexception& e) {
            if (TString(e.what()).Contains("Interrupted by user")) {
                interrupted = true;
            } else {
                throw;
            }
        }
    }

    if (interrupted) {
        response.IsSuccess = false;
        response.Text = "Tool execution interrupted by user.";
        return response;
    }

    if (result->UserMessage) {
        YDB_CLI_LOG(Debug, "User message during tool call: " << result->ToolResult);
        userMessages.emplace_back(std::move(result->UserMessage));
    }
    if (!result->IsSuccess) {
        YDB_CLI_LOG(Warning, "Tool call failed: " << result->ToolResult);
    }
    response.IsSuccess = result->IsSuccess;
    response.Text = std::move(result->ToolResult);

    return response;
}

void TModelHandler::SetupModel(TInteractiveConfigurationManager::TAiProfile::TPtr profile, const TSettings& settings) {
    Y_VALIDATE(profile, "AI profile must be initialized");

    TString ValidationError;
    Y_VALIDATE(profile->IsValid(ValidationError), "AI profile must be valid, but got: " << ValidationError);

    const auto apiType = profile->GetApiType();
    Y_VALIDATE(apiType, "AI profile must have API type");

    const auto& endpoint = profile->GetApiEndpoint();
    Y_VALIDATE(endpoint, "AI profile must have API endpoint");

    const auto& apiKey = profile->GetApiToken();
    const auto& modelName = profile->GetModelName();

    TString systemPrompt = SYSTEM_PROMPT;
    if (!settings.ConnectionString.empty()) {
        systemPrompt += "\n[CONTEXT] The user is connected to YDB with this command line prefix: " + settings.ConnectionString + "\n"
                        "When using `exec_shell` to run `ydb` commands, you MUST prepend this prefix (it includes binary path and global options).\n"
                        "Do NOT add any other connection parameters (like -p, --endpoint, --database) unless they are explicitly present in this prefix. If no connection options are provided, it means the environment is already configured (e.g., via default profile or environment variables).\n";
    }

    switch (*apiType) {
        case TInteractiveConfigurationManager::EAiApiType::OpenAI:
            Model = CreateOpenAiModel({.BaseUrl = endpoint, .ModelId = modelName, .ApiKey = apiKey, .SystemPrompt = systemPrompt});
            break;
        case TInteractiveConfigurationManager::EAiApiType::Anthropic:
            Model = CreateAnthropicModel({.BaseUrl = endpoint, .ModelId = modelName, .ApiKey = apiKey, .SystemPrompt = systemPrompt});
            break;
        case TInteractiveConfigurationManager::EAiApiType::Invalid:
            Y_VALIDATE(false, "Invalid API type: " << *apiType);
    }
}

void TModelHandler::SetupTools(const TSettings& settings) {
    Y_VALIDATE(Model, "Model must be initialized before initializing tools");

    Tools = {
        {"list_directory", CreateListDirectoryTool({.Database = settings.Database, .Driver = settings.Driver})},
        {"exec_query", CreateExecQueryTool({.Prompt = settings.Prompt, .Database = settings.Database, .Driver = settings.Driver})},
        {"describe", CreateDescribeTool({.Database = settings.Database, .Driver = settings.Driver})},
        {"ydb_help", CreateYdbHelpTool({.Driver = settings.Driver})},
        {"exec_shell", CreateExecShellTool({.Driver = settings.Driver})},
    };

    for (const auto& [name, tool] : Tools) {
        Model->RegisterTool(name, tool->GetParametersSchema(), tool->GetDescription());
    }
}

} // namespace NYdb::NConsoleClient::NAi
