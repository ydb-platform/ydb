#include "ai_model_handler.h"

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/interactive_log_defs.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/ai/models/model_anthropic.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/ai/models/model_openai.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/ai/tools/exec_query_tool.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/ai/tools/list_directory_tool.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/ai/tools/describe_tool.h>

#include <util/string/strip.h>
#include <util/string/printf.h>

namespace NYdb::NConsoleClient {
    void PrintFtxuiMessage(const TString& message, const TString& title = "");
}

namespace NYdb::NConsoleClient::NAi {

namespace {

constexpr char SYSTEM_PROMPT[] = R"(You are an intelligent assistant working in a CLI terminal. Your output is rendered directly to the console.
You have access to tools to interact with the database.

CRITICAL RULES:
1. NEVER guess table names, column names, column types, or primary keys.
2. If you need to write a SQL query and you don't know the exact schema (column names, types, primary keys) of the table, you MUST use the `describe` tool first to inspect the table.
3. Only use column names and types that you have confirmed exist via `describe` or from previous query results.

INTERACTION GUIDELINES:
- For simple requests (e.g., "list tables", "describe table X"), just execute the tool.
- For complex requests (e.g., "delete all users over 50", "calculate statistics"), PROPOSE A PLAN first.
  - List the steps you intend to take.
  - Ask the user for confirmation or clarification if the request is ambiguous.
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

TModelHandler::TModelHandler(const TSettings& settings, const TInteractiveLogger& log)
    : Log(log)
{
    SetupModel(settings.Profile);
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
            Cerr << Colors.Red() << "Failed to perform model API request: " << e.what() << Colors.OldColor() << Endl;
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
        }

        std::vector<TString> userMessages;
        for (const auto& toolCall : output.ToolCalls) {
            NAi::IModel::TToolResponse response = {.ToolCallId = toolCall.Id};

            if (const auto it = Tools.find(toolCall.Name); it != Tools.end()) {
                std::optional<NAi::ITool::TResponse> result;
                try {
                    result.emplace(it->second->Execute(toolCall.Parameters));
                } catch (const yexception& e) {
                    if (TString(e.what()).Contains("Interrupted by user")) {
                        response.IsSuccess = false;
                        response.Text = "Tool execution interrupted by user.";
                        Model->AddMessage(response);
                        messages.clear();
                        break;
                    }
                    throw;
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
            } else {
                YDB_CLI_LOG(Warning, "Call to unknown tool: " << toolCall.Name);
                response.IsSuccess = false;
                response.Text = TStringBuilder() << "Call to unknown tool: " << toolCall.Name << ". Only allowed tools: " << PrintToolsNames(Tools);
            }

            messages.emplace_back(std::move(response));
        }

        for (auto& message : userMessages) {
            messages.emplace_back(IModel::TUserMessage{.Text = std::move(message)});
        }
        userMessages.clear();
    }

    Cout << Endl;
}

void TModelHandler::ClearContext() {
    Y_VALIDATE(Model, "Model must be initialized before handling clearing context");
    Model->ClearContext();
}

void TModelHandler::SetupModel(TInteractiveConfigurationManager::TAiProfile::TPtr profile) {
    Y_VALIDATE(profile, "AI profile must be initialized");

    TString ValidationError;
    Y_VALIDATE(profile->IsValid(ValidationError), "AI profile must be valid, but got: " << ValidationError);

    const auto apiType = profile->GetApiType();
    Y_VALIDATE(apiType, "AI profile must have API type");

    const auto& endpoint = profile->GetApiEndpoint();
    Y_VALIDATE(endpoint, "AI profile must have API endpoint");

    const auto& apiKey = profile->GetApiToken();
    const auto& modelName = profile->GetModelName();

    switch (*apiType) {
        case TInteractiveConfigurationManager::EAiApiType::OpenAI:
            Model = CreateOpenAiModel({.BaseUrl = endpoint, .ModelId = modelName, .ApiKey = apiKey, .SystemPrompt = SYSTEM_PROMPT}, Log);
            break;
        case TInteractiveConfigurationManager::EAiApiType::Anthropic:
            Model = CreateAnthropicModel({.BaseUrl = endpoint, .ModelId = modelName, .ApiKey = apiKey, .SystemPrompt = SYSTEM_PROMPT}, Log);
            break;
        case TInteractiveConfigurationManager::EAiApiType::Invalid:
            Y_VALIDATE(false, "Invalid API type: " << *apiType);
    }
}

void TModelHandler::SetupTools(const TSettings& settings) {
    Y_VALIDATE(Model, "Model must be initialized before initializing tools");

    Tools = {
        {"list_directory", NAi::CreateListDirectoryTool({.Database = settings.Database, .Driver = settings.Driver}, Log)},
        {"exec_query", NAi::CreateExecQueryTool({.Prompt = settings.Prompt, .Database = settings.Database, .Driver = settings.Driver}, Log)},
        {"describe", NAi::CreateDescribeTool({.Database = settings.Database, .Driver = settings.Driver}, Log)},
    };

    for (const auto& [name, tool] : Tools) {
        Model->RegisterTool(name, tool->GetParametersSchema(), tool->GetDescription());
    }
}

} // namespace NYdb::NConsoleClient::NAi
