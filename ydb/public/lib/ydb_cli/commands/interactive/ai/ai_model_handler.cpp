#include "ai_model_handler.h"

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/interactive_log_defs.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/ai/models/model_anthropic.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/ai/models/model_openai.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/ai/tools/exec_query_tool.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/ai/tools/list_directory_tool.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/ai/tools/describe_tool.h>

#include <util/string/strip.h>

namespace NYdb::NConsoleClient::NAi {

namespace {

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

- Formating of tool params
- Approving before tool use + rewriting request
- Config time validation and advanced suggestions
- Streamable model response printing
- Progress printing
- Think about robust
- Provide system prompt
- Somehow render markdown

*/

TModelHandler::TModelHandler(const TSettings& settings, const TInteractiveLogger& log)
    : Log(log)
{
    SetupModel(settings.Profile);
    SetupTools(settings);
}

void TModelHandler::HandleLine(const TString& input, std::function<void()> onStartWaiting, std::function<void()> onFinishWaiting) {
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
            Cerr << Colors.Red() << "Failed to perform model API request: " << e.what() << Colors.OldColor() << Endl;
            break;
        }

        if (!output.Text && output.ToolCalls.empty()) {
            Cout << Colors.Yellow() << "Model answer is empty, try to reformulate question." << Colors.OldColor() << Endl;
            break;
        }

        if (output.Text) {
            Cout << Endl << StripStringRight(output.Text) << Endl;
        }

        std::vector<TString> userMessages;
        for (const auto& toolCall : output.ToolCalls) {
            NAi::IModel::TToolResponse response = {.ToolCallId = toolCall.Id};

            if (const auto it = Tools.find(toolCall.Name); it != Tools.end()) {
                auto result = it->second->Execute(toolCall.Parameters);
                if (result.UserMessage) {
                    YDB_CLI_LOG(Debug, "User message during tool call: " << result.ToolResult);
                    userMessages.emplace_back(std::move(result.UserMessage));
                }
                if (!result.IsSuccess) {
                    YDB_CLI_LOG(Warning, "Tool call failed: " << result.ToolResult);
                }
                response.IsSuccess = result.IsSuccess;
                response.Text = std::move(result.ToolResult);
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
            Model = CreateOpenAiModel({.BaseUrl = endpoint, .ModelId = modelName, .ApiKey = apiKey}, Log);
            break;
        case TInteractiveConfigurationManager::EAiApiType::Anthropic:
            Model = CreateAnthropicModel({.BaseUrl = endpoint, .ModelId = modelName, .ApiKey = apiKey}, Log);
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
