#include "ai_model_handler.h"

#include <ydb/core/base/validation.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/ai/models/model_anthropic.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/ai/models/model_openai.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/ai/tools/list_directory_tool.h>

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

void TModelHandler::HandleLine(const TString& input) {
    Y_DEBUG_VERIFY(Model, "Model must be initialized before handling input");

    if (!input) {
        return;
    }

    std::vector<IModel::TMessage> messages = {IModel::TUserMessage{.Text = input}};
    while (!messages.empty()) {
        IModel::TResponse output;
        try {
            output = Model->HandleMessages(messages);
            messages.clear();
        } catch (const std::exception& e) {
            Cerr << Colors.Red() << "Failed to perform model API request. "
                << Colors.OldColor() << "Use " << Colors.BoldColor() << "Ctrl+G" << Colors.OldColor()
                << " to change model settings. Error reason: " << e.what() << Endl;
            break;
        }

        if (!output.Text && output.ToolCalls.empty()) {
            Cout << Colors.Yellow() << "Model answer is empty, try to reformulate question." << Colors.OldColor() << Endl;
            break;
        }

        if (output.Text) {
            Cout << Endl << output.Text << Endl << Endl;
        }

        for (const auto& toolCall : output.ToolCalls) {
            NAi::IModel::TToolResponse response = {.ToolCallId = toolCall.Id};

            if (const auto it = Tools.find(toolCall.Name); it != Tools.end()) {
                const auto& result = it->second->Execute(toolCall.Parameters);
                if (!result.IsSuccess) {
                    Log.Warning() << "Tool call failed: " << result.Text;
                }
                response.IsSuccess = result.IsSuccess;
                response.Text = result.Text;
            } else {
                Log.Warning() << "Call to unknown tool: " << toolCall.Name;
                response.IsSuccess = false;
                response.Text = TStringBuilder() << "Call to unknown tool: " << toolCall.Name << ". Only allowed tools: " << PrintToolsNames(Tools);
                messages.push_back(response);
            }

            messages.emplace_back(std::move(response));
        }
    }
}

void TModelHandler::ClearContext() {
    Y_DEBUG_VERIFY(Model, "Model must be initialized before handling clearing context");
    Model->ClearContext();
}

void TModelHandler::SetupModel(TInteractiveConfigurationManager::TAiProfile::TPtr profile) {
    Y_DEBUG_VERIFY(profile, "AI profile must be initialized");

    TString ValidationError;
    Y_DEBUG_VERIFY(profile->IsValid(ValidationError), "AI profile must be valid, but got: %s", ValidationError.c_str());

    const auto apiType = profile->GetApiType();
    Y_DEBUG_VERIFY(apiType, "AI profile must have API type");

    const auto& endpoint = profile->GetApiEndpoint();
    Y_DEBUG_VERIFY(endpoint, "AI profile must have API endpoint");

    const auto& apiKey = profile->GetApiToken();
    const auto& modelName = profile->GetModelName();

    switch (*apiType) {
        case TInteractiveConfigurationManager::TAiProfile::EApiType::OpenAI:
            Model = CreateOpenAiModel({.BaseUrl = endpoint, .ModelId = modelName, .ApiKey = apiKey}, Log);
            break;
        case TInteractiveConfigurationManager::TAiProfile::EApiType::Anthropic:
            Model = CreateAnthropicModel({.BaseUrl = endpoint, .ModelId = modelName, .ApiKey = apiKey}, Log);
            break;
        case TInteractiveConfigurationManager::TAiProfile::EApiType::Invalid:
            Y_DEBUG_VERIFY(false, "Invalid API type: %s", ToString(*apiType).c_str());
    }
}

void TModelHandler::SetupTools(const TSettings& settings) {
    Y_DEBUG_VERIFY(Model, "Model must be initialized before initializing tools");

    Tools = {
        {"list_directory", NAi::CreateListDirectoryTool({.Database = settings.Database, .Driver = settings.Driver}, Log)},
    };

    for (const auto& [name, tool] : Tools) {
        Model->RegisterTool(name, tool->GetParametersSchema(), tool->GetDescription());
    }
}

} // namespace NYdb::NConsoleClient::NAi
