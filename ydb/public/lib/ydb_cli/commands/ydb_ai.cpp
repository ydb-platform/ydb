#include "ydb_ai.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_ai/common/json_utils.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_ai/line_reader.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_ai/models/model_anthropic.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_ai/models/model_openai.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_ai/tools/exec_query_tool.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_ai/tools/list_directory_tool.h>

#include <util/string/strip.h>
#include <util/system/env.h>

/*

FEATURES-TODO:

- Config time validation and advanced suggestions
- Streamable model response printing
- Streamable results printing
- Adjusting errors, progress and response printing
- Approving before tool use
- Integration into common interactive mode
- Think about helps
- Think about robust
- Provide system promt
- Somehow render markdown

*/

namespace NYdb::NConsoleClient {

namespace {

void PrintExitMessage() {
    Cout << "\nBye" << Endl;
}

} // anonymous namespace

TCommandAi::TCommandAi()
    : TBase("ai", {}, "AI-TODO: KIKIMR-24198 -- description")
{}

void TCommandAi::Config(TConfig& config) {
    TBase::Config(config);
    config.Opts->SetTitle("AI-TODO: KIKIMR-24198 -- title");
    config.Opts->SetFreeArgsNum(0);
}

int TCommandAi::Run(TConfig& config) {
    Cout << "AI-TODO: KIKIMR-24198 -- welcome message" << Endl;

    // AI-TODO: KIKIMR-24202 - robust file creation
    NAi::TLineReader lineReader("ydb-ai> ", (TFsPath(HomeDir) / ".ydb-ai/history").GetPath());

    // DeepSeek
    // const auto model = NAi::CreateOpenAiModel({
    //     .BaseUrl = "https://api.eliza.yandex.net/raw/internal/deepseek", // AI-TODO: KIKIMR-24214 -- configure it
    //     .ModelId = "deepseek-0324", // AI-TODO: KIKIMR-24214 -- configure it
    //     .ApiKey = GetEnv("MODEL_TOKEN"), // AI-TODO: KIKIMR-24214 -- configure it
    // }, config);

    // Claude 3.5 haiku
    // const auto model = NAi::CreateAnthropicModel({
    //     .BaseUrl = "https://api.eliza.yandex.net/anthropic", // AI-TODO: KIKIMR-24214 -- configure it
    //     .ModelId = "claude-3-5-haiku-20241022",
    //     .ApiKey = GetEnv("MODEL_TOKEN"), // AI-TODO: KIKIMR-24214 -- configure it
    // }, config);

    // YandexGPT Pro
    const auto model = NAi::CreateOpenAiModel({
        .BaseUrl = "https://api.eliza.yandex.net/internal/zeliboba/32b_aligned_quantized_202506/generative", // AI-TODO: KIKIMR-24214 -- configure it
        .ApiKey = GetEnv("MODEL_TOKEN"), // AI-TODO: KIKIMR-24214 -- configure it
    }, config);

    std::unordered_map<TString, NAi::ITool::TPtr> tools = {
        {"execute_query", NAi::CreateExecQueryTool(config)},
        {"list_directory", NAi::CreateListDirectoryTool(config)},
    };
    for (const auto& [name, tool] : tools) {
        model->RegisterTool(name, tool->GetParametersSchema(), tool->GetDescription());
    }

    // AI-TODO: there is strange highlighting of brackets
    std::vector<NAi::IModel::TMessage> messages;
    while (const auto& maybeLine = lineReader.ReadLine()) {
        const auto& input = *maybeLine;
        if (input.empty()) {
            continue;
        }

        if (IsIn({"quit", "exit"}, to_lower(input))) {
            PrintExitMessage();
            return EXIT_SUCCESS;
        }

        // AI-TODO: limit interaction number
        messages.emplace_back(NAi::IModel::TUserMessage{.Text = input});
        while (!messages.empty()) {
            // AI-TODO: progress visualization
            const auto output = model->HandleMessages(messages);
            messages.clear();

            if (!output.Text && output.ToolCalls.empty()) {
                // AI-TODO: proper answer format
                Cout << "Model answer is empty(" << Endl;
                break;
            }

            if (output.Text) {
                // AI-TODO: proper answer format
                Cout << "Model answer:\n" << output.Text << Endl;
            }

            for (const auto& toolCall : output.ToolCalls) {
                const auto it = tools.find(toolCall.Name);
                if (it == tools.end()) {
                    // AI-TODO: proper wrong tool handling
                    Cout << "Unsupported tool: " << toolCall.Name << Endl;
                    return EXIT_FAILURE;
                }

                // AI-TODO: proper tool call printing
                Cout << "Calling tool: " << toolCall.Name << " with params:\n" << NAi::FormatJsonValue(toolCall.Parameters) << Endl;

                // AI-TODO: add approving
                const auto& result = it->second->Execute(toolCall.Parameters);
                if (!result.IsSuccess) {
                    // AI-TODO: proper error handling
                    Cout << result.Text << Endl;
                }
                // AI-TODO: show progress

                messages.push_back(NAi::IModel::TToolResponse{
                    .Text = result.Text,
                    .ToolCallId = toolCall.Id,
                    .IsSuccess = result.IsSuccess,
                });
            }
        }
    }

    PrintExitMessage();
    return EXIT_SUCCESS;
}

} // namespace NYdb::NConsoleClient