#include "ydb_ai.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_ai/line_reader.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_ai/models/model_anthropic.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_ai/models/model_openai.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_ai/tools/exec_query_tool.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_ai/tools/list_directory_tool.h>

#include <util/string/strip.h>
#include <util/system/env.h>

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
    Y_UNUSED(config);

    Cout << "AI-TODO: KIKIMR-24198 -- welcome message" << Endl;

    // AI-TODO: KIKIMR-24202 - robust file creation
    NAi::TLineReader lineReader("ydb-ai> ", (TFsPath(HomeDir) / ".ydb-ai/history").GetPath());

    // DeepSeek
    // const auto model = NAi::CreateOpenAiModel({
    //     .BaseUrl = "https://api.eliza.yandex.net/raw/internal/deepseek", // AI-TODO: KIKIMR-24214 -- configure it
    //     .ModelId = "deepseek-0324", // AI-TODO: KIKIMR-24214 -- configure it
    //     .ApiKey = GetEnv("MODEL_TOKEN"), // AI-TODO: KIKIMR-24214 -- configure it
    // });

    const auto model = NAi::CreateAnthropicModel({
        .BaseUrl = "https://api.eliza.yandex.net/anthropic", // AI-TODO: KIKIMR-24214 -- configure it
        .ModelId = "claude-3-5-haiku-20241022",
        .ApiKey = GetEnv("MODEL_TOKEN"), // AI-TODO: KIKIMR-24214 -- configure it
        .MaxTokens = 2048,   // AI-TODO configure it
    });

    // YandexGPT Pro
    // const auto model = NAi::CreateOpenAiModel({
    //     .BaseUrl = "https://api.eliza.yandex.net/internal/zeliboba/32b_aligned_quantized_202506/generative", // AI-TODO: KIKIMR-24214 -- configure it
    //     .ApiKey = GetEnv("MODEL_TOKEN"), // AI-TODO: KIKIMR-24214 -- configure it
    // });

    std::unordered_map<TString, NAi::ITool::TPtr> tools;

    const auto sqlTool = NAi::CreateExecQueryTool(config);
    tools.emplace(sqlTool->GetName(), sqlTool);

    const auto lsTool = NAi::CreateListDirectoryTool(config);
    tools.emplace(lsTool->GetName(), lsTool);

    for (const auto& [name, tool] : tools) {
        model->RegisterTool(name, tool->GetParametersSchema(), tool->GetDescription());
    }

    // AI-TODO: there is strange highlighting of brackets
    std::vector<NAi::IModel::TRequest> requests;
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
        requests.push_back({.Text = input});
        while (!requests.empty()) {
            // AI-TODO: progress visualization
            auto output = model->HandleMessages(requests);
            requests.clear();
            Y_ENSURE(output.Text || !output.ToolCalls.empty());

            if (output.Text) {
                // AI-TODO: proper answer format
                // AI-TODO: how can I render markdown?
                Cout << "Model answer:\n" << *output.Text << Endl;
            }

            for (const auto& toolCall : output.ToolCalls) {
                const auto it = tools.find(toolCall.Name);
                if (it == tools.end()) {
                    // AI-TODO: proper wrong tool handling
                    Cout << "Unsupported tool: " << toolCall.Name << Endl;
                    return EXIT_FAILURE;
                }

                // AI-TODO: ask permission before call and show progress
                requests.push_back({
                    .Text = it->second->Execute(toolCall.Parameters),
                    .ToolCallId = toolCall.Id
                });
            }
        }
    }

    PrintExitMessage();
    return EXIT_SUCCESS;
}

} // namespace NYdb::NConsoleClient