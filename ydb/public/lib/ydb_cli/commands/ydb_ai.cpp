#include "ydb_ai.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_ai/line_reader.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_ai/models/model_openai.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_ai/tools/exec_query_tool.h>

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
    const auto model = NAi::CreateOpenAiModel({
        .BaseUrl = "https://api.eliza.yandex.net/raw/internal/deepseek/v1", // AI-TODO: KIKIMR-24214 -- configure it
        .ModelId = "deepseek-0324", // AI-TODO: KIKIMR-24214 -- configure it
        .ApiKey = GetEnv("MODEL_TOKEN"), // AI-TODO: KIKIMR-24214 -- configure it
    });

    const auto sqlTool = NAi::CreateExecQueryTool(config);  // AI-TODO: more generic tools registration
    model->RegisterTool(sqlTool->GetName(), sqlTool->GetParametersSchema(), sqlTool->GetDescription());

    // AI-TODO: there is strange highlighting of brackets
    while (const auto& maybeLine = lineReader.ReadLine()) {
        TString input = *maybeLine;
        if (input.empty()) {
            continue;
        }

        if (IsIn({"quit", "exit"}, to_lower(input))) {
            PrintExitMessage();
            return EXIT_SUCCESS;
        }

        // AI-TODO: limit interaction number
        std::optional<TString> toolCallId;
        while (input) {
            // AI-TODO: progress visualization
            auto output = model->HandleMessage(input);
            Y_ENSURE(output.Text || output.ToolCall);

            if (output.Text) {
                // AI-TODO: proper answer format
                Cout << "Model answer:\n" << *output.Text << Endl;
            }

            if (!output.ToolCall) {
                break;
            }

            const auto& toolCall = *output.ToolCall;
            if (toolCall.Name != sqlTool->GetName()) {
                // AI-TODO: proper wrong tool handling
                Cout << "Unsupported tool: " << toolCall.Name << Endl;
                return EXIT_FAILURE;
            }

            // AI-TODO: ask permission before call and show progress
            toolCallId = toolCall.Id;
            input = sqlTool->Execute(toolCall.Parameters);
        }
    }

    PrintExitMessage();
    return EXIT_SUCCESS;
}

} // namespace NYdb::NConsoleClient