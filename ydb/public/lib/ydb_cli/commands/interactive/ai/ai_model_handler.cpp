#include "ai_model_handler.h"

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/lib/ydb_cli/common/log.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/ai/models/model_anthropic.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/ai/models/model_openai.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/ai/tools/exec_query_tool.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/ai/tools/exec_shell_tool.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/ai/tools/explain_query_tool.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/ai/tools/list_directory_tool.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/ai/tools/describe_tool.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/ai/tools/docs_search_tool.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/ai/tools/ydb_help_tool.h>
#include <ydb/public/lib/ydb_cli/common/ftxui.h>

#include <contrib/libs/ftxui/include/ftxui/dom/table.hpp>
#include <contrib/libs/ftxui/include/ftxui/screen/terminal.hpp>

#include <library/cpp/json/json_writer.h>

#include <util/charset/utf8.h>
#include <util/string/printf.h>
#include <util/string/split.h>
#include <util/string/strip.h>
#include <util/string/subst.h>
#include <util/system/env.h>

namespace NYdb::NConsoleClient::NAi {

namespace {

constexpr char SYSTEM_PROMPT[] = R"(You are an intelligent assistant working in a CLI terminal.
You have access to tools to interact with the YDB database.

*** IMPORTANT: YOU DO NOT KNOW THE YDB CLI COMMAND SYNTAX. ***
*** YOU MUST DISCOVER IT USING TOOLS. DO NOT HALLUCINATE COMMANDS. ***

OUTPUT FORMATTING (this matters as much as the rules below):
- Your reply is printed verbatim into a plain terminal. It is NOT rendered, so any markup shows up as raw characters and looks broken to the user.
- Never use Markdown or any other markup: no ** or __ for bold, no # headings, no backticks, no triple-backtick code fences, no Markdown tables built from | and ---.
- This holds even when you reuse documentation or tool output: that text is usually Markdown, so do not pass it through. Extract the facts and rewrite them as plain text.
- Structure answers with plain text only: short paragraphs separated by a blank line, an UPPERCASE or "Word:" prefix for headings, and "- " for list items (one per line).
- Do not align text into columns with spaces, the terminal collapses repeated spaces. Present tabular data as one "- field: value" line per field, or one record per line, never as a table.
- Be concise. Put any query or command on its own line, without fences.

CRITICAL EXECUTION RULES:

1. MANDATORY DISCOVERY: You are PROHIBITED from executing any ydb shell command unless you have successfully run ydb_help with empty arguments and then ydb_help for a subcommand that you are going to use in this session to verify its syntax.
   - WRONG: "I will import..." then exec_shell("ydb import ...") (HALLUCINATION - STOP!)
   - CORRECT: "I need to check import syntax..." then ydb_help(), then ydb_help("import"), then exec_shell("ydb import file"), etc.

2. UNKNOWN SCHEMA: You are PROHIBITED from writing SQL queries or importing data without first inspecting the table schema using describe.

3. UNKNOWN DATA VALUES: You are PROHIBITED from filtering data (WHERE clause) using guessed values.
   - WRONG: Directly using a guessed value in WHERE clause (e.g., WHERE status = 'some_guess') without knowing if it exists.
   - CORRECT: First inspect the data (e.g., SELECT DISTINCT column FROM table LIMIT 20) to see actual values, then use them in filtering.

4. CONNECTION PARAMETERS:
   - The user's connection parameters are provided in the [CONTEXT] below.
   - You MUST use ONLY those parameters when using ydb cli in exec_shell tool.
   - NEVER add -p, --profile, --endpoint, etc., unless they are explicitly in the [CONTEXT].

5. VALIDATE YQL BEFORE EXECUTING IT: If you are not 100% certain a YQL query is valid (unfamiliar built-in, complex JOIN/window, multi-statement script, first use of an idiom in this session), you MUST run explain_query BEFORE exec_query. explain_query does not execute the query and does not prompt the user, so you can iterate on errors silently. Only call exec_query after explain_query succeeds — this way the user is prompted once, for a query already known to be valid.

6. CONSULT THE DOCS WHEN UNSURE: If you are not 100% certain how a YQL feature, built-in function, YDB scheme entity, recipe, configuration option, or YDB CLI command works, you MUST use docs_search BEFORE composing a query, running a tool, or answering the user. Treat the docs as authoritative; your prior knowledge of YDB-specific behaviour may be outdated.

STRATEGY FOR ANY REQUEST:
1. Can I use native tools (list_directory, describe, exec_query)? If yes, use them.
2. If not, maybe I can use YDB CLI binary? If I need the YDB CLI binary:
   a. Call ydb_help (empty) to list all available commands.
   b. Call ydb_help <subcommand> to learn syntax.
   c. ONLY THEN construct and execute the exec_shell command.

INTERACTION GUIDELINES:
- For complex tasks propose a plan first.
  1. List the tools you intend to call and the actions you will take.
  2. Ask the user for confirmation if the plan consists of more than 3 actions.
  3. If you asked for confirmation, WAIT for the user's explicit confirmation ("yes", "ok", etc.) before executing ANY tool.
- Once confirmed, proceed with execution.
- If the user's request implies deleting or modifying data, be extra careful and verify the WHERE clause logic by inspecting the schema first.
- If a tool returns "skipped" status or "User skipped execution", DO NOT treat it as an error. Do NOT apologize. Just consider it as a user request to skip the tool execution. Do not output verbose confirmations like "I acknowledge that the user skipped". Proceed directly to the next logical step or ask what to do next.

REMINDER: answer in plain terminal text only, with no Markdown — this applies especially when you summarize documentation or tool output.
)";

TStringBuf TrimSpaces(TStringBuf s) {
    while (!s.empty() && (s.front() == ' ' || s.front() == '\t')) {
        s.Skip(1);
    }
    while (!s.empty() && (s.back() == ' ' || s.back() == '\t')) {
        s.Chop(1);
    }
    return s;
}

// ASCII alphanumerics, '_' and any UTF-8 lead/continuation byte (>= 0x80, e.g. Cyrillic) count
// as "word" bytes, so emphasis markers glued to a word are not mistaken for formatting.
bool IsWordByte(unsigned char c) {
    return ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || ('0' <= c && c <= '9') || c == '_' || c >= 0x80;
}

// A line made only of '|', '-', ':' and spaces (with at least one '-') is a Markdown table
// separator or a horizontal rule. Such lines carry no information and only add noise.
bool IsTableSeparatorLine(TStringBuf trimmed) {
    if (trimmed.empty()) {
        return false;
    }
    bool hasDash = false;
    for (const char c : trimmed) {
        if (c == '-') {
            hasDash = true;
        } else if (c != '|' && c != ':' && c != ' ') {
            return false;
        }
    }
    return hasDash;
}

// Remove paired single-char emphasis markers ('*' or '_') while keeping markers that are part of
// the text: "SELECT *" (marker followed by a space) or "snake_case" (marker glued to a word on
// both sides). Only a marker that opens right before non-space text and closes right after
// non-space text, on a word boundary, is treated as emphasis and dropped.
void RemovePairedEmphasis(TString& s, char marker) {
    TString out;
    out.reserve(s.size());
    const size_t n = s.size();
    for (size_t i = 0; i < n;) {
        if (s[i] == marker) {
            const char prevOut = out.empty() ? '\0' : out.back();
            const char next = (i + 1 < n) ? s[i + 1] : '\0';
            const bool canOpen = !IsWordByte(prevOut) && prevOut != marker
                && next != '\0' && next != ' ' && next != '\t' && next != marker;
            if (canOpen) {
                size_t close = TString::npos;
                for (size_t j = i + 1; j < n; ++j) {
                    if (s[j] != marker) {
                        continue;
                    }
                    const char cprev = s[j - 1];
                    const char cnext = (j + 1 < n) ? s[j + 1] : '\0';
                    if (cprev != ' ' && cprev != '\t' && cprev != marker && !IsWordByte(cnext) && cnext != marker) {
                        close = j;
                        break;
                    }
                }
                if (close != TString::npos) {
                    for (size_t k = i + 1; k < close; ++k) {
                        out.push_back(s[k]);
                    }
                    i = close + 1;
                    continue;
                }
            }
        }
        out.push_back(s[i++]);
    }
    s = std::move(out);
}

TString StripInlineMarkdown(TStringBuf line) {
    size_t start = 0;
    while (start < line.size() && line[start] == ' ') {
        ++start;
    }

    bool strippedBlock = false;
    // Blockquote markers: '>' optionally followed by a space, possibly repeated for nesting.
    while (start < line.size() && line[start] == '>') {
        ++start;
        if (start < line.size() && line[start] == ' ') {
            ++start;
        }
        strippedBlock = true;
    }
    // Heading markers: 1-6 '#' followed by a space.
    size_t hashes = 0;
    while (start + hashes < line.size() && line[start + hashes] == '#') {
        ++hashes;
    }
    if (1 <= hashes && hashes <= 6 && start + hashes < line.size() && line[start + hashes] == ' ') {
        start += hashes + 1;
        strippedBlock = true;
    }

    // Keep the original leading spaces unless a block marker was stripped.
    TString result(strippedBlock ? line.substr(start) : line);
    SubstGlobal(result, "**", "");     // bold
    RemovePairedEmphasis(result, '*'); // italic; keeps "SELECT *" intact
    RemovePairedEmphasis(result, '_'); // italic; keeps snake_case identifiers intact
    SubstGlobal(result, "`", "");      // inline code
    return result;
}

// Split a Markdown table row "| a | b |" into trimmed, Markdown-stripped cells.
std::vector<std::string> SplitTableRow(TStringBuf trimmed) {
    if (trimmed.StartsWith('|')) {
        trimmed.Skip(1);
    }
    if (trimmed.EndsWith('|')) {
        trimmed.Chop(1);
    }

    std::vector<std::string> cells;
    for (const TStringBuf cell : StringSplitter(trimmed).Split('|')) {
        cells.emplace_back(StripInlineMarkdown(TrimSpaces(cell)));
    }
    return cells;
}

// Render collected Markdown table rows (the first row is the header) as a bordered FTXUI table.
ftxui::Element BuildTable(std::vector<std::vector<std::string>> rows) {
    ftxui::Table table(std::move(rows));
    table.SelectAll().Border(ftxui::LIGHT);
    table.SelectAll().SeparatorVertical(ftxui::LIGHT);
    table.SelectAll().SeparatorHorizontal(ftxui::LIGHT);
    table.SelectRow(0).Decorate(ftxui::bold);
    return table.Render();
}

// A code-block bar: a full-width horizontal rule. With a language tag it shows it as
// "── yql ──────", mirroring the "── Agent response ──" header style; without one it is a plain rule.
ftxui::Element BuildCodeBlockBar(TStringBuf language) {
    if (language.empty()) {
        return ftxui::separator() | ftxui::color(ftxui::Color::Grey42);
    }

    int languageWidth = 0;
    for (size_t i = 0; i < language.size(); i += UTF8RuneLen(language[i])) {
        ++languageWidth;
    }

    std::string fill;
    const int screenWidth = ftxui::Terminal::Size().dimx;
    for (int i = 0; i < screenWidth - languageWidth - 4; ++i) { // 4 = "──" + a space on each side
        fill += "─";
    }

    return ftxui::hbox({
        ftxui::text("──") | ftxui::color(ftxui::Color::Grey42),
        ftxui::text(" " + std::string(language) + " ") | ftxui::bold | ftxui::color(ftxui::Color::Grey70),
        ftxui::text(fill) | ftxui::color(ftxui::Color::Grey42),
    });
}

// Render a fenced code block as its own panel: horizontal bars above and below (in their own color,
// the top one carrying the language tag) over a separate, slightly lighter background. Both
// background and foreground are pinned so the panel stays readable on any terminal theme. Lines are
// kept verbatim (indentation preserved, no reflow), unlike prose which paragraph() reflows.
ftxui::Element BuildCodeBlock(const std::vector<TStringBuf>& codeLines, TStringBuf language) {
    std::vector<ftxui::Element> content;
    content.reserve(codeLines.size());
    for (const TStringBuf line : codeLines) {
        content.push_back(ftxui::text(std::string(line)));
    }

    ftxui::Element body = ftxui::vbox(std::move(content))
        | ftxui::xflex                         // fill the whole width so the background spans it
        | ftxui::bgcolor(ftxui::Color::Grey19) // panel background, a touch lighter than Grey11
        | ftxui::color(ftxui::Color::Grey85);  // pinned text color, readable on the dark panel

    return ftxui::vbox({
        BuildCodeBlockBar(language),
        std::move(body),
        ftxui::separator() | ftxui::color(ftxui::Color::Grey42),
    });
}

// The model is instructed to answer in plain text, but it still tends to emit Markdown. Our output
// is printed to the terminal, so convert Markdown to a presentable form before showing it: inline
// markup (headings, bold/italic, inline code, blockquotes) is reduced to plain text, Markdown tables
// are rendered as real bordered tables, and fenced code blocks become highlighted panels. This
// affects only what the user sees: the model's conversation history and the audit log keep the
// original text.
ftxui::Element MarkdownToElement(TStringBuf text) {
    std::vector<TStringBuf> lines;
    for (const TStringBuf line : StringSplitter(text).Split('\n')) {
        lines.push_back(line);
    }

    std::vector<ftxui::Element> blocks;
    TStringBuilder textRun;

    auto flushText = [&]() {
        TString block = textRun;
        textRun.clear();
        while (!block.empty()) {
            const char c = block.back();
            if (c != '\n' && c != '\r' && c != ' ' && c != '\t') {
                break;
            }
            block.pop_back();
        }
        if (!block.empty()) {
            blocks.push_back(ftxui::paragraph(std::string(block)));
        }
    };

    for (size_t i = 0; i < lines.size();) {
        const TStringBuf trimmed = TrimSpaces(lines[i]);

        // Fenced code block: collect everything up to the closing fence and render it as a panel.
        if (trimmed.StartsWith("```")) {
            flushText();

            // The opening fence may carry an info string ("```yql"); its first token is the language.
            TStringBuf language = trimmed;
            while (language.StartsWith('`')) {
                language.Skip(1);
            }
            language = TrimSpaces(language);
            if (const size_t space = language.find(' '); space != TStringBuf::npos) {
                language = language.substr(0, space);
            }

            std::vector<TStringBuf> codeLines;
            for (++i; i < lines.size(); ++i) {
                if (TrimSpaces(lines[i]).StartsWith("```")) {
                    ++i; // consume the closing fence
                    break;
                }
                codeLines.push_back(lines[i]);
            }
            if (!codeLines.empty()) {
                blocks.push_back(BuildCodeBlock(codeLines, language));
            }
            continue;
        }

        // A run of consecutive "| ... |" lines is a Markdown table: render it as a real table.
        if (!IsTableSeparatorLine(trimmed) && trimmed.StartsWith('|')) {
            flushText();
            std::vector<std::vector<std::string>> rows;
            for (; i < lines.size(); ++i) {
                const TStringBuf row = TrimSpaces(lines[i]);
                if (row.StartsWith("```") || (!IsTableSeparatorLine(row) && !row.StartsWith('|'))) {
                    break;
                }
                if (!IsTableSeparatorLine(row)) {
                    rows.push_back(SplitTableRow(row));
                }
            }
            if (!rows.empty()) {
                blocks.push_back(BuildTable(std::move(rows)));
            }
            continue;
        }

        if (!textRun.empty()) {
            textRun << '\n';
        }
        textRun << StripInlineMarkdown(lines[i]);
        ++i;
    }
    flushText();

    if (blocks.empty()) {
        return ftxui::text("");
    }
    if (blocks.size() == 1) {
        return std::move(blocks.front());
    }
    return ftxui::vbox(std::move(blocks));
}

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

TModelHandler::TModelHandler(const TSettings& settings)
    : ConfigurationManager(settings.ConfigurationManager)
    , AuditEnabled(TryGetEnv("YDB_CLI_AI_AUDIT_LOG").Defined())
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
            Cerr << Endl << Colors.Red() << e.what() << Colors.OldColor() << Endl;

            if (AuditEnabled) {
                NJson::TJsonValue auditInfo;
                auditInfo["seq"] = AuditSeq++;
                auditInfo["error"] = e.what();
                YDB_CLI_LOG(Info, "[AUDIT] fatal_error " << NJson::WriteJson(&auditInfo, /* formatOutput = */ false));
            }
            break;
        }

        if (AuditEnabled) {
            NJson::TJsonValue auditInfo;
            auditInfo["seq"] = AuditSeq++;
            auditInfo["input_tokens"] = output.Usage.InputTokens;
            auditInfo["output_tokens"] = output.Usage.OutputTokens;
            auditInfo["cached_input_tokens"] = output.Usage.CachedInputTokens;
            YDB_CLI_LOG(Info, "[AUDIT] model_usage " << NJson::WriteJson(&auditInfo, /* formatOutput = */ false));
        }

        if (!output.Text && output.ToolCalls.empty()) {
            Cout << Colors.Yellow() << "Model answer is empty, try to reformulate question." << Colors.OldColor() << Endl;

            if (AuditEnabled) {
                NJson::TJsonValue auditInfo;
                auditInfo["seq"] = AuditSeq++;
                auditInfo["error"] = "Model answer is empty.";
                YDB_CLI_LOG(Info, "[AUDIT] fatal_error " << NJson::WriteJson(&auditInfo, /* formatOutput = */ false));
            }
            break;
        }

        if (output.Text) {
            TString title = "Agent response";
            if (getThinkingTime) {
                if (double elapsed = getThinkingTime(); elapsed > 0.0) {
                    title += Sprintf(" (after %.2fs)", elapsed);
                }
            }
            ::NYdb::NConsoleClient::PrintFtxuiMessage(MarkdownToElement(StripStringRight(output.Text)), title);

            if (AuditEnabled) {
                NJson::TJsonValue auditInfo;
                auditInfo["seq"] = AuditSeq++;
                auditInfo["text"] = StripStringRight(output.Text);
                YDB_CLI_LOG(Info, "[AUDIT] agent_response " << NJson::WriteJson(&auditInfo, /* formatOutput = */ false));
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
}

void TModelHandler::ClearContext() {
    Y_VALIDATE(Model, "Model must be initialized before handling clearing context");
    Model->ClearContext();
}

IModel::TToolResponse TModelHandler::CallTool(const IModel::TResponse::TToolCall& toolCall, std::vector<TString>& userMessages, bool& interrupted) {
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
        YDB_CLI_LOG(Notice, "Tool call failed: " << result->ToolResult);
    }

    if (AuditEnabled) {
        NJson::TJsonValue auditInfo;
        auditInfo["seq"] = AuditSeq++;
        auditInfo["name"] = toolCall.Name;
        auditInfo["success"] = result->IsSuccess;
        auditInfo["args"] = toolCall.Parameters;
        auditInfo["result"] = result->ToolResult;
        YDB_CLI_LOG(Info, "[AUDIT] tool_call " << NJson::WriteJson(&auditInfo, /* formatOutput = */ false));
    }

    response.IsSuccess = result->IsSuccess;
    response.Text = std::move(result->ToolResult);

    return response;
}

void TModelHandler::SetupModel(TAiModelConfig::TPtr profile, const TSettings& settings) {
    Y_VALIDATE(profile, "AI profile must be initialized");

    TString ValidationError;
    Y_VALIDATE(profile->IsValid(ValidationError), "AI profile must be valid, but got: " << ValidationError);

    const auto apiType = profile->GetApiType();
    Y_VALIDATE(apiType, "AI profile must have API type");

    const auto& endpoint = profile->GetEndpoint();
    Y_VALIDATE(endpoint, "AI profile must have API endpoint");

    const auto& apiKey = profile->GetApiToken();
    if (!apiKey) {
        throw yexception() << "API key resolving was interrupted";
    }

    const auto& modelName = profile->GetModelName();

    TString systemPrompt = SYSTEM_PROMPT;
    if (!settings.ConnectionString.empty()) {
        systemPrompt += "\n[CONTEXT] The user is connected to YDB with this command line prefix: " + settings.ConnectionString + "\n"
                        "When using exec_shell to run ydb commands, you MUST prepend this prefix (it includes binary path and global options).\n"
                        "Do NOT add any other connection parameters (like -p, --endpoint, --database) unless they are explicitly present in this prefix. If no connection options are provided, it means the environment is already configured (e.g., via default profile or environment variables).\n";
    }

    if (!ConfigurationManager->IsSystemPromptEnabled()) {
        YDB_CLI_LOG(Warning, "System prompt is disabled by configuration");
        systemPrompt = "";
    }

    switch (*apiType) {
        case TAiPresets::EApiType::OpenAI:
            Model = CreateOpenAiModel({.BaseUrl = endpoint, .ModelId = modelName, .ApiKey = *apiKey, .SystemPrompt = systemPrompt});
            break;
        case TAiPresets::EApiType::Anthropic:
            Model = CreateAnthropicModel({.BaseUrl = endpoint, .ModelId = modelName, .ApiKey = *apiKey, .SystemPrompt = systemPrompt});
            break;
        case TAiPresets::EApiType::Max:
            Y_VALIDATE(false, "Invalid API type: " << *apiType);
    }
}

void TModelHandler::SetupTools(const TSettings& settings) {
    Y_VALIDATE(Model, "Model must be initialized before initializing tools");
    Y_VALIDATE(settings.LazyDriver, "TModelHandler requires a non-null LazyDriver in settings");

    for (const auto& [name, tool] : std::vector<std::pair<TString, ITool::TPtr>>{
        {"list_directory", CreateListDirectoryTool({.Database = settings.Database, .LazyDriver = settings.LazyDriver})},
        {"exec_query", CreateExecQueryTool({.Prompt = settings.Prompt, .Database = settings.Database, .LazyDriver = settings.LazyDriver})},
        {"explain_query", CreateExplainQueryTool({.LazyDriver = settings.LazyDriver})},
        {"describe", CreateDescribeTool({.Database = settings.Database, .LazyDriver = settings.LazyDriver})},
        {"ydb_help", CreateYdbHelpTool({.UsageInfoGetter = settings.UsageInfoGetter})},
        {"docs_search", CreateDocsSearchTool()},
        {"exec_shell", CreateExecShellTool({.Prompt = settings.Prompt})},
    }) {
        if (!tool) {
            continue;
        }

        const auto autoAction = settings.ConfigurationManager->GetToolAutoAction(name);
        if (autoAction == TInteractiveConfigurationManager::EToolAutoAction::Hide) {
            YDB_CLI_LOG(Warning, "Skipping tool " << name << " because it is hidden by configuration");
            continue;
        }

        if (autoAction != TInteractiveConfigurationManager::EToolAutoAction::Ask) {
            YDB_CLI_LOG(Warning, "Skipping ask prompt for " << name << " tool, do action: " << autoAction);
            tool->SetAutoAction(autoAction);
        }

        Model->RegisterTool(name, tool->GetParametersSchema(), tool->GetDescription());
        Y_VALIDATE(Tools.emplace(name, tool).second, "Tool " << name << " already registered");
    }
}

} // namespace NYdb::NConsoleClient::NAi
