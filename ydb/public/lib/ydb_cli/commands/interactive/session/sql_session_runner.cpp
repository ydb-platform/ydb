#include "sql_session_runner.h"
#include "session_runner_common.h"

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/config_ui.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/interactive_config.h>
#include <ydb/public/lib/ydb_cli/common/format.h>
#include <ydb/public/lib/ydb_cli/common/ftxui.h>
#include <ydb/public/lib/ydb_cli/common/query_utils.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/query_stats/stats.h>

#include <util/folder/path.h>
#include <util/generic/scope.h>
#include <util/string/builder.h>

namespace NYdb::NConsoleClient {

namespace {

class TSqlSessionRunner final : public TSessionRunnerBase, public TInterruptableCommand {
    using TBase = TSessionRunnerBase;

    class TLexer {
    public:
        struct TToken {
            std::string_view data;
        };

        explicit TLexer(std::string_view input)
            : Input(input)
            , Position(Input.data())
        {}

        static std::vector<TToken> Tokenize(std::string_view input) {
            std::vector<TToken> tokens;
            TLexer lexer(input);

            while (auto token = lexer.GetNextToken()) {
                tokens.push_back(*token);
            }

            return tokens;
        }

    private:
        std::optional<TToken> GetNextToken() {
            while (Position < Input.end() && std::isspace(*Position)) {
                ++Position;
            }

            if (Position == Input.end()) {
                return {};
            }

            const char* tokenStart = Position;
            if (IsSeparatedTokenSymbol(*Position)) {
                ++Position;
            } else {
                while (Position < Input.end() && !std::isspace(*Position) && !IsSeparatedTokenSymbol(*Position)) {
                    ++Position;
                }
            }

            std::string_view TokenData(tokenStart, Position);
            return TToken{TokenData};
        }

        static bool IsSeparatedTokenSymbol(char c) {
            return c == '=' || c == ';';
        }

    private:
        std::string_view Input;
        const char* Position = nullptr;
    };

public:
    explicit TSqlSessionRunner(const TSqlSessionSettings& settings)
        : TBase(CreateSessionSettings(settings))
        , QueryPlanPrinter(EDataFormat::Default)
        , ExplainRunner(settings.Driver)
        , ExecuteRunner(settings.Driver)
        , EnableAiInteractive(settings.EnableAiInteractive)
    {}

    void HandleLine(const TString& line) final {
        Y_DEFER { ResetInterrupted(); };

        if (to_lower(line) == "/help") {
            Cout << Endl;
            PrintFtxuiMessage(CreateHelpMessage(EnableAiInteractive), "YDB CLI Interactive Mode – Hotkeys and Special Commands", ftxui::Color::White);
            Cout << Endl;
            return;
        }

        if (to_lower(line) == "/config") {
            TConfigUI::TContext ctx{
                .LineReader = LineReader,
            };
            TConfigUI::Run(ctx);
            return;
        }

        const auto& tokens = TLexer::Tokenize(line);
        if (tokens.empty()) {
            return;
        }

        if (to_lower(TString(tokens[0].data)) == "set") {
            ParseSetCommand(tokens);
            return;
        }

        if (to_lower(TString(tokens[0].data)) == "explain") {
            size_t tokensSize = tokens.size();
            bool printAst = tokensSize >= 2 && to_lower(TString(tokens[1].data)) == "ast";
            size_t skipTokens = 1 + printAst;
            TString explainQuery;

            for (size_t i = skipTokens; i < tokensSize; ++i) {
                explainQuery += tokens[i].data;
                explainQuery += ' ';
            }

            const auto& result = ExplainRunner.Explain(explainQuery);
            if (printAst) {
                Cout << "Query AST:" << Endl << result.Ast << Endl;
            } else {
                Cout << "Query Plan:" << Endl;
                QueryPlanPrinter.Print(result.PlanJson);
            }

            return;
        }

        NQuery::TExecuteQuerySettings settings;
        settings.StatsMode(CollectStatsMode);
        settings.ConcurrentResultSets(false);
        ExecuteRunner.Execute(line, {.Settings = settings, .AddIndent = true});
    }

private:
    static ftxui::Element CreateHelpMessage(bool enableAiInteractive) {
        using namespace ftxui;

        std::vector<ftxui::Element> elements = {
            paragraph("By default, any input is treated as an YQL query and sent directly to the YDB server."),
            text(""),
            CreateEntityName("Hotkeys:"),
        };

        if (enableAiInteractive) {
            elements.emplace_back(
                CreateListItem(hbox({
                    CreateEntityName("Ctrl+T"), text(" or "), CreateEntityName("/switch"),
                    text(": switch to "),
                    text(ToString(TInteractiveConfigurationManager::EMode::AI)) | color(Color::Cyan),
                    text(" interactive mode.")
                }))
            );
        }

        elements.emplace_back(CreateListItem(hbox({
            CreateEntityName("TAB"), text(": complete the current word based on YQL syntax.")
        })));

        PrintCommonHotKeys(elements);

        elements.emplace_back(text(""));
        elements.emplace_back(CreateEntityName("Special Commands:"));

        const auto keyword = [](const std::string& s) { return text(s) | color(Color::Green); };
        elements.emplace_back(CreateListItem(hbox({
            keyword("SET stats = "), CreateEntityName("STATS_MODE"),
            text(": set statistics collection mode, allowed modes: "),
            CreateEntityName("none"), text(", "), CreateEntityName("basic"), text(", "),
            CreateEntityName("full"), text(", "), CreateEntityName("profile"), text(".")
        })));

        elements.emplace_back(CreateListItem(hbox({
            keyword("EXPLAIN"), text(" ["), keyword("AST"), text("] "), CreateEntityName("SQL_QUERY"),
            text(": execute query in explain mode and optionally print AST.")
        })));

        elements.emplace_back(text(""));
        elements.emplace_back(CreateEntityName("Interactive Commands:"));
        elements.emplace_back(CreateListItem(hbox({
            CreateEntityName("/config"), text(": change interactive mode settings (hints, color theme, colors mode).")
        })));
        PrintCommonInteractiveCommands(elements);

        return vbox(elements);
    }

    static TLineReaderSettings CreateSessionSettings(const TSqlSessionSettings& settings) {
        TString placeholder = "Type YQL query (Enter to execute, Ctrl+J for newline";
        if (settings.EnableAiInteractive) {
            placeholder += ", Ctrl+T for AI mode";
        }
        placeholder += ", Ctrl+D to exit)";

        return {
            .Driver = settings.Driver,
            .Database = settings.Database,
            .Prompt = TStringBuilder() << TInteractiveConfigurationManager::ModeToString(TInteractiveConfigurationManager::EMode::YQL) << "> ",
            .HistoryFilePath = TFsPath(HomeDir) / ".ydb_history",
            .AdditionalCommands = {"/help", "/config"},
            .Placeholder = placeholder,
            .EnableSwitchMode = settings.EnableAiInteractive,
        };
    }

    void ParseSetCommand(const std::vector<TLexer::TToken>& tokens) {
        if (tokens.size() == 1) {
            Cerr << "Missing variable name for \"SET\" special command." << Endl;
        } else if (tokens.size() == 2 || tokens[2].data != "=") {
            Cerr << "Missing \"=\" symbol for \"SET\" special command." << Endl;
        } else if (tokens.size() == 3) {
            Cerr << "Missing variable value for \"SET\" special command." << Endl;
        } else if (to_lower(TString(tokens[1].data)) == "stats") {
            TrySetCollectStatsMode(tokens);
        } else {
            Cerr << "Unknown variable name \"" << tokens[1].data << "\" for \"SET\" special command." << Endl;
        }
    }

    void TrySetCollectStatsMode(const std::vector<TLexer::TToken>& tokens) {
        size_t tokensSize = tokens.size();
        Y_VALIDATE(tokensSize >= 4, "Not enough tokens for \"SET stats\" special command.");

        if (tokensSize > 4) {
            Cerr << "Variable value for \"SET stats\" special command should contain exactly one token." << Endl;
            return;
        }

        auto statsMode = NQuery::ParseStatsMode(tokens[3].data);
        if (!statsMode) {
            Cerr << "Unknown stats collection mode: \"" << tokens[3].data << "\"." << Endl;
        }

        CollectStatsMode = *statsMode;
    }

private:
    TQueryPlanPrinter QueryPlanPrinter;
    TExplainGenericQuery ExplainRunner;
    TExecuteGenericQuery ExecuteRunner;
    NQuery::EStatsMode CollectStatsMode = NQuery::EStatsMode::None;
    bool EnableAiInteractive;
};

} // anonymous namespace

ISessionRunner::TPtr CreateSqlSessionRunner(const TSqlSessionSettings& settings) {
    return std::make_shared<TSqlSessionRunner>(settings);
}

} // namespace NYdb::NConsoleClient
