#include "sql_session_runner.h"
#include "session_runner_common.h"

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/interactive_config.h>
#include <ydb/public/lib/ydb_cli/common/format.h>
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
    TSqlSessionRunner(const TSqlSessionSettings& settings, const TInteractiveLogger& log)
        : TBase(CreateSessionSettings(settings), log)
        , QueryPlanPrinter(EDataFormat::Default)
        , ExplainRunner(settings.Driver)
        , ExecuteRunner(settings.Driver)
    {}

    void HandleLine(const TString& line) final {
        Y_DEFER { ResetInterrupted(); };

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
        ExecuteRunner.Execute(line, {.Settings = settings});
    }

private:
    static TString CreateHelpMessage() {
        using TLog = TInteractiveLogger;

        return TStringBuilder() << Endl << "YDB CLI Interactive Mode – Hotkeys and Special Commands." << Endl
            << Endl << TLog::EntityName("Hotkeys:") << Endl
            << "  " << TLog::EntityName("Ctrl+T") << " or " << TLog::EntityName("/switch") << ": switch to " << TInteractiveConfigurationManager::ModeToString(TInteractiveConfigurationManager::EMode::AI) << " interactive mode." << Endl
            << "  " << TLog::EntityName("TAB") << ": complete the current word based on YQL syntax." << Endl
            << PrintCommonHotKeys()
            << Endl << TLog::EntityName("Special Commands:") << Endl
            << "  " << Colors.Green() << "SET stats = " << Colors.OldColor() << TLog::EntityName("STATS_MODE") << ": set statistics collection mode, allowed modes: "
            << TLog::EntityName("none") << ", " << TLog::EntityName("basic") << ", " << TLog::EntityName("full") << ", " << TLog::EntityName("profile") << "." << Endl
            << "  " << Colors.Green() << "EXPLAIN" << Colors.OldColor() << " [" << Colors.Green() << "AST" << Colors.OldColor() << "] " << TLog::EntityName("SQL_QUERY") << ": execute query in explain mode and optionally print AST." << Endl
            << Endl << TLog::EntityName("Interactive Commands:") << Endl
            << "  " << TLog::EntityName("/help") << ": print this help message." << Endl
            << Endl << "By default, any input is treated as an YQL query and sent directly to the YDB server." << Endl;
    }

    static TLineReaderSettings CreateSessionSettings(const TSqlSessionSettings& settings) {
        return {
            .Driver = settings.Driver,
            .Database = settings.Database,
            .Prompt = TStringBuilder() << TInteractiveConfigurationManager::ModeToString(TInteractiveConfigurationManager::EMode::YQL) << "> ",
            .HistoryFilePath = TFsPath(settings.YdbPath) / "bin" / "interactive_cli_sql_history.txt",
            .HelpMessage = CreateHelpMessage(),
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
};

} // anonymous namespace

ISessionRunner::TPtr CreateSqlSessionRunner(const TSqlSessionSettings& settings, const TInteractiveLogger& log) {
    return std::make_shared<TSqlSessionRunner>(settings, log);
}

} // namespace NYdb::NConsoleClient
