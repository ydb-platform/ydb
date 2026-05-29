#include "sql_session_runner.h"
#include "session_runner_common.h"

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/config_ui.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/interactive_config.h>
#include <ydb/public/lib/ydb_cli/common/format.h>
#include <ydb/public/lib/ydb_cli/common/ftxui.h>
#include <ydb/public/lib/ydb_cli/common/query_utils.h>
#include <ydb/public/lib/ydb_cli/common/tx_mode_utils.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/query_stats/stats.h>

#include <util/folder/path.h>
#include <util/generic/scope.h>
#include <util/string/builder.h>
#include <util/string/strip.h>

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
        , SqlLazyDriver(settings.SqlLazyDriver)
        , EnableAiInteractive(settings.EnableAiInteractive)
        , EnableInteractiveTransactions(settings.EnableInteractiveTransactions)
        , BasePrompt(Settings.Prompt)
    {
        Y_VALIDATE(SqlLazyDriver, "TSqlSessionRunner requires a non-null SqlLazyDriver");
        Y_VALIDATE(settings.CompleterLazyDriver, "TSqlSessionRunner requires a non-null CompleterLazyDriver");
    }

    ~TSqlSessionRunner() override {
        if (!EnableInteractiveTransactions) {
            return;
        }
        if (Transaction && Transaction->IsActive()) {
            Cerr << Colors.Yellow() << "Warning: open transaction is rolled back on exit." << Colors.OldColor() << Endl;
        }
        RollbackOpenTransaction(/* silent */ true);
    }

    void HandleLine(const TString& line) final {
        Y_DEFER { ResetInterrupted(); };
        // Keep the driver alive while an interactive transaction is open.
        const bool inTransaction = EnableInteractiveTransactions
            && Transaction && Transaction->IsActive();
        if (!inTransaction) {
            Y_DEFER { SqlLazyDriver->Stop(true); };
        }

        if (to_lower(line) == "/help") {
            PrintFtxuiMessage(
                CreateHelpMessage(EnableAiInteractive, EnableInteractiveTransactions),
                "YDB CLI Interactive Mode – Hotkeys and Special Commands",
                ftxui::Color::White);
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

        if (IsBeginCommand(line) || IsCommitCommand(line) || IsRollbackCommand(line)) {
            if (!EnableInteractiveTransactions) {
                PrintInteractiveTransactionsNotAvailable();
                return;
            }
        }

        if (IsBeginCommand(line)) {
            HandleBegin(line);
            return;
        }

        if (IsCommitCommand(line)) {
            HandleCommit();
            return;
        }

        if (IsRollbackCommand(line)) {
            HandleRollback();
            return;
        }

        if (inTransaction) {
            ExecuteInTransaction(line);
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

            TExplainGenericQuery explainRunner(SqlLazyDriver->Get());
            const auto& result = explainRunner.Explain(explainQuery);
            if (printAst) {
                Cout << Colors.Green() << "\nQuery AST:" << Colors.OldColor() << Endl << Endl << Strip(result.Ast) << Endl;
            } else {
                Cout << Colors.Green() << "\nQuery Plan:" << Colors.OldColor() << Endl << Endl;
                QueryPlanPrinter.Print(result.PlanJson);
            }

            return;
        }

        NQuery::TExecuteQuerySettings settings;
        settings.StatsMode(CollectStatsMode);
        settings.ConcurrentResultSets(false);
        if (!ResourcePool.empty()) {
            settings.ResourcePool(ResourcePool);
        }

        try {
            TExecuteGenericQuery executeRunner(SqlLazyDriver->Get());
            executeRunner.Execute(line, {.Settings = settings, .AddIndent = true});
        } catch (NStatusHelpers::TYdbErrorException& error) {
            Cerr << Colors.Red() << "\nFailed to execute query:" << Colors.OldColor() << Endl << Strip(ToString(error)) << Endl;
        } catch (std::exception& error) {
            Cerr << Colors.Red() << "\nFailed to execute query:" << Colors.OldColor() << Endl << Strip(error.what()) << Endl;
        }
    }

    void HandleBegin(const TString& line) {
        if (Transaction && Transaction->IsActive()) {
            Cerr << Colors.Red() << "\nThere is already a transaction in progress." << Colors.OldColor() << Endl;
            return;
        }

        auto txSettings = ParseBeginTransactionIsolation(line);
        if (!txSettings) {
            Cerr << Colors.Red() << "\nUnknown transaction isolation level." << Colors.OldColor() << Endl;
            Cerr << "Supported modes: " << GetTxModeNamesForHelp() << Endl;
            return;
        }

        try {
            EnsureQuerySession();
            auto beginResult = Session->BeginTransaction(*txSettings).GetValueSync();
            NStatusHelpers::ThrowOnErrorOrPrintIssues(beginResult);
            Transaction = beginResult.GetTransaction();
            UpdateTransactionPrompt();
            Cout << "BEGIN" << Endl;
        } catch (NStatusHelpers::TYdbErrorException& error) {
            Cerr << Colors.Red() << "\nFailed to begin transaction:" << Colors.OldColor() << Endl << Strip(ToString(error)) << Endl;
            ResetTransactionState();
        } catch (std::exception& error) {
            Cerr << Colors.Red() << "\nFailed to begin transaction:" << Colors.OldColor() << Endl << Strip(error.what()) << Endl;
            ResetTransactionState();
        }
    }

    void HandleCommit() {
        if (!Transaction || !Transaction->IsActive()) {
            Cerr << Colors.Red() << "\nThere is no active transaction." << Colors.OldColor() << Endl;
            return;
        }

        try {
            auto commitResult = Transaction->Commit().GetValueSync();
            NStatusHelpers::ThrowOnErrorOrPrintIssues(commitResult);
            Cout << "COMMIT" << Endl;
        } catch (NStatusHelpers::TYdbErrorException& error) {
            Cerr << Colors.Red() << "\nFailed to commit transaction:" << Colors.OldColor() << Endl << Strip(ToString(error)) << Endl;
        } catch (std::exception& error) {
            Cerr << Colors.Red() << "\nFailed to commit transaction:" << Colors.OldColor() << Endl << Strip(error.what()) << Endl;
        }
        ResetTransactionState();
        SqlLazyDriver->Stop(true);
    }

    void HandleRollback() {
        if (!Transaction || !Transaction->IsActive()) {
            Cerr << Colors.Red() << "\nThere is no active transaction." << Colors.OldColor() << Endl;
            return;
        }

        try {
            auto rollbackResult = Transaction->Rollback().GetValueSync();
            NStatusHelpers::ThrowOnErrorOrPrintIssues(rollbackResult);
            Cout << "ROLLBACK" << Endl;
        } catch (NStatusHelpers::TYdbErrorException& error) {
            Cerr << Colors.Red() << "\nFailed to rollback transaction:" << Colors.OldColor() << Endl << Strip(ToString(error)) << Endl;
        } catch (std::exception& error) {
            Cerr << Colors.Red() << "\nFailed to rollback transaction:" << Colors.OldColor() << Endl << Strip(error.what()) << Endl;
        }
        ResetTransactionState();
        SqlLazyDriver->Stop(true);
    }

    void ExecuteInTransaction(const TString& line) {
        NQuery::TExecuteQuerySettings settings;
        settings.StatsMode(CollectStatsMode);
        settings.ConcurrentResultSets(false);
        if (!ResourcePool.empty()) {
            settings.ResourcePool(ResourcePool);
        }

        try {
            TExecuteGenericQuery executeRunner(SqlLazyDriver->Get());
            executeRunner.Execute(line, {
                .Settings = settings,
                .AddIndent = true,
                .Session = *Session,
                .TxControl = NQuery::TTxControl::Tx(*Transaction),
            });
        } catch (NStatusHelpers::TYdbErrorException& error) {
            Cerr << Colors.Red() << "\nFailed to execute query:" << Colors.OldColor() << Endl << Strip(ToString(error)) << Endl;
        } catch (std::exception& error) {
            Cerr << Colors.Red() << "\nFailed to execute query:" << Colors.OldColor() << Endl << Strip(error.what()) << Endl;
        }
    }

    static void PrintInteractiveTransactionsNotAvailable() {
        Cerr << Colors.Red()
             << "\nInteractive transactions (BEGIN/COMMIT/ROLLBACK) are available only in ydb_int."
             << Colors.OldColor() << Endl;
    }

private:
    static ftxui::Element CreateHelpMessage(bool enableAiInteractive, bool enableInteractiveTransactions) {
        using namespace ftxui;

        std::vector<ftxui::Element> elements = {
            paragraph("By default, any input is treated as an YQL query and sent directly to the YDB server."),
            text(""),
            CreateEntityName("Hotkeys:"),
        };

        if (enableAiInteractive) {
            elements.emplace_back(
                CreateListItem(hbox({
                    CreateEntityName("Ctrl+T or /switch"),
                    text(": switch to "),
                    text(ToString(TInteractiveConfigurationManager::EMode::AI)) | color(Color::Cyan),
                    text(" interactive mode.")
                }))
            );
        }

        elements.emplace_back(CreateListItem(hbox({
            CreateEntityName("TAB"), text(": complete the current word based on YQL syntax.")
        })));

        elements.emplace_back(CreateListItem(hbox({
            CreateEntityName("Arrows Ctrl+↑ and Ctrl+↓"), text(": navigate through the current word completion list.")
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
            keyword("SET resource_pool = "), CreateEntityName("POOL_NAME"),
            text(": set resource pool for workload manager (empty string to reset).")
        })));

        elements.emplace_back(CreateListItem(hbox({
            keyword("EXPLAIN"), text(" ["), keyword("AST"), text("] "), CreateEntityName("SQL_QUERY"),
            text(": execute query in explain mode and optionally print AST.")
        })));

        if (enableInteractiveTransactions) {
            elements.emplace_back(CreateListItem(hbox({
                keyword("BEGIN"), text(" ["), keyword("TRANSACTION"), text("] ["),
                keyword("ISOLATION LEVEL"), text(" "), CreateEntityName("MODE"),
                text("]: start an interactive transaction (Query service session).")
            })));
            elements.emplace_back(CreateListItem(hbox({
                text("  Modes: "), CreateEntityName(GetTxModeNamesForHelp())
            })));
            elements.emplace_back(CreateListItem(hbox({
                keyword("COMMIT"), text(" | "), keyword("END"), text(": commit the current transaction.")
            })));
            elements.emplace_back(CreateListItem(hbox({
                keyword("ROLLBACK"), text(": rollback the current transaction.")
            })));
        }

        elements.emplace_back(text(""));
        elements.emplace_back(CreateEntityName("Interactive Commands:"));
        elements.emplace_back(CreateListItem(hbox({
            CreateEntityName("/config"), text(": change interactive mode settings (hints, color theme, colors mode).")
        })));
        PrintCommonInteractiveCommands(elements);

        return vbox(elements);
    }

    static TLineReaderSettings CreateSessionSettings(const TSqlSessionSettings& settings) {
        auto placeholder = TStringBuilder() << "Type YQL query (Enter to execute, Ctrl+Enter for newline";
        if (settings.EnableAiInteractive) {
            placeholder << ", Ctrl+T for AI mode";
        }
        placeholder << ", Ctrl+D to exit)";

        return {
            .LazyDriver = settings.CompleterLazyDriver,
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
            Cerr << Colors.Red() << "\nMissing variable name for \"SET\" special command." << Colors.OldColor() << Endl;
        } else if (tokens.size() == 2 || tokens[2].data != "=") {
            Cerr << Colors.Red() << "\nMissing \"=\" symbol for \"SET\" special command." << Colors.OldColor() << Endl;
        } else if (tokens.size() == 3) {
            Cerr << Colors.Red() << "\nMissing variable value for \"SET\" special command." << Colors.OldColor() << Endl;
        } else if (to_lower(TString(tokens[1].data)) == "stats") {
            TrySetCollectStatsMode(tokens);
        } else if (to_lower(TString(tokens[1].data)) == "resource_pool") {
            TrySetResourcePool(tokens);
        } else {
            Cerr << Colors.Red() << "\nUnknown variable name \"" << tokens[1].data << "\" for \"SET\" special command." << Colors.OldColor() << Endl;
        }
    }

    void TrySetCollectStatsMode(const std::vector<TLexer::TToken>& tokens) {
        size_t tokensSize = tokens.size();
        Y_VALIDATE(tokensSize >= 4, "Not enough tokens for \"SET stats\" special command.");

        if (tokensSize > 4) {
            Cerr << Colors.Red() << "\nVariable value for \"SET stats\" special command should contain exactly one token." << Colors.OldColor() << Endl;
            return;
        }

        auto statsMode = NQuery::ParseStatsMode(tokens[3].data);
        if (!statsMode) {
            Cerr << Colors.Red() << "\nUnknown stats collection mode: \"" << tokens[3].data << "\"." << Colors.OldColor() << Endl;
            return;
        }

        CollectStatsMode = *statsMode;
    }

    void TrySetResourcePool(const std::vector<TLexer::TToken>& tokens) {
        size_t tokensSize = tokens.size();
        Y_VALIDATE(tokensSize >= 4, "Not enough tokens for \"SET resource_pool\" special command.");

        if (tokensSize > 4) {
            Cerr << Colors.Red() << "\nVariable value for \"SET resource_pool\" special command should contain exactly one token." << Colors.OldColor() << Endl;
            return;
        }

        ResourcePool = std::string(tokens[3].data);
        Cout << "Resource pool set to \"" << ResourcePool << "\"." << Endl;
    }

    void EnsureQuerySession() {
        if (Session) {
            return;
        }
        QueryClient = NQuery::TQueryClient(SqlLazyDriver->Get());
        auto sessionResult = QueryClient->GetSession().GetValueSync();
        NStatusHelpers::ThrowOnErrorOrPrintIssues(sessionResult);
        Session = sessionResult.GetSession();
    }

    void UpdateTransactionPrompt() {
        if (LineReader) {
            LineReader->SetPrompt(TStringBuilder() << BasePrompt << "* ");
        }
    }

    void ResetTransactionPrompt() {
        if (LineReader) {
            LineReader->SetPrompt(BasePrompt);
        }
    }

    void ResetTransactionState() {
        Transaction.reset();
        Session.reset();
        QueryClient.reset();
        ResetTransactionPrompt();
    }

    void RollbackOpenTransaction(bool silent) {
        if (!Transaction || !Transaction->IsActive()) {
            ResetTransactionState();
            return;
        }
        auto rollbackResult = Transaction->Rollback().GetValueSync();
        if (!rollbackResult.IsSuccess() && !silent) {
            NStatusHelpers::ThrowOnErrorOrPrintIssues(rollbackResult);
        }
        if (!silent) {
            Cout << "ROLLBACK" << Endl;
        }
        ResetTransactionState();
    }

private:
    TQueryPlanPrinter QueryPlanPrinter;
    TLazyDriver::TPtr SqlLazyDriver;
    NQuery::EStatsMode CollectStatsMode = NQuery::EStatsMode::None;
    std::string ResourcePool;
    bool EnableAiInteractive;
    bool EnableInteractiveTransactions;
    TString BasePrompt;

    std::optional<NQuery::TQueryClient> QueryClient;
    std::optional<NQuery::TSession> Session;
    std::optional<NQuery::TTransaction> Transaction;
};

} // anonymous namespace

ISessionRunner::TPtr CreateSqlSessionRunner(const TSqlSessionSettings& settings) {
    return std::make_shared<TSqlSessionRunner>(settings);
}

} // namespace NYdb::NConsoleClient
